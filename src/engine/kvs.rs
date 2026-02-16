#![allow(missing_docs)]

//! # kvs
//!
//! a simple in-memory key/value store that maps strings to strings

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_jsonlines::{
    JsonLinesFileIter, JsonLinesIter, JsonLinesReader, WriteExt, append_json_lines,
};
use std::fs::{self, File, OpenOptions, exists, remove_file};
use std::io::{self, BufReader, BufWriter, prelude::*};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::{collections::HashMap, path::PathBuf};
use walkdir::{DirEntry, WalkDir};

use crate::Error;
use crate::engine::KvsEngine;

/// the key/value store is an abstract data type
///
/// # Examples
///
/// ```rust
/// use kvs::KvStore;
/// let mut kvs = KvStore::new();
/// kvs.set("foo".into(), "bar".into());
///
/// assert_eq!(kvs.get("foo".into()), Some("bar".into()));
/// ```
pub struct KvStore {
    keydir: KeyDir,
    active_file_id: u32,
    datastore_path: PathBuf,
}

type KeyDir = HashMap<String, ValueInfo>;

#[derive(Debug)]
struct ValueInfo {
    file_id: u32,
    file_offset: u64,
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set(String, String),
    Del(String),
}

impl KvsEngine for KvStore {
    /// get `key` if it exists
    fn get(&mut self, key: String) -> crate::Result<Option<String>> {
        let Some(value_info) = self.keydir.get(&key) else {
            return Ok(None);
        };

        let mut fp = BufReader::new(File::open(self.get_data_file_path(value_info.file_id))?);

        fp.seek_relative(value_info.file_offset as i64)?;

        let mut reader = JsonLinesReader::new(fp);

        Ok(reader.read::<Command>()?.map(|cmd| match cmd {
            Command::Set(_, value) => value,
            Command::Del(_) => panic!(),
        }))
    }

    /// set or replace `key` to `value`
    fn set(&mut self, key: String, value: String) -> crate::Result<()> {
        let path = self.get_active_wal_file()?;
        let offset = fs::metadata(&path)?.size();

        Self::append_new_entry(&path, Command::Set(key.clone(), value))?;

        self.keydir.insert(
            key,
            ValueInfo {
                file_offset: offset,
                file_id: self.active_file_id,
            },
        );

        Ok(())
    }

    /// remove an key if exists and return the value
    fn remove(&mut self, key: String) -> crate::Result<()> {
        let Some(_) = self.get(key.clone())? else {
            return Err(Error::KeyNotFound);
        };

        let path = self.get_active_wal_file()?;

        Self::append_new_entry(&path, Command::Del(key.clone()))?;

        self.keydir.remove(&key);

        Ok(())
    }
}

impl KvStore {
    /// restore database index
    pub fn open(path: impl Into<PathBuf>) -> crate::Result<Self> {
        let path: PathBuf = path.into();

        let keydir = Self::restore_keydir(&path)?;

        let default_active_wal = path.join("0000.wal");

        let active_wal_path = Self::active_wal_file(&path).unwrap_or(default_active_wal);

        Self::touch(&active_wal_path)?;

        let active_file_id = Self::get_data_file_id(active_wal_path);

        Ok(KvStore {
            keydir,
            active_file_id,
            datastore_path: path,
        })
    }

    pub fn active_wal_file<P: AsRef<Path>>(path: P) -> Option<PathBuf> {
        Self::get_wal_files_ordered(path)
            .into_iter()
            .reduce(|acc, e| acc.max(e)) // I know
    }

    /// apply log compaction
    fn merge(&mut self) -> crate::Result<()> {
        let Some(merged_file) = self.get_next_merged_file() else {
            return Ok(());
        };

        let merged_file_id = Self::get_data_file_id(&merged_file);

        let wal_files = Self::get_wal_files_ordered(&self.datastore_path);

        let fp = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&merged_file)?;
        let mut writer = BufWriter::new(fp);

        for path in wal_files {
            let id = Self::get_data_file_id(&path);

            if id == self.active_file_id {
                continue;
            }

            for line in JsonLinesWithOffsetIter::json_lines(&path)? {
                let (cmd, offset) = line?;

                match cmd {
                    Command::Set(key, value) => {
                        if let Some(ValueInfo {
                            file_id,
                            file_offset,
                        }) = self.keydir.get(&key)
                            && *file_id == id
                            && *file_offset == offset
                        {
                            let offset = writer.stream_position()?;

                            writer.write_json_lines(&[Command::Set(key.clone(), value)])?;

                            self.keydir
                                .insert(
                                    key,
                                    ValueInfo {
                                        file_offset: offset,
                                        file_id: merged_file_id,
                                    },
                                )
                                .ok_or(Error::Storage)
                                .map(drop)?
                        }
                    }
                    Command::Del(_) => (),
                }
            }

            remove_file(&path)?;
        }

        writer.flush()?;

        Ok(())
    }

    fn restore_keydir<P: AsRef<Path>>(dir: P) -> crate::Result<KeyDir> {
        let mut keydir: HashMap<String, ValueInfo> = HashMap::new();

        for path in Self::get_wal_files_ordered(dir) {
            let file_id = Self::get_data_file_id(&path);

            for line in JsonLinesWithOffsetIter::json_lines(&path)? {
                let (cmd, offset) = line?;

                match cmd {
                    Command::Set(k, _) => keydir.insert(
                        k,
                        ValueInfo {
                            file_offset: offset,
                            file_id,
                        },
                    ),
                    Command::Del(k) => keydir.remove(&k),
                };
            }
        }

        Ok(keydir)
    }

    fn append_new_entry(wal_path: &PathBuf, command: Command) -> crate::Result<()> {
        Ok(append_json_lines(wal_path, &[command])?)
    }

    fn get_active_wal_file(&mut self) -> crate::Result<PathBuf> {
        let path = self.get_data_file_path(self.active_file_id);
        let wal = File::open(&path)?;

        if BufReader::new(wal).lines().count() < 100 {
            Ok(path)
        } else {
            Ok(self.get_next_wal_file()?)
        }
    }

    // how do I make it more obvious that I don't know how to handler errors
    // albeit without proof, paths inputs to this method come from
    // get_wal_files_ordered or derived from the current active file
    fn get_data_file_id<P: AsRef<Path>>(path: P) -> u32 {
        path.as_ref()
            .file_stem()
            .unwrap()
            .to_os_string()
            .into_string()
            .unwrap()
            .parse()
            .unwrap()
    }

    fn get_next_merged_file(&mut self) -> Option<PathBuf> {
        // merged file is always one less than current active file
        let path = self.get_data_file_path(self.active_file_id - 1);

        if exists(&path).ok()? {
            // files already merged
            None
        } else {
            Some(path)
        }
    }

    fn get_data_file_path(&self, file_id: u32) -> PathBuf {
        let base = &self.datastore_path;

        base.join(PathBuf::from(format!("{:04}.wal", file_id)))
    }

    fn get_next_wal_file(&mut self) -> crate::Result<PathBuf> {
        // obviously this is not how you would run a compaction process,
        // but we are running a single-threaded server and so it's ok
        if self.get_total_num_wal_files() > 5 {
            self.merge()?
        }

        // log files are even-numbered
        self.active_file_id += 2;
        let next = self.get_data_file_path(self.active_file_id);

        Self::touch(&next)?;

        Ok(next)
    }

    fn is_wal_file(entry: &DirEntry) -> bool {
        let re = Regex::new(r"([0-9]{4}).wal").unwrap();

        entry.file_type().is_file()
            && entry
                .file_name()
                .to_str()
                .map(|s| re.is_match(s))
                .unwrap_or(false)
    }

    /// return wal files in chronological order
    fn get_wal_files_ordered<P: AsRef<Path>>(path: P) -> Vec<PathBuf> {
        WalkDir::new(path)
            .max_depth(1)
            .sort_by_file_name()
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(Self::is_wal_file)
            .map(|e| e.path().to_owned())
            .collect()
    }

    fn get_total_num_wal_files(&mut self) -> usize {
        Self::get_wal_files_ordered(&self.datastore_path).len()
    }

    fn touch<P: AsRef<Path>>(path: P) -> crate::Result<()> {
        Ok(OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .and(Ok(()))?)
    }
}

struct JsonLinesWithOffsetIter {
    inner: JsonLinesFileIter<Command>,
}

impl JsonLinesWithOffsetIter {
    pub fn new(reader: BufReader<File>) -> Self {
        JsonLinesWithOffsetIter {
            inner: JsonLinesIter::new(reader),
        }
    }

    pub fn json_lines<P: AsRef<Path>>(path: P) -> crate::Result<JsonLinesWithOffsetIter> {
        let reader = BufReader::new(File::open(path)?);
        Ok(Self::new(reader))
    }
}

impl Iterator for JsonLinesWithOffsetIter {
    type Item = io::Result<(Command, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.inner.get_mut().stream_position().ok()?; // this is not correct
        let item = self.inner.next()?;

        Some(item.map(|cmd| (cmd, offset)))
    }
}
