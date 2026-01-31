#![allow(missing_docs)]

//! # kvs
//!
//! a simple in-memory key/value store that maps strings to strings

use regex::Regex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_jsonlines::{
    append_json_lines, JsonLinesFileIter, JsonLinesIter, JsonLinesReader, WriteExt,
};
use std::fs::{self, exists, remove_file, File, OpenOptions};
use std::io::{self, prelude::*, BufReader, BufWriter};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::{collections::HashMap, path::PathBuf, result};
use thiserror::Error;
use walkdir::{DirEntry, WalkDir};

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
    active_wal_path: PathBuf,
    datastore_path: PathBuf,
}

type KeyDir = HashMap<String, ValueInfo>;

#[derive(Debug)]
struct ValueInfo {
    wal_path: PathBuf,
    wal_offset: u64,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Storage error")]
    Storage,
    #[error("File error")]
    IO(#[from] io::Error),
    #[error("Directory error")]
    WalkDirError(#[from] walkdir::Error),
    #[error("Key not found")]
    KeyNotFound,
    #[error("Unknown key-value store error")]
    Unknown,
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set(String, String),
    Del(String),
}

impl KvStore {
    /// restore database index
    pub fn open(path: impl Into<PathBuf> + Copy) -> Result<Self> {
        let default_active_wal = path.into().join("0000.wal");

        let keydir = Self::restore_keydir(&path.into())?;

        let active_wal_path: PathBuf =
            Self::active_wal_file(&path.into()).unwrap_or(default_active_wal);

        Self::touch(&active_wal_path)?;

        Ok(KvStore {
            keydir,
            active_wal_path,
            datastore_path: path.into(),
        })
    }

    /// get `key` if it exists
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let Some(value_info) = self.keydir.get(&key) else {
            return Ok(None);
        };

        let path = &value_info.wal_path;
        let mut fp = BufReader::new(File::open(path)?);

        fp.seek_relative(value_info.wal_offset as i64)?;

        let mut reader = JsonLinesReader::new(fp);

        Ok(reader.read::<Command>()?.map(|cmd| match cmd {
            Command::Set(_, value) => value,
            Command::Del(_) => panic!(),
        }))
    }

    /// set or replace `key` to `value`
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let path = self.get_active_wal_file()?;
        let offset = fs::metadata(&path)?.size();

        Self::append_new_entry(&path, Command::Set(key.clone(), value))?;

        self.keydir.insert(
            key,
            ValueInfo {
                wal_offset: offset,
                wal_path: path,
            },
        );

        Ok(())
    }

    /// remove an key if exists and return the value
    pub fn remove(&mut self, key: String) -> Result<()> {
        let Some(_) = self.get(key.clone())? else {
            return Err(Error::KeyNotFound);
        };

        let path = self.get_active_wal_file()?;

        Self::append_new_entry(&path, Command::Del(key.clone()))?;

        self.keydir.remove(&key);

        Ok(())
    }

    /// apply log compaction
    pub fn merge(&mut self) -> Result<()> {
        let wal_files = Self::get_wal_files_ordered(&self.datastore_path);
        let merged_file = self.get_next_merged_file().ok_or(Error::Storage)?;
        let fp = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&merged_file)?;
        let mut writer = BufWriter::new(fp);

        for path in wal_files {
            if path == self.active_wal_path {
                continue;
            }

            let reader = BufReader::new(File::open(&path)?);

            for line in JsonLinesWithOffsetIter::new(reader) {
                let (cmd, offset) = line?;

                match cmd {
                    Command::Set(key, value) => {
                        if let Some(ValueInfo {
                            wal_path,
                            wal_offset,
                        }) = self.keydir.get(&key)
                        {
                            if *wal_path == path && *wal_offset == offset {
                                let offset = writer.stream_position()?;

                                writer.write_json_lines(&[Command::Set(key.clone(), value)])?;

                                self.keydir
                                    .insert(
                                        key,
                                        ValueInfo {
                                            wal_path: merged_file.clone(),
                                            wal_offset: offset,
                                        },
                                    )
                                    .ok_or(Error::Storage)
                                    .map(|_| ())?
                            }
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

    fn restore_keydir(dir: &PathBuf) -> Result<KeyDir> {
        let mut keydir: HashMap<String, ValueInfo> = HashMap::new();

        for path in Self::get_wal_files_ordered(&dir.into()) {
            let reader = BufReader::new(File::open(&path)?);

            for line in JsonLinesWithOffsetIter::new(reader) {
                let (cmd, offset) = line?;

                match cmd {
                    Command::Set(k, _) => keydir.insert(
                        k,
                        ValueInfo {
                            wal_offset: offset,
                            wal_path: path.to_owned(),
                        },
                    ),
                    Command::Del(k) => keydir.remove(&k),
                };
            }
        }

        Ok(keydir)
    }

    fn append_new_entry(wal_path: &PathBuf, command: Command) -> Result<()> {
        Ok(append_json_lines(wal_path, &[command])?)
    }

    fn get_active_wal_file(&mut self) -> Result<PathBuf> {
        let wal = File::open(&self.active_wal_path)?;

        if BufReader::new(wal).lines().count() < 100 {
            Ok(self.active_wal_path.clone())
        } else {
            let next = self.get_next_wal_file().ok_or(Error::Storage)?;

            Self::touch(&next)?;

            self.active_wal_path = next.clone();

            Ok(next)
        }
    }

    fn get_active_wal_seq_num(&mut self) -> Option<u32> {
        let path = &self.active_wal_path;
        let path = path.clone().into_os_string().into_string().ok()?;
        let re = Regex::new(r"([0-9]{4}).wal").ok()?;

        re.captures(&path)?.get(1)?.as_str().parse().ok()
    }

    fn get_next_merged_file(&mut self) -> Option<PathBuf> {
        let cur = self.get_active_wal_seq_num()?;
        let path = self.get_data_file_path(cur - 1);

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

    fn get_next_wal_file(&mut self) -> Option<PathBuf> {
        // obviously this is not how you would run a compaction process,
        // but we are running a single-threaded server and so it's ok
        if self.get_total_num_wal_files() > 5 {
            self.merge().ok()?
        }

        let cur = self.get_active_wal_seq_num()?;

        // log files are even-numbered
        let path = self.get_data_file_path(cur + 2);

        Some(path)
    }

    fn is_wal_file(entry: &DirEntry) -> bool {
        entry
            .file_name()
            .to_str()
            .map(|s| s.ends_with(".wal"))
            .unwrap_or(false)
    }

    /// return wal files in chronological order
    fn get_wal_files_ordered(path: &PathBuf) -> Vec<PathBuf> {
        WalkDir::new(path)
            .max_depth(1)
            .sort_by_file_name()
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| Self::is_wal_file(e))
            .map(|e| e.path().to_owned())
            .collect()
    }

    fn get_total_num_wal_files(&mut self) -> usize {
        Self::get_wal_files_ordered(&self.datastore_path).len()
    }

    fn active_wal_file(path: &PathBuf) -> Option<PathBuf> {
        Self::get_wal_files_ordered(path)
            .into_iter()
            .reduce(|acc, e| acc.max(e)) // I know
    }

    fn touch(path: &Path) -> Result<()> {
        Ok(OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .and(Ok(()))?)
    }
}

struct JsonLinesWithOffsetIter<T> {
    inner: JsonLinesFileIter<T>,
}

impl<T> JsonLinesWithOffsetIter<T> {
    pub fn new(reader: BufReader<File>) -> Self {
        JsonLinesWithOffsetIter {
            inner: JsonLinesIter::new(reader),
        }
    }
}

impl<T> Iterator for JsonLinesWithOffsetIter<T>
where
    T: DeserializeOwned,
{
    type Item = io::Result<(T, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.inner.get_mut().stream_position().ok()?; // this is not correct
        let item = self.inner.next()?;

        Some(item.map(|i| (i, offset)))
    }
}
