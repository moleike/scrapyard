#![allow(missing_docs)]

//! # kvs
//!
//! a simple in-memory key/value store that maps strings to strings

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_jsonlines::{append_json_lines, json_lines, JsonLinesReader};
use std::fs::{self, File, OpenOptions};
use std::io::{self, prelude::*, BufReader};
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
    datastore_path: PathBuf
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
    Set(String, String, u64),
    Del(String, u64),
}

impl KvStore {

    /// restore database index
    pub fn open(path: impl Into<PathBuf> + Copy) -> Result<Self> {
        let default_active_wal = path.into().join("0001.wal");

        let keydir = Self::restore_keydir(&path.into())?;

        let active_wal_path: PathBuf =
            Self::active_wal_file(&path.into()).unwrap_or(default_active_wal);

        Self::touch(&active_wal_path)?;

        Ok(KvStore {
            keydir,
            active_wal_path,
            datastore_path: path.into()
        })
    }

    /// get `key` if it exists
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value_info) = self.keydir.get(&key) {
            let path = &value_info.wal_path;
            let mut fp = BufReader::new(File::open(path)?);

            fp.seek_relative(value_info.wal_offset as i64)?;

            let mut reader = JsonLinesReader::new(fp);

            Ok(reader.read::<Command>()?.map(|cmd| match cmd {
                Command::Set(_, value, _) => value,
                Command::Del(_, _) => panic!(),
            }))
        } else {
            Ok(None)
        }
    }

    /// set or replace `key` to `value`
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let path = self.get_active_wal_file()?;

        let metadata = fs::metadata(&path)?;

        let offset = metadata.size();

        Self::append_new_entry(&path, Command::Set(key.clone(), value, offset))?;

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
        if let Some(_) = self.get(key.clone())? {
            let path = self.get_active_wal_file()?;

            let metadata = fs::metadata(&path)?;

            let offset = metadata.size();

            Self::append_new_entry(&path, Command::Del(key.clone(), offset))?;

            self.keydir.remove(&key);

            Ok(())
        } else {
            Err(Error::KeyNotFound)
        }
    }

    fn restore_keydir(dir: &PathBuf) -> Result<KeyDir> {
        let mut keydir: HashMap<String, ValueInfo> = HashMap::new();

        for path in Self::get_wal_files_ordered(&dir.into()) {
            let metadata = fs::metadata(&path)?;

            if metadata.len() > 0 {
                let cmds_iter = json_lines::<Command, _>(&path)?;

                for cmd in cmds_iter {
                    match cmd? {
                        Command::Set(k, _, offset) => keydir.insert(
                            k,
                            ValueInfo {
                                wal_offset: offset,
                                wal_path: path.to_owned(),
                            },
                        ),
                        Command::Del(k, _) => keydir.remove(&k),
                    };
                }
            }
        }

        Ok(keydir)
    }

    fn append_new_entry(wal_path: &PathBuf, command: Command) -> Result<()> {
        Ok(append_json_lines(wal_path, &[command])?)
    }

    fn get_active_wal_file(&mut self) -> Result<PathBuf> {
        let wal = File::open(&self.active_wal_path)?;

        if io::BufReader::new(wal).lines().count() < 3 {
            Ok(self.active_wal_path.clone())
        } else {
            let next = self.get_next_wal().ok_or(Error::Storage)?;
            let mut perms = fs::metadata(&self.active_wal_path)?.permissions();

            perms.set_readonly(true);
            fs::set_permissions(&self.active_wal_path, perms)?;

            self.active_wal_path = next.clone();

            Ok(next)
        }
    }

    fn get_next_wal<'a>(&mut self) -> Option<PathBuf> {
        let path = &self.active_wal_path;
        let base = &self.active_wal_path.parent()?;
        let path = path.clone().into_os_string().into_string().ok()?;
        let re = Regex::new(r"([0-9]{4}).wal").ok()?;
        let cur: u32 = re.captures(&path)?.get(1)?.as_str().parse().ok()?;

        let path = base.join(PathBuf::from(format!("{:04}.wal", cur + 1)));

        Self::touch(&path).ok();

        Some(path)
    }

    fn is_wal_file(entry: &DirEntry) -> bool {
        entry
            .file_name()
            .to_str()
            .map(|s| s.ends_with(r".wal"))
            .unwrap_or(false)
    }

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
