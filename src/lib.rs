#![allow(missing_docs)]

//! # kvs
//!
//! a simple in-memory key/value store that maps strings to strings

use serde::{Deserialize, Serialize};
use serde_jsonlines::JsonLinesReader;
use std::fs::{self, read_to_string, File, OpenOptions};
use std::io::{self, prelude::*, BufReader};
use std::os::unix::fs::MetadataExt;
use std::{collections::HashMap, path::PathBuf, result};
use thiserror::Error;

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
    keydir: HashMap<String, ValueInfo>,
    wal_path: PathBuf,
}

struct ValueInfo {
    wal_offset: u64,
}

#[derive(Error, Debug)]
pub enum Error {
    //#[error("serialization error")]
    //Wire(#[from] serde_json::Error),
    #[error("File I/O error")]
    IO(#[from] io::Error),
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
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into().join("test.wal");

        let _ = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.clone())?;

        let mut keydir: HashMap<String, ValueInfo> = HashMap::new();

        let metadata = fs::metadata(&path)?;

        if metadata.len() > 0 {
            let iter = serde_jsonlines::json_lines::<Command, _>(&path)?;

            let cmds = iter.collect::<io::Result<Vec<Command>>>()?;

            for cmd in cmds {
                match cmd {
                    Command::Set(k, _, offset) => {
                        keydir.insert(k, ValueInfo { wal_offset: offset })
                    }
                    Command::Del(k, _) => keydir.remove(&k),
                };
            }
        }

        Ok(KvStore {
            keydir,
            wal_path: path,
        })
    }

    /// get `key` if it exists
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(value_info) = self.keydir.get(&key) {
            let path = &self.wal_path;
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
        let path = self.wal_path.clone();

        let metadata = fs::metadata(&path)?;

        let offset = metadata.size();

        serde_jsonlines::append_json_lines(path, &[Command::Set(key.clone(), value, offset)])?;

        self.keydir
            .insert(key.clone(), ValueInfo { wal_offset: offset });

        Ok(())
    }

    /// remove an key if exists and return the value
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(_) = self.get(key.clone())? {
            let path = self.wal_path.clone();

            let metadata = fs::metadata(&path)?;

            let offset = metadata.size();

            serde_jsonlines::append_json_lines(
                self.wal_path.clone(),
                &[Command::Del(key.clone(), offset)],
            )?;

            self.keydir.remove(&key);

            Ok(())
        } else {
            Err(Error::KeyNotFound)
        }
    }
}
