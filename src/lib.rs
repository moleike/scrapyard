#![allow(missing_docs)]

//! # kvs
//!
//! a simple in-memory key/value store that maps strings to strings

use serde::{Deserialize, Serialize};
use std::fs::{self, read_to_string, File, OpenOptions};
use std::io::{self, prelude::*};
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
    cache: HashMap<String, String>,
    wal_path: PathBuf,
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
    Set(String, String),
    Del(String),
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

        let cache: HashMap<String, String> = HashMap::new();

        Ok(KvStore {
            cache,
            wal_path: path,
        })
    }

    /// get `key` if it exists
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let path = &self.wal_path;
        let metadata = fs::metadata(path)?;

        if metadata.len() > 0 {
            let cmds =
                serde_jsonlines::json_lines(path)?.collect::<std::io::Result<Vec<Command>>>()?;

            for cmd in cmds {
                match cmd {
                    Command::Set(k, v) => self.cache.insert(k, v),
                    Command::Del(k) => self.cache.remove(&k),
                };
            }
        }

        Ok(self.cache.get(&key).cloned())
    }

    /// set or replace `key` to `value`
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_jsonlines::append_json_lines(
            self.wal_path.clone(),
            &[Command::Set(key, value)],
        )?;

        Ok(())
    }

    /// remove an key if exists and return the value
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(_) = self.get(key.clone())? {
            serde_jsonlines::append_json_lines(
                self.wal_path.clone(),
                &[Command::Del(key.clone())],
            )?;

            Ok(())
        } else {
            Err(Error::KeyNotFound)
        }
    }
}
