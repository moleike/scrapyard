use std::path::{Path, PathBuf};

use sled::Db;
use tracing::{error, info};

use crate::Error;

use super::KvsEngine;

pub struct Sled {
    db: Db,
}

impl Sled {
    pub fn open(path: impl Into<PathBuf>) -> crate::Result<Self> {
        let db = sled::open(path.into())?;
        Ok(Self { db })
    }

    pub fn is_restart<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().join("conf").exists()
    }
}

impl Drop for Sled {
    fn drop(&mut self) {
        let flushed = self.db.flush().is_ok();
        info!("data flushed to disk {}", flushed);
    }
}

impl KvsEngine for Sled {
    fn get(&mut self, key: String) -> crate::Result<Option<String>> {
        let Some(data) = self.db.get(key)? else {
            return Ok(None);
        };

        Ok(Some(String::from_utf8(data.to_vec())?))
    }

    fn set(&mut self, key: String, value: String) -> crate::Result<()> {
        Ok(self.db.insert(key, value.as_bytes()).map(drop)?)
    }

    fn remove(&mut self, key: String) -> crate::Result<()> {
        self.db.remove(key)?.ok_or(Error::KeyNotFound).map(drop)
    }
}
