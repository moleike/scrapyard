#![deny(missing_docs)]

//! # kvs
//!
//! a simple in-memory key/value store that maps strings to strings

use std::collections::HashMap;

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
pub struct KvStore(HashMap<String, String>);

impl KvStore {
    /// create a new store
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// get `key` if it exists
    pub fn get(&self, key: String) -> Option<String> {
        self.0.get(&key).cloned()
    }

    /// set or replace `key` to `value`
    pub fn set(&mut self, key: String, value: String) -> Option<String> {
        self.0.insert(key, value)
    }

    /// remove an key if exists and return the value
    pub fn remove(&mut self, key: String) -> Option<String> {
        self.0.remove(&key)
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}
