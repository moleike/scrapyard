#![feature(if_let_guard)]

pub mod messages;

pub mod server;

pub mod client;

mod error;

pub use error::{Error, Result};

mod engine;

pub use engine::{KvsEngine, kvs::KvStore, sled::Sled};
