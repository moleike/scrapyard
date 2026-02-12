use flatbuffers::InvalidFlatbuffer;
use std::{io, result};
use thiserror::Error;

/// errors
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
    #[error("Server error")]
    ServerError,
    #[error("Protocol error")]
    ProtocolError(#[from] InvalidFlatbuffer),
    #[error("Unknown error")]
    Unknown,
}

pub type Result<T> = result::Result<T, Error>;
