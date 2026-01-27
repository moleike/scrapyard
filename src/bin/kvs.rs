use std::process::{self, exit};

use clap::{Parser, Subcommand};
use kvs::{KvStore, Result};

#[derive(Parser)]
#[command(version, author)] // Read from `Cargo.toml`
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    #[command(name = "rm")]
    Del {
        key: String,
    },
}

fn main() {
    if let Err(e) = run() {
        println!("{}", e);
        exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let mut kv_store = KvStore::open(".")?;

    match &cli.command {
        Command::Get { key } => {
            if let Some(value) = kv_store.get(key.to_string())? {
                println!("{}", value);
            } else {
                println!("{}", kvs::Error::KeyNotFound);
            }

            Ok(())
        }
        Command::Del { key } => kv_store.remove(key.to_string()),
        Command::Set { key, value } => kv_store.set(key.to_string(), value.to_string()),
    }
}
