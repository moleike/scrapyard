use std::process;

use clap::{Parser, Subcommand};
use kvs::KvStore;

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
    let cli = Cli::parse();
    let _ = KvStore::new();

    match &cli.command {
        Command::Get { key } => eprintln!("unimplemented"),
        Command::Del { key } => eprintln!("unimplemented"),
        Command::Set { key, value } => eprintln!("unimplemented"),
    }

    process::exit(1);
}
