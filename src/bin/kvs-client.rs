use std::{net::SocketAddr, process::exit, str::FromStr};

use clap::{Parser, Subcommand};
use kvs::{Error, Result, client::Client};

#[derive(Parser)]
#[command(version, about, long_about = None)] // Read from `Cargo.toml`
pub struct Cli {
    #[command(subcommand)]
    command: Command,
    #[arg(long,
          value_parser = clap::value_parser!(SocketAddr),
          global = true,
          display_order = 2000
    )]
    addr: Option<SocketAddr>,
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
    Compact,
}

fn main() {
    if let Err(e) = run() {
        println!("{}", e);
        exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let addr = cli
        .addr
        .unwrap_or(SocketAddr::from_str("127.0.0.1:4000").unwrap());

    let mut client = Client::connect(addr)?;

    match &cli.command {
        Command::Get { key } => match client.get(key) {
            Ok(value) => Ok(println!("{}", value)),
            Err(Error::KeyNotFound) => Ok(println!("Key not found")),
            Err(e) => Err(e),
        },
        Command::Set { key, value } => Ok(client.set(key, value)?),
        Command::Del { key } => match client.delete(key) {
            Ok(()) => Ok(()),
            Err(e) => {
                if let Error::KeyNotFound = e {
                    eprintln!("Key not found");
                }
                Err(e)
            }
        },
        _ => Ok(println!("not implemented")),
    }
}
