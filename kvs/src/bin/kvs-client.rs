use std::{env::current_dir, net::SocketAddr, process::{self, exit}, str::FromStr};

use clap::{Parser, Subcommand};
use kvs::{client::{self, Client}, messages::*, Result};

#[derive(Parser)]
#[command(version, author)] // Read from `Cargo.toml`
pub struct Cli {
    #[command(subcommand)]
    command: Command,
    #[arg(long, value_parser = clap::value_parser!(SocketAddr))]
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
    Compact
}

fn main() {
    if let Err(e) = run() {
        println!("{}", e);
        exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let addr = cli.addr.unwrap_or(SocketAddr::from_str("127.0.0.1:4000").unwrap());

    let mut client = Client::connect(addr)?;

    match &cli.command {
        Command::Get { key } => {
            let value = client.get(key)?;

            println!("{}", value)
        },
        Command::Set { key, value } => {
            client.set(key, value)?;
        }
        _ => println!("not implemented")
    }
    Ok(())
}
