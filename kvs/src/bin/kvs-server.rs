use std::net::SocketAddr;
use std::net::TcpListener;
use std::process::exit;
use std::str::FromStr;

use kvs::Result;
use kvs::server;
use tracing::info;

use clap::{Parser, ValueEnum};
use tracing::Level;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, value_parser = clap::value_parser!(SocketAddr))]
    addr: Option<SocketAddr>,

    #[arg(long, value_enum)]
    engine: Option<Engine>,
}

#[derive(Clone, Debug, ValueEnum)]
enum Engine {
    Kvs,
    Sled,
}

fn main() {
    if let Err(e) = run() {
        println!("{}", e);
        exit(1);
    }
}

fn run() -> Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();

    let cli = Cli::parse();

    info!("{:?} {:?}", cli.addr, cli.engine);

    let addr = cli.addr.unwrap_or(SocketAddr::from_str("127.0.0.1:4000").unwrap());

    let listener = TcpListener::bind(addr)?;

    info!("listening on {:?}", addr);

    server::run(listener);

    Ok(())
}
