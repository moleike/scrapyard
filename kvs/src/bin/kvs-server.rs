use std::io::stderr;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::exit;

use kvs::Result;
use kvs::server::Server;
use tracing::info;

use clap::{Parser, ValueEnum};
use tracing::Level;

const DEFAULT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000);

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, value_parser = clap::value_parser!(SocketAddr), default_value_t=DEFAULT_ADDR)]
    addr: SocketAddr,

    #[arg(long, value_enum, default_value_t=Engine::Kvs)]
    engine: Engine,
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
    let version = env!("CARGO_PKG_VERSION");

    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_max_level(Level::TRACE)
        .with_writer(stderr)
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();

    let cli = Cli::parse();

    info!("{}", version);
    info!("{:?}", cli.addr);
    info!("{:?}", cli.engine);

    let mut server = Server::new(cli.addr, None)?;

    server.run()?;

    Ok(())
}
