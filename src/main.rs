use clap::Parser;
use cli::{Cli, CommandArgs};

mod cli;
mod database;
mod error;
mod os;
mod query;
mod sql;
mod storage;
mod tcp;
mod utils;
mod vm;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    dotenvy::dotenv()?;

    let cli = Cli::parse();

    match cli.command {
        cli::Commands::Run(CommandArgs {
            database_file_path,
            hostname,
            port,
        }) => {
            log::info!("File path: {}", database_file_path);
            log::info!("Hostname: {}", hostname);
            log::info!("Port: {}", port);
        }
    }

    Ok(())
}
