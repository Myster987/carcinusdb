use std::env;

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

    let hostname = env::var("CARCINUSDB_HOSTNAME")?;
    let port = env::var("CARCINUSDB_PORT")?.parse::<u16>()?;

    database::run(hostname, port).await?;

    Ok(())
}
