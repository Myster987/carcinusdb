use std::env;

mod database;
mod os;
mod sql;
mod storage;
mod tcp;
mod utils;
mod vm;

fn main() -> anyhow::Result<()> {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    dotenvy::dotenv()?;

    let db_path = env::var("CARCINUSDB_PATH")?;
    let hostname = env::var("CARCINUSDB_HOSTNAME")?;
    let port = env::var("CARCINUSDB_PORT")?.parse::<u16>()?;

    database::run(db_path, hostname, port)?;

    Ok(())
}
