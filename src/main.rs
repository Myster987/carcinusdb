use std::env;

mod database;
mod os;
mod sql;
mod storage;
mod tcp;
mod utils;
mod vm;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    simple_logger::init()?;
    dotenvy::dotenv()?;

    let db_path = env::var("CARCINUSDB_PATH")?;
    let hostname = env::var("CARCINUSDB_HOSTNAME").unwrap_or("127.0.0.1".to_string());
    let port = env::var("CARCINUSDB_PORT")
        .unwrap_or("4000".to_string())
        .parse::<u16>()?;

    database::run(db_path, hostname, port).await?;

    Ok(())
}

#[tokio::test]
async fn test_client() -> anyhow::Result<()> {
    use crate::tcp::client::ClientConnection;

    simple_logger::init()?;

    let mut client = ClientConnection::connect("127.0.0.1:4000").await?;

    let sql = utils::io::input("Enter SQL: ")?;

    let query_result = client.query(&sql).await?;

    println!("{query_result}");

    Ok(())
}
