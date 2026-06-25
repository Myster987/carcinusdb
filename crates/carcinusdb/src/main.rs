use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    simple_logger::init()?;
    // dotenvy::dotenv()?;

    let db_path = env::args().nth(1).expect("DB path not provided");
    let hostname = env::var("CARCINUSDB_HOSTNAME").unwrap_or("127.0.0.1".to_string());
    let port = env::args()
        .nth(2)
        .unwrap_or("4000".to_string())
        .parse::<u16>()?;

    carcinusdb::run(db_path, hostname, port).await?;

    Ok(())
}
