use tokio::net::TcpListener;

use crate::{database::Database, error::DatabaseResult};

pub async fn start(hostname: &str, port: i32) -> DatabaseResult<()> {
    // let db = Database::init(path)?;
    let tcp_server = TcpServer::new(hostname, port);

    tcp_server.run().await?;

    Ok(())
} 

pub struct TcpServer {
    hostname: String,
    port: i32
}

impl TcpServer {
    pub fn new(hostname: &str, port: i32) -> Self {
        Self { hostname: hostname.to_string(), port }
    }

    pub async fn run(&self) -> DatabaseResult<()> {
        let listener = TcpListener::bind(format!("{}:{}", self.hostname, self.port)).await?;

        log::info!("Listening on {}", listener.local_addr()?);



        Ok(())
    }
}
