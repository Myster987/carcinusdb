use std::net::SocketAddr;

use connection::Connection;
use tokio::net::TcpListener;

use crate::{database::Database, error::DatabaseResult};

pub mod connection;
pub struct TcpServer {
    listener: TcpListener,
}

impl TcpServer {
    pub async fn new(addr: SocketAddr) -> DatabaseResult<Self> {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    pub async fn accept_connection(&self) -> DatabaseResult<Connection> {
        let (stream, _) = self.listener.accept().await?;

        Ok(Connection::new(stream))
    }
}

pub async fn start(addr: SocketAddr, database_name: &str) -> DatabaseResult<()> {
    let db = Database::init(database_name)?;

    let tcp_server = TcpServer::new(addr).await?;

    log::info!("Listening on: {}", tcp_server.local_addr());
    loop {
        let conn = tcp_server.accept_connection().await?;

        handle_connection(conn)?
    }

    Ok(())
}

pub fn handle_connection(conn: Connection) -> DatabaseResult<()> {
    log::info!("Connection from: {}", conn.client_address());

    Ok(())
}
