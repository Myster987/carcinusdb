use std::net::SocketAddr;

use connection::Connection;
use tokio::net::TcpListener;

use crate::database::Result;

pub mod connection;
pub struct TcpServer {
    listener: TcpListener,
}

impl TcpServer {
    pub async fn new(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    pub async fn accept_connection(&self) -> Result<Connection> {
        let (stream, _) = self.listener.accept().await?;

        Ok(Connection::new(stream))
    }
}
