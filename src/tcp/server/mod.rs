use std::net::{SocketAddr, TcpListener};

use crate::{database::Result, tcp::server::connection::Connection};

pub mod connection;

pub struct TcpServer {
    listener: TcpListener,
}

impl TcpServer {
    pub fn new(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr)?;

        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    pub fn accept_connection(&self) -> Result<Connection> {
        let (stream, _) = self.listener.accept()?;

        Ok(Connection::new(stream))
    }
}
