use tokio::net::TcpStream;

use crate::error::DatabaseResult;


pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn client_address(&self) -> String {
        self.stream.peer_addr().unwrap().to_string()
    }
}
