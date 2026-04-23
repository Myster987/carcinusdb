use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::tcp::{
    self,
    protocol::{CarcinusClientCodec, Request, Response},
};

pub struct ClientConnection {
    framed: Framed<TcpStream, CarcinusClientCodec>,
}

impl ClientConnection {
    pub async fn connect(addr: &str) -> tcp::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            framed: Framed::new(stream, CarcinusClientCodec),
        })
    }

    pub async fn send(&mut self, request: Request) -> tcp::Result<()> {
        self.framed.send(request).await
    }

    pub async fn receive(&mut self) -> tcp::Result<Response> {
        self.framed
            .next()
            .await
            .ok_or(tcp::Error::ConnectionClosed)?
    }
}
