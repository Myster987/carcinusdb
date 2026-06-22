use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::{
    tcp::{
        self,
        protocol::{CarcinusServerCodec, Request, Response},
    },
    vm::query_result::QueryResult,
};

/// Async handler for sending responses and reciving requests. Contains all
/// the necessary methods to process, execute and encode query results.
pub struct ServerConnection {
    /// Framed [TcpStream], that allows to parse only complete data.
    framed: Framed<TcpStream, CarcinusServerCodec>,
}

impl ServerConnection {
    /// Create new client/server connection.
    pub fn new(stream: TcpStream) -> Self {
        Self {
            framed: Framed::new(stream, CarcinusServerCodec),
        }
    }

    /// Recive request from `ClientConnection`.
    pub async fn receive(&mut self) -> tcp::Result<Request> {
        self.framed
            .next()
            .await
            .ok_or(tcp::Error::ConnectionClosed)?
    }

    /// Send `Response` to client.
    pub async fn send(&mut self, response: Response) -> tcp::Result<()> {
        self.framed.send(response).await
    }

    /// Stream whole `QueryResult` to client.
    pub async fn send_query_result<'tx>(&mut self, result: QueryResult<'tx>) -> tcp::Result<()> {
        match result {
            QueryResult::RecordsAffected(n) => self.send(Response::RecordsAffected(n)).await?,
            QueryResult::Records(row_iterator) => {
                let schema = row_iterator.schema().clone();

                self.send(Response::Schema(schema)).await?;

                for record_result in row_iterator {
                    match record_result {
                        Ok(r) => self.send(Response::Record(r)).await?,
                        Err(err) => {
                            self.send(Response::Error(format!("{err}"))).await?;
                            break;
                        }
                    }
                }
                self.send(Response::End).await?;
            }
        }
        Ok(())
    }
}
