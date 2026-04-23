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

pub struct ServerConnection {
    framed: Framed<TcpStream, CarcinusServerCodec>,
}

impl ServerConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            framed: Framed::new(stream, CarcinusServerCodec),
        }
    }

    pub async fn receive(&mut self) -> tcp::Result<Request> {
        self.framed
            .next()
            .await
            .ok_or(tcp::Error::ConnectionClosed)?
    }

    pub async fn send(&mut self, response: Response) -> tcp::Result<()> {
        self.framed.send(response).await
    }

    pub async fn send_query_result<'tx>(&mut self, result: QueryResult<'tx>) -> tcp::Result<()> {
        match result {
            QueryResult::RowsAffected(n) => self.send(Response::RowsAffected(n)).await?,
            QueryResult::Rows(row_iterator) => {
                let schema = row_iterator.schema().clone();

                self.send(Response::Schema(schema)).await?;

                for record_result in row_iterator {
                    match record_result {
                        Ok(row) => self.send(Response::Row(row)).await?,
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
