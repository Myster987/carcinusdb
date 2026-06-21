use std::fmt::Display;

use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::{
    sql::{record::Record, schema::Schema},
    tcp::{
        self,
        protocol::{CarcinusClientCodec, Request, Response},
    },
    utils::debug_table::DebugTable,
};

/// Async handler for sending requests and reciving responses. Contains all
/// the necessary methods to execute SQL queries and parse results.
pub struct ClientConnection {
    /// Framed [TcpStream], that allows to parse only complete data.
    framed: Framed<TcpStream, CarcinusClientCodec>,
}

impl ClientConnection {
    /// Create new connection to server at given `addr`. Can return error.
    pub async fn connect(addr: &str) -> tcp::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            framed: Framed::new(stream, CarcinusClientCodec),
        })
    }

    /// Send `Request` to server.
    pub async fn send(&mut self, request: Request) -> tcp::Result<()> {
        self.framed.send(request).await
    }

    /// Recive response from
    pub async fn receive(&mut self) -> tcp::Result<Response> {
        self.framed
            .next()
            .await
            .ok_or(tcp::Error::ConnectionClosed)?
    }

    /// Send SQL query to db and return parsed result.
    pub async fn query(&mut self, sql: &str) -> tcp::Result<ClientQueryResult> {
        self.send(Request::Query(sql.to_string())).await?;

        match self.receive().await? {
            Response::RecordsAffected(n) => Ok(ClientQueryResult::RowsAffected(n)),
            Response::Schema(schema) => {
                let mut records = Vec::new();
                loop {
                    match self.receive().await? {
                        Response::Record(r) => records.push(r),
                        Response::End => break,
                        Response::Error(err) => return Err(tcp::Error::ServerError(err)),
                        _ => return Err(tcp::Error::Corrupted),
                    }
                }
                Ok(ClientQueryResult::Table(Table::new(schema, records)))
            }
            Response::Error(err) => Err(tcp::Error::ServerError(err)),
            _ => Err(tcp::Error::Corrupted),
        }
    }
}

impl Drop for ClientConnection {
    fn drop(&mut self) {
        futures::executor::block_on(self.framed.send(Request::Close))
            .expect("Should close connection gracefuly");
    }
}

pub enum ClientQueryResult {
    RowsAffected(usize),
    Table(Table),
}

impl Display for ClientQueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RowsAffected(n) => write!(f, "ROWS AFFECTED {n}"),
            Self::Table(table) => write!(f, "{table}"),
        }
    }
}

/// Simple wrapper to handle data that is returned by query.
pub struct Table {
    /// Definition of data.
    pub schema: Schema,
    /// All records that were returned by query.
    pub records: Vec<Record>,
}

impl Table {
    pub fn new(schema: Schema, records: Vec<Record>) -> Self {
        Self { schema, records }
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg_table = DebugTable::new();

        for col in self.schema.column_names() {
            dbg_table.add_column(&col);
        }

        for record in &self.records {
            dbg_table.insert_row(record.values().iter().map(|v| v.to_string()).collect());
        }

        dbg_table.fmt(f)
    }
}
