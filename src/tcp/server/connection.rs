use std::{
    io::{BufWriter, Write},
    net::TcpStream,
};

use crate::{
    tcp,
    utils::bytes::{BytesCursor, VarInt},
    vm::query_result::QueryResult,
};

pub struct Connection {
    stream: BufWriter<TcpStream>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
        }
    }

    pub fn write(&mut self, query_result: QueryResult) -> tcp::Result<()> {
        match query_result {
            QueryResult::RowsAffected(rows_affected) => {
                let buffer = vec![b'a'];

                let mut cursor = BytesCursor::new(buffer);

                cursor.put_varint(rows_affected as VarInt);

                let response = cursor.into_inner();

                self.stream.write_all(&response)?;
            }
            QueryResult::Rows(row_iterator) => {
                todo!()
            }
        }

        Ok(())
    }
}
