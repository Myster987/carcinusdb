use std::{
    io::{BufWriter, Write},
    net::TcpStream,
};

use crate::{
    tcp::{
        self,
        protocol::{Encode, Response},
    },
    utils::bytes::BytesCursor,
    vm::query_result::QueryResult,
};

pub struct Connection {
    stream: BufWriter<TcpStream>,
    write_buffer: BytesCursor<Vec<u8>>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            write_buffer: BytesCursor::new(Vec::with_capacity(4 * 1024)),
        }
    }

    pub fn send_query_result<'tx>(&mut self, result: QueryResult<'tx>) -> tcp::Result<()> {
        match result {
            QueryResult::RowsAffected(n) => self.send(&Response::RowsAffected(n))?,
            QueryResult::Rows(row_iterator) => {
                let schema = row_iterator.schema().clone();

                self.send(&Response::Schema(schema))?;

                for record_result in row_iterator {
                    match record_result {
                        Ok(row) => self.send(&Response::Row(row))?,
                        Err(err) => {
                            self.send(&Response::Error(format!("{err}")))?;
                            break;
                        }
                    }
                }
                self.send(&Response::End)?;
            }
        }

        self.stream.flush()?;

        Ok(())
    }

    fn send<T: Encode>(&mut self, value: &T) -> tcp::Result<()> {
        self.write_buffer.get_mut().clear();

        value.encode(&mut self.write_buffer);

        self.stream.write_all(self.write_buffer.get_ref())?;

        Ok(())
    }
}
