use std::{
    io::{BufWriter, Write},
    net::TcpStream,
};

use crate::{
    tcp::{self, protocol::TcpWrite},
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
                cursor.put_bytes(b"\r\n");

                let response = cursor.into_inner();

                self.stream.write_all(&response)?;
            }
            QueryResult::Rows(row_iterator) => {
                let buffer = vec![b'i'];

                let mut cursor = BytesCursor::new(buffer);

                let schema = row_iterator.schema();

                cursor.put_varint(schema.len() as VarInt);

                for column in &schema.columns {
                    TcpWrite::push_to_buffer(column, &mut cursor);
                }

                let mut buffer = cursor.into_inner();

                self.stream.write_all(&buffer)?;

                buffer.clear();

                let mut cursor = BytesCursor::new(buffer);

                for record_result in row_iterator {
                    match record_result {
                        Ok(row) => {
                            cursor.put_u8(b'o');
                            TcpWrite::push_to_buffer(&row, &mut cursor);
                            self.stream.write_all(cursor.get_ref())?;
                        }
                        Err(err) => {
                            cursor.put_u8(b'e');
                            let error_message = format!("{err}");

                            cursor.put_varint(error_message.len() as VarInt);
                            cursor.put_bytes(error_message.as_bytes());

                            self.stream.write_all(cursor.get_ref())?;
                            break;
                        }
                    }
                    cursor.get_mut().clear();
                }

                self.stream.write_all(b"\r\n")?;
            }
        }

        Ok(())
    }
}
