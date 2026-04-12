// plan:
// Request comes in (SQL).
// DB runs SQL.
// QueryResult iterator is returned.
// Now Connector starts responding and writes rows to buffer.
// When buffer is filled up it is flushed to connection
// ^ this is reapeded untill query is empty
// Enconding etc should be like this:
// | Request header that contains response schema | Row 1 (variable size) | Row 2 | Row 3 | ... EOF

use std::io::Cursor;

use crate::{
    sql::schema::Schema,
    tcp,
    utils::bytes::{BytesCursor, VarInt},
    vm::query_result::QueryResult,
};

pub trait TcpRead {
    /// This function should validate if incoming request is correct and
    /// advance cursor position to go over request len.
    fn validate(src: &mut Cursor<&[u8]>) -> tcp::Result<()>;

    fn parse(src: &mut Cursor<&[u8]>) -> tcp::Result<Self>
    where
        Self: Sized;
}

pub trait TcpWrite {
    /// Anything that implements `to_bytes` can be send over tcp.
    /// The problem is if it can be later parsed safely.
    fn to_bytes(self) -> Vec<u8>;
}

pub struct Response<'a> {
    schema: Schema,
    query_result: QueryResult<'a>,
}

impl<'a> Response<'a> {
    pub fn new(schema: Schema, query_result: QueryResult<'a>) -> Self {
        Self {
            schema,
            query_result,
        }
    }

    /// For iterator based respond type is "i" and for rows affected, it is
    /// "a".
    pub fn response_type(&self) -> u8 {
        match self.query_result {
            QueryResult::Rows(_) => b'i',
            QueryResult::RowsAffected(_) => b'a',
        }
    }
}

impl<'a> TcpWrite for Response<'a> {
    fn to_bytes(self) -> Vec<u8> {
        let buffer = vec![self.response_type()];

        let mut cursor = BytesCursor::new(buffer);

        let columns = self.schema.column_names();
        let columns_len = columns.len() as VarInt;

        cursor.put_varint(columns_len);

        for col in self.schema.column_names() {
            cursor.put_varint(col.len() as VarInt);
            cursor.put_bytes(col.as_bytes());
        }

        match self.query_result {
            QueryResult::RowsAffected(rows_affected) => cursor.put_varint(rows_affected as VarInt),
            QueryResult::Rows(row_iterator) => {
                for row_result in row_iterator {
                    match row_result {
                        Ok(row) => cursor.put_bytes(&row.to_bytes()),
                        Err(_) => todo!(),
                    }
                }
            }
        }

        cursor.put_bytes(b"\r\n");

        todo!()
    }
}
