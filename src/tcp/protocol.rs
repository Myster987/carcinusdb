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

use crate::{sql::schema::Schema, tcp, vm::query_result::QueryResult};

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
}
