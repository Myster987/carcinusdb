//! Protocol used by CarcinusDB:
//!
//! Response format:
//!
//! ```text
//! +-------------------------------------------+
//! |                 HEADER                    |
//! +-------------------------------------------+
//! | Schema:                                   |
//! |   - Schema len - varint                   |
//! |   - N Columns:                            |
//! |       - Column name len in bytes - varint |
//! |       - Column name - n bytes             |
//! |       - Column data type - 1 byte         |
//! +-------------------------------------------+
//! |                  BODY                     |
//! +-------------------------------------------+
//! | N Rows:                                   |
//! |   - type -> OK (o) or ERROR (r) - 1 byte  |
//! |   - raw row in DB format for fast parse   |
//! +-------------------------------------------+
//! |               END - \r\n                  |
//! +-------------------------------------------+
//! ```

use std::io::Cursor;

use crate::{sql::schema::Schema, tcp, utils::bytes::BytesCursor, vm::query_result::QueryResult};

pub trait TcpRead {
    /// This function should validate if incoming request is correct and
    /// advance cursor position to go over request len.
    fn validate(src: &mut Cursor<&[u8]>) -> tcp::Result<()>;

    fn parse(src: &mut Cursor<&[u8]>) -> tcp::Result<Self>
    where
        Self: Sized;
}

pub trait TcpWrite {
    /// Writes `self` to cursor at ***the end*** of it.
    /// Anything that implements `write_to_buffer` can be send over tcp.
    /// The problem is if it can be later parsed safely.
    fn push_to_buffer<T: AsRef<[u8]> + AsMut<[u8]> + Extend<u8>>(&self, src: &mut BytesCursor<T>);
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
