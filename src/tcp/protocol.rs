//! Protocol used by CarcinusDB:
//!
//! Response format:
//!
//! ```text
//! +-------------------------------------------+
//! |    HEADER - response type ("a" or "i")    |
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

use crate::{
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
    /// Writes `self` to cursor at ***the end*** of it.
    /// Anything that implements `write_to_buffer` can be send over tcp.
    /// The problem is if it can be later parsed safely.
    fn push_to_buffer<T: AsRef<[u8]> + AsMut<[u8]> + Extend<u8>>(&self, src: &mut BytesCursor<T>);
}

pub struct Response<'a> {
    query_result: QueryResult<'a>,
}

impl<'a> Response<'a> {
    pub fn new(query_result: QueryResult<'a>) -> Self {
        Self { query_result }
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

pub struct Message {
    len: usize,
    r#type: char,
    data: Vec<u8>,
}

// impl<'a> TcpWrite for Response<'a> {
//     fn push_to_buffer<T: AsRef<[u8]> + AsMut<[u8]> + Extend<u8>>(&self, src: &mut BytesCursor<T>) {
//         src.put_u8(self.response_type());

//         match self.query_result {
//             QueryResult::RowsAffected(rows_affected) => {
//                 src.put_varint(rows_affected as VarInt);
//                 src.put_bytes(b"\r\n");
//             }
//             QueryResult::Rows(row_iterator) => {
//                 let schema = row_iterator.schema();

//                 src.put_varint(schema.len() as VarInt);

//                 for column in &schema.columns {
//                     TcpWrite::push_to_buffer(column, src);
//                 }

//                 self.stream.write_all(&buffer)?;

//                 buffer.clear();

//                 let mut cursor = BytesCursor::new(buffer);

//                 for record_result in row_iterator {
//                     match record_result {
//                         Ok(row) => {
//                             cursor.put_u8(b'o');
//                             TcpWrite::push_to_buffer(&row, &mut cursor);
//                             self.stream.write_all(cursor.get_ref())?;
//                         }
//                         Err(err) => {
//                             cursor.put_u8(b'e');
//                             let error_message = format!("{err}");

//                             cursor.put_varint(error_message.len() as VarInt);
//                             cursor.put_bytes(error_message.as_bytes());

//                             self.stream.write_all(cursor.get_ref())?;
//                             break;
//                         }
//                     }
//                     cursor.get_mut().clear();
//                 }

//                 self.stream.write_all(b"\r\n")?;
//             }
//         }
//     }
// }
