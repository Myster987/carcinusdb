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

use crate::{
    sql::schema::Schema,
    tcp,
    utils::bytes::{BytesCursor, VarInt, encode_to_varint},
    vm::operator::Row,
};

pub trait Decode: Sized {
    /// This function should validate if incoming request is correct and
    /// advance cursor position to go over request len and returns parsed
    /// data if correct.
    fn decode(src: &mut BytesCursor<&[u8]>) -> tcp::Result<Self>;
}

pub trait Encode {
    /// Writes `self` to cursor at ***the end*** of it.
    /// Anything that implements `write_to_buffer` can be send over tcp.
    /// The problem is if it can be later parsed safely.
    fn encode(&self, dst: &mut BytesCursor<Vec<u8>>);
}

#[repr(u8)]
pub enum MessageType {
    Query = b'Q',
    Schema = b'S',
    Column = b'C',
    Record = b'R',
    Error = b'E',
    RowsAffected = b'A',
    End = b'Z',
}

pub enum Request<'a> {
    Query(&'a str),
    Close,
}

pub enum Response {
    Schema(Schema),
    Row(Row),
    RowsAffected(usize),
    Error(String),
    End,
}

impl Decode for Response {
    fn decode(src: &mut BytesCursor<&[u8]>) -> tcp::Result<Self> {
        let position = src.position();

        let (response_size, _) = src.try_read_varint().map_err(|_| tcp::Error::Incomplete)?;

        todo!()
    }
}

impl Encode for Response {
    fn encode(&self, dst: &mut BytesCursor<Vec<u8>>) {
        match self {
            Self::Schema(schema) => {
                schema.encode(dst);
            }
            Self::Row(row) => {
                row.encode(dst);
            }
            Self::RowsAffected(n) => {
                let rows_affected_varint = encode_to_varint(*n as VarInt);
                dst.put_varint(rows_affected_varint.len() as VarInt);
                dst.put_u8(MessageType::RowsAffected as u8);
                dst.put_bytes(&rows_affected_varint);
            }
            Self::Error(err) => {
                dst.put_varint(err.len() as VarInt);
                dst.put_u8(MessageType::Error as u8);
                dst.put_bytes(err.as_bytes());
            }
            Self::End => {
                dst.put_varint(1);
                dst.put_u8(MessageType::End as u8);
            }
        }
    }
}
