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

use bytes::{Buf, BytesMut};

use crate::{
    sql::{record::Record, schema::Schema},
    tcp,
    utils::bytes::{BytesCursor, VarInt, encode_to_varint, read_varint},
    vm::operator::Row,
};

pub trait Decode: Sized {
    /// This function should validate if incoming request is correct and
    /// advance cursor position to go over request len and returns parsed
    /// data if correct.
    fn decode(src: &mut BytesMut) -> tcp::Result<Self>;
}

pub trait Encode {
    /// Writes `self` to cursor at ***the end*** of it.
    /// Anything that implements `write_to_buffer` can be send over tcp.
    /// The problem is if it can be later parsed safely.
    fn encode(&self, dst: &mut BytesMut);
}

#[repr(u8)]
pub enum MessageType {
    // Query = b'Q',
    Schema = b'S',
    Column = b'C',
    Record = b'R',
    Err = b'E',
    RowsAffected = b'A',
    End = b'Z',
}

impl TryFrom<u8> for MessageType {
    type Error = tcp::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            // b'Q' => Self::Query,
            b'S' => Self::Schema,
            b'C' => Self::Column,
            b'R' => Self::Record,
            b'E' => Self::Err,
            b'A' => Self::RowsAffected,
            b'Z' => Self::End,
            _ => return Err(Self::Error::Corrupted),
        })
    }
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
    fn decode(src: &mut BytesMut) -> tcp::Result<Self> {
        let raw = src.chunk();

        let (response_size, varint_len) = read_varint(raw).map_err(|_| tcp::Error::Incomplete)?;

        let total_needed = varint_len + size_of::<MessageType>() + response_size as usize;

        if total_needed < src.remaining() {
            return Err(tcp::Error::Incomplete);
        }

        src.advance(varint_len);

        let message_type = MessageType::try_from(src.get_u8())?;

        Ok(match message_type {
            MessageType::Schema => {
                let schema = Schema::decode(src)?;
                Response::Schema(schema)
            }
            MessageType::Record => {
                let record = Record::decode(src)?;
                Response::Row(record)
            }
            MessageType::RowsAffected => {
                let (rows_affected, _) = read_varint(buf)
                Response::RowsAffected(rows_affected as usize)
            }
            MessageType::Err => {
                let mut buffer = vec![0; response_size as usize];

                src.try_read_exact(&mut buffer).map_err(|_| {
                    src.set_position(position);
                    tcp::Error::Incomplete
                })?;

                let error_message = String::from_utf8(buffer).map_err(|_| {
                    src.set_position(position);
                    tcp::Error::Corrupted
                })?;

                Response::Error(error_message)
            }
            MessageType::End => Response::End,

            _ => unreachable!(),
        })
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
                dst.put_u8(MessageType::Err as u8);
                dst.put_bytes(err.as_bytes());
            }
            Self::End => {
                dst.put_varint(1);
                dst.put_u8(MessageType::End as u8);
            }
        }
    }
}
