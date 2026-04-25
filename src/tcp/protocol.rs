//! Protocol used by CarcinusDB.
//!
//! Every message (both requests and responses) shares the same frame layout:
//!
//! ```text
//! +-----------------------------------+
//! | payload_len  - varint             |  length of everything after this field
//! | message_type - 1 byte (ASCII)     |  see table below
//! | payload      - payload_len bytes  |
//! +-----------------------------------+
//! ```
//!
//! Message types:
//!
//! ```text
//! | Tag | Name         | Direction        | Payload                              |
//! |-----|--------------|------------------|--------------------------------------|
//! | 'Q' | Query        | Client → Server  | UTF-8 SQL string                     |
//! | 'X' | Close        | Client → Server  | (empty)                              |
//! | 'S' | Schema       | Server → Client  | varint col_count, then per column:   |
//! |     |              |                  |   varint name_len, name bytes,       |
//! |     |              |                  |   1 byte data type                   |
//! | 'R' | Record       | Server → Client  | raw row in DB serialization format   |
//! | 'A' | RowsAffected | Server → Client  | varint row count                     |
//! | 'E' | Error        | Server → Client  | UTF-8 error message                  |
//! | 'Z' | End          | Server → Client  | (empty) — terminates a response      |
//! ```
//!
//! A typical successful SELECT exchange looks like:
//!
//! ```text
//! Client:  [Q] "SELECT * FROM users"
//! Server:  [S] schema
//!          [R] row
//!          [R] row
//!          ...
//!          [Z] end
//! ```
//!
//! A DML statement (INSERT / UPDATE / DELETE):
//!
//! ```text
//! Client:  [Q] "INSERT INTO ..."
//! Server:  [A] rows affected
//!          [Z] end
//! ```
//!
//! On error:
//!
//! ```text
//! Client:  [Q] "SELECT * FROM nonexistent"
//! Server:  [E] error message
//!          [Z] end
//! ```

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    sql::{record::Record, schema::Schema},
    tcp,
    utils::bytes::{VarInt, VarintBuf, encode_to_varint, read_varint},
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
    Query = b'Q',
    Schema = b'S',
    Column = b'C',
    Record = b'R',
    Err = b'E',
    RowsAffected = b'A',
    End = b'Z',
    Close = b'X',
}

impl TryFrom<u8> for MessageType {
    type Error = tcp::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            b'Q' => Self::Query,
            b'S' => Self::Schema,
            b'C' => Self::Column,
            b'R' => Self::Record,
            b'E' => Self::Err,
            b'A' => Self::RowsAffected,
            b'Z' => Self::End,
            b'X' => Self::Close,
            _ => return Err(Self::Error::Corrupted),
        })
    }
}

#[derive(Debug)]
pub enum Request {
    Query(String),
    Close,
    // End,
}

impl Decode for Request {
    fn decode(src: &mut BytesMut) -> tcp::Result<Self> {
        let raw = src.chunk();
        let (request_size, varint_len) = read_varint(raw).map_err(|_| tcp::Error::Incomplete)?;

        let total_needed = varint_len + size_of::<MessageType>() + request_size as usize;

        if src.remaining() < total_needed {
            return Err(tcp::Error::Incomplete);
        }
        src.advance(varint_len);

        let message_type = MessageType::try_from(src.get_u8())?;

        Ok(match message_type {
            MessageType::Query => {
                let buffer = src.copy_to_bytes(request_size as usize).to_vec();
                let sql = String::from_utf8(buffer).map_err(|_| tcp::Error::Corrupted)?;
                Request::Query(sql)
            }
            MessageType::Close => Request::Close,
            // MessageType::End => Request::End,
            _ => return Err(tcp::Error::Corrupted),
        })
    }
}

impl Encode for Request {
    fn encode(&self, dst: &mut BytesMut) {
        match self {
            Self::Query(sql) => {
                dst.put_varint(sql.len() as VarInt);
                dst.put_u8(MessageType::Query as u8);
                dst.put_slice(sql.as_bytes());
            }
            // Self::End => {
            //     dst.put_varint(0);
            //     dst.put_u8(MessageType::End as u8);
            // }
            Self::Close => {
                dst.put_varint(0);
                dst.put_u8(MessageType::Close as u8);
            }
        }
    }
}

#[derive(Debug)]
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

        if src.remaining() < total_needed {
            return Err(tcp::Error::Incomplete);
        }

        // src.advance(varint_len);

        // let message_type = MessageType::try_from(src.get_u8())?;

        let message_type = MessageType::try_from(raw[varint_len])?;

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
                src.advance(varint_len + 1);
                let (rows_affected, _) = src.read_varint();
                Response::RowsAffected(rows_affected as usize)
            }
            MessageType::Err => {
                src.advance(varint_len + 1);
                let buffer = src.copy_to_bytes(response_size as usize).to_vec();
                let error_message = String::from_utf8(buffer).map_err(|_| tcp::Error::Corrupted)?;
                Response::Error(error_message)
            }
            MessageType::End => {
                src.advance(varint_len + 1);
                Response::End
            }

            _ => return Err(tcp::Error::Corrupted),
        })
    }
}

impl Encode for Response {
    fn encode(&self, dst: &mut BytesMut) {
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
                dst.put_slice(&rows_affected_varint);
            }
            Self::Error(err) => {
                dst.put_varint(err.len() as VarInt);
                dst.put_u8(MessageType::Err as u8);
                dst.put_slice(err.as_bytes());
            }
            Self::End => {
                dst.put_varint(0);
                dst.put_u8(MessageType::End as u8);
            }
        }
    }
}

pub struct CarcinusClientCodec;

impl tokio_util::codec::Decoder for CarcinusClientCodec {
    type Item = Response;
    type Error = tcp::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Response::decode(src) {
            Ok(res) => Ok(Some(res)),
            Err(tcp::Error::Incomplete) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl tokio_util::codec::Encoder<Request> for CarcinusClientCodec {
    type Error = tcp::Error;

    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

pub struct CarcinusServerCodec;

impl tokio_util::codec::Decoder for CarcinusServerCodec {
    type Item = Request;
    type Error = tcp::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Request::decode(src) {
            Ok(req) => Ok(Some(req)),
            Err(tcp::Error::Incomplete) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl tokio_util::codec::Encoder<Response> for CarcinusServerCodec {
    type Error = tcp::Error;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}
