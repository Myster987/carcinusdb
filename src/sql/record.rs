use std::{fmt::Debug, io::Cursor};

use thiserror::Error;

use crate::{
    sql::statement::{Value, ValueRef},
    utils::{
        bytes::{self, VarInt, read_varint, write_varint},
        debug_table::DebugTable,
    },
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("record contains unknown column serial type")]
    InvalidSerialType,
}

/// Possible types of columns in record format. [Format details](https://www.sqlite.org/fileformat.html#record_format).
#[derive(Debug)]
pub enum SerialType {
    /// N = 0
    Null,
    /// N = 1
    Int8,
    /// N = 2
    Int16,
    /// N = 3
    Int32,
    /// N = 4
    Int64,
    /// N >= 12 and even -> len = (N-12) / 2
    Blob(usize),
    /// N >= 13 and odd -> len = (N-13) / 2
    String(usize),
}

impl SerialType {
    /// Creates type from given varint. If `value` doesn't match any type,
    /// error is returned.
    fn try_from_varint(value: VarInt) -> Result<Self> {
        let serial_type = match value {
            0 => Self::Null,
            1 => Self::Int8,
            2 => Self::Int16,
            3 => Self::Int32,
            4 => Self::Int64,
            n if n >= 12 && n % 2 == 0 => Self::Blob(((n - 12) / 2) as usize),
            n if n >= 13 && n % 2 != 0 => Self::String(((n - 13) / 2) as usize),
            _ => return Err(Error::InvalidSerialType),
        };

        Ok(serial_type)
    }

    /// Converts `&self` into varint with proper encoding that can be stored on disk.
    fn to_type_code(&self) -> VarInt {
        match self {
            Self::Null => 0,
            Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int32 => 3,
            Self::Int64 => 4,
            Self::Blob(len) => ((len * 2) + 12) as VarInt,
            Self::String(len) => ((len * 2) + 13) as VarInt,
        }
    }

    /// Writtes type to beginning of a buffer and returns how many bytes were written.
    fn write_to_buffer(&self, buffer: &mut [u8]) -> usize {
        let type_code = self.to_type_code();
        write_varint(buffer, type_code)
    }

    /// Returns size of given type in **bytes**.
    fn size(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int32 => 4,
            Self::Int64 => 8,
            Self::Blob(n) => *n,
            Self::String(n) => *n,
        }
    }
}

/// Immutable view to database record. Record starts with `header_size` varint,
/// that includes total size of header in bytes.
pub struct Record<'a> {
    data: &'a [u8],
    header_size: usize,
    column_values: Vec<SerialType>,
}

impl<'a> Record<'a> {
    /// Creates record from byte slice. Decoding is done in sqlite style.
    pub fn try_from_bytes(data: &'a [u8]) -> Result<Self> {
        let mut cursor = Cursor::new(data);

        let (header_size, n) = read_varint(&mut cursor);
        let header_size = header_size as usize;
        let mut to_read = header_size - n as usize;

        let mut column_values = vec![];

        while to_read > 0 {
            let (type_code, n) = read_varint(&mut cursor);
            column_values.push(SerialType::try_from_varint(type_code)?);
            to_read -= n as usize;
        }

        Ok(Self {
            data,
            header_size,
            column_values,
        })
    }

    /// Calculates offset to value at given offset.
    fn get_value_offset(&self, index: usize) -> usize {
        let offset = self.header_size
            + self.column_values[..index]
                .iter()
                .map(|c| c.size())
                .sum::<usize>();
        offset
    }

    /// Returns reference to raw bytes of value at given index.
    pub fn get_value_raw(&self, index: usize) -> &[u8] {
        let offset = self.get_value_offset(index);
        let type_size = self.column_values[index].size();

        &self.data[offset..offset + type_size]
    }

    /// Returns reference to decoded value at given index.
    pub fn get_value(&self, index: usize) -> ValueRef<'_> {
        let offset = self.get_value_offset(index);
        let serial_type = &self.column_values[index];

        value_ref_from_bytes(&self.data[offset..offset + serial_type.size()], serial_type)
    }
}

/// Decodes record from raw bytes and given serial type.
///
/// # Panics
///
/// When given bytes doesn't match record encoding.
fn value_ref_from_bytes<'a>(bytes: &'a [u8], serial_type: &SerialType) -> ValueRef<'a> {
    let mut cursor = Cursor::new(bytes);
    match serial_type {
        SerialType::Null => ValueRef::Null,
        SerialType::Int8 => {
            ValueRef::Number(bytes::get_u8(&mut cursor).expect("This shouldn't be empty") as i128)
        }
        SerialType::Int16 => {
            ValueRef::Number(bytes::get_u16(&mut cursor).expect("This shouldn't be empty") as i128)
        }
        SerialType::Int32 => {
            ValueRef::Number(bytes::get_u32(&mut cursor).expect("This shouldn't be empty") as i128)
        }
        SerialType::Int64 => {
            ValueRef::Number(bytes::get_u64(&mut cursor).expect("This shouldn't be empty") as i128)
        }
        SerialType::Blob(_) => ValueRef::Blob(bytes),
        SerialType::String(_) => {
            ValueRef::String(str::from_utf8(bytes).expect("This shouldn't panic"))
        }
    }
}

impl<'a> Debug for Record<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg_table = DebugTable::new();
        let mut header = Vec::new();
        let mut row = Vec::new();

        for i in 0..self.column_values.len() {
            // let serial_type = &self.column_values[i];
            let dyn_ref: Box<dyn Debug> = Box::new(self.get_value(i));
            header.push(format!("column {} value", i + 2));
            row.push(dyn_ref);
        }

        header.iter().for_each(|h| dbg_table.add_column(h));

        dbg_table.insert_row(row.iter().map(|v| v.as_ref()).collect());

        dbg_table.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serial_type() -> anyhow::Result<()> {
        let mut buffer = vec![0; 8];
        let value = 15;

        write_varint(&mut buffer, value);

        let (type_code, _) = read_varint(&mut buffer.as_slice());

        let serial_type = SerialType::try_from_varint(type_code)?;

        assert!(serial_type.to_type_code() == value);

        Ok(())
    }

    #[test]
    fn test_record_debug() -> anyhow::Result<()> {
        let buffer = &[4, 1, 0, 15, 15, 65];

        let record = Record::try_from_bytes(buffer).unwrap();

        println!("{:?}", record);

        Ok(())
    }
}
