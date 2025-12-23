use std::{cell::RefCell, fmt::Debug};

use crate::{
    sql::{
        SqlError,
        types::{ValueRef, parse_value, serial::SerialType},
    },
    utils::{
        bytes::{BytesCursor, VarInt},
        debug_table::DebugTable,
    },
};

pub const MAX_COLUMN_COUNT: usize = 2000;

/// Immutable view to database row.
pub struct Record<'a> {
    cursor: RefCell<RecordCursor<'a>>,
}

impl<'a> Record<'a> {
    pub fn new(payload: &'a [u8]) -> Self {
        let cursor = RefCell::new(RecordCursor::new(payload));
        Self { cursor }
    }

    pub fn get_value(&self, index: usize) -> ValueRef<'a> {
        self.try_get_value(index).unwrap()
    }

    pub fn try_get_value(&self, index: usize) -> Result<ValueRef<'a>, SqlError> {
        self.cursor.borrow_mut().get_value(index)
    }

    pub fn count(&self) -> usize {
        self.cursor.borrow_mut().len()
    }

    pub fn len(&self) -> usize {
        self.cursor.borrow_mut().len()
    }
}

impl<'a> Debug for Record<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg_table = DebugTable::new();
        let mut header = Vec::new();
        let mut row = Vec::new();

        for i in 0..self.len() {
            // let serial_type = &self.column_values[i];
            let dyn_ref: Box<dyn Debug> = Box::new(self.get_value(i));
            header.push(format!("column {} value", i + 1));
            row.push(dyn_ref);
        }

        header.iter().for_each(|h| dbg_table.add_column(h));

        dbg_table.insert_row(row.iter().map(|v| v.as_ref()).collect());

        dbg_table.fmt(f)
    }
}

/// Lazly parses values when needed. Helps to optimize parsing,
/// by reducing serializing that sometimes is just not needed.
pub struct RecordCursor<'a> {
    payload: &'a [u8],
    /// All parsed types in order. Length of vector represents, how many types
    /// were parsed starting at the beginning.
    serial_types: Vec<SerialType>,
    /// List of all offsets to types. This has the same properties as `serial_types`.
    offsets: Vec<usize>,
    header_size: VarInt,
    parsed_bytes: usize,
}

impl<'a> RecordCursor<'a> {
    pub fn new(payload: &'a [u8]) -> Self {
        Self {
            payload,
            serial_types: Vec::new(),
            offsets: Vec::new(),
            header_size: 0,
            parsed_bytes: 0,
        }
    }

    pub fn with_capacity(payload: &'a [u8], capacity: usize) -> Self {
        Self {
            payload,
            serial_types: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity + 1),
            header_size: 0,
            parsed_bytes: 0,
        }
    }

    fn parse_up_to(&mut self, index: usize) -> Result<(), SqlError> {
        // type is already parsed, this is no-op
        if self.serial_types.len() > index {
            return Ok(());
        }

        let mut cursor = BytesCursor::new(self.payload);

        if self.serial_types.is_empty() && self.offsets.is_empty() {
            let (header_size, bytes_read) = cursor.read_varint();
            self.header_size = header_size;
            self.parsed_bytes += bytes_read;
            self.offsets.push(self.header_size as usize);
        }

        cursor.set_position(self.parsed_bytes);

        while self.serial_types.len() <= index && self.parsed_bytes < self.header_size as usize {
            let (serial_type_code, bytes_read) = cursor.try_read_varint()?;

            let serial_type = SerialType::try_from(serial_type_code)?;
            let serial_type_size = serial_type.size();
            self.parsed_bytes += bytes_read;
            self.serial_types.push(serial_type);

            let prev_offset = *self.offsets.last().unwrap();
            self.offsets.push(serial_type_size + prev_offset);
        }

        Ok(())
    }

    pub fn full_parse(&mut self) -> Result<(), SqlError> {
        self.parse_up_to(MAX_COLUMN_COUNT)
    }

    pub fn get_value(&mut self, index: usize) -> Result<ValueRef<'a>, SqlError> {
        self.parse_up_to(index)?;

        if index >= self.serial_types.len() {
            return Ok(ValueRef::Null);
        }

        let serial_type = &self.serial_types[index];
        let start = self.offsets[index];
        let end = self.offsets[index + 1];

        let payload = &self.payload[start..end];

        parse_value(payload, serial_type)
    }

    pub fn len(&mut self) -> usize {
        let _ = self.full_parse();
        self.serial_types.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record() -> anyhow::Result<()> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&crate::utils::bytes::encode_to_varint(5));
        buffer.extend_from_slice(&crate::utils::bytes::encode_to_varint(0));
        buffer.extend_from_slice(&crate::utils::bytes::encode_to_varint(1));
        buffer.extend_from_slice(&crate::utils::bytes::encode_to_varint(2));
        buffer.extend_from_slice(&crate::utils::bytes::encode_to_varint(3));

        buffer.extend_from_slice(&crate::utils::bytes::encode_to_varint(125));

        let record = Record::new(&buffer);

        println!("{:?}", record);

        Ok(())
    }
}
