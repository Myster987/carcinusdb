use std::{borrow::Cow, cell::RefCell, fmt::Debug, rc::Rc};

use crate::{
    sql::{
        self,
        types::{Value, ValueRef, parse_value, serial::SerialType},
    },
    utils::{
        bytes::{BytesCursor, VarInt, varint_size},
        debug_table::DebugTable,
    },
};

pub const MAX_COLUMN_COUNT: usize = 2000;

/// Immutable view to database row.
#[derive(Clone)]
pub struct Record<'a> {
    payload: Cow<'a, [u8]>,
    cursor: Rc<RefCell<RecordCursor>>,
}

impl<'a> Record<'a> {
    pub fn new(payload: Cow<'a, [u8]>) -> Self {
        let cursor = Rc::new(RefCell::new(RecordCursor::new()));
        Self { payload, cursor }
    }

    pub fn from_borrowed(payload: &'a [u8]) -> Self {
        Self::new(Cow::Borrowed(payload))
    }

    pub fn from_owned(payload: Vec<u8>) -> Self {
        Self::new(Cow::Owned(payload))
    }

    pub fn raw(&'a self) -> &'a [u8] {
        &self.payload
    }

    pub fn get_value(&'a self, index: usize) -> ValueRef<'a> {
        self.try_get_value(index).unwrap()
    }

    pub fn try_get_value(&'a self, index: usize) -> sql::Result<ValueRef<'a>> {
        self.cursor.borrow_mut().get_value(&self.payload, index)
    }

    pub fn count(&self) -> usize {
        self.cursor.borrow_mut().len(&self.payload)
    }

    pub fn len(&self) -> usize {
        self.cursor.borrow_mut().len(&self.payload)
    }

    pub fn values(&'a self) -> Vec<ValueRef<'a>> {
        let len = self.len();
        let mut values = Vec::with_capacity(len);

        for i in 0..len {
            values.push(self.get_value(i));
        }

        values
    }

    pub fn to_owned(&self) -> Record<'static> {
        Record {
            payload: Cow::Owned(self.payload.to_vec()),
            cursor: Rc::new(RefCell::new(self.cursor.borrow().clone())),
        }
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
#[derive(Debug, Clone)]
pub struct RecordCursor {
    /// All parsed types in order. Length of vector represents, how many types
    /// were parsed starting at the beginning.
    serial_types: Vec<SerialType>,
    /// List of all offsets to types. This has the same properties as `serial_types`.
    offsets: Vec<usize>,
    header_size: VarInt,
    parsed_bytes: usize,
}

impl RecordCursor {
    pub fn new() -> Self {
        Self {
            serial_types: Vec::new(),
            offsets: Vec::new(),
            header_size: 0,
            parsed_bytes: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            serial_types: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity + 1),
            header_size: 0,
            parsed_bytes: 0,
        }
    }

    fn parse_up_to(&mut self, payload: &[u8], index: usize) -> sql::Result<()> {
        // type is already parsed, this is no-op
        if self.serial_types.len() > index {
            return Ok(());
        }

        let mut cursor = BytesCursor::new(payload);

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

    pub fn full_parse(&mut self, payload: &[u8]) -> sql::Result<()> {
        self.parse_up_to(payload, MAX_COLUMN_COUNT)
    }

    pub fn get_value<'a>(&mut self, payload: &'a [u8], index: usize) -> sql::Result<ValueRef<'a>> {
        self.parse_up_to(payload, index)?;

        if index >= self.serial_types.len() {
            return Ok(ValueRef::Null);
        }

        let serial_type = &self.serial_types[index];
        let start = self.offsets[index];
        let end = self.offsets[index + 1];

        let payload = &payload[start..end];

        parse_value(payload, serial_type)
    }

    /// Returns number of columns in this record.
    pub fn len(&mut self, payload: &[u8]) -> usize {
        let _ = self.full_parse(payload);
        self.serial_types.len()
    }
}

pub struct RecordBuilder {
    values: Vec<Value>,
}

impl RecordBuilder {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn add(&mut self, value: Value) {
        self.values.push(value);
    }

    fn calculate_header_size(size_of_serial_types: usize) -> usize {
        if size_of_serial_types < i8::MAX as usize {
            return size_of_serial_types + 1;
        }

        size_of_serial_types + varint_size(size_of_serial_types as VarInt)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut total_size = 0;
        let mut size_of_serial_types = 0;

        for value in &self.values {
            let serial_type = SerialType::from(value);
            let serial_type_varint_size = serial_type.to_varint().len();
            size_of_serial_types += serial_type_varint_size;
            total_size += serial_type_varint_size + serial_type.size();
        }

        let header_size = Self::calculate_header_size(size_of_serial_types);
        total_size += varint_size(header_size as VarInt);

        let buffer = vec![0; total_size];
        let mut cursor = BytesCursor::new(buffer);
        let mut offset_to_value = header_size;

        cursor.write_varint(header_size as VarInt);

        for value in &self.values {
            let serial_type = SerialType::from(value);
            match value {
                Value::Null | Value::Bool(_) => cursor.write_u8(serial_type.code() as u8),
                Value::Int(v) => {
                    cursor.write_u8(serial_type.code() as u8);
                    let pos = cursor.position();
                    cursor.set_position(offset_to_value);

                    let size = serial_type.size();
                    cursor.write_bytes(&(*v).to_le_bytes()[..size]);
                    offset_to_value += size;

                    cursor.set_position(pos);
                }
                Value::Blob(v) => {
                    cursor.write_varint(serial_type.code());
                    let pos = cursor.position();
                    cursor.set_position(offset_to_value);

                    let size = v.len();
                    cursor.write_bytes(v);
                    offset_to_value += size;

                    cursor.set_position(pos);
                }
                Value::Text(v) => {
                    cursor.write_varint(serial_type.code());
                    let pos = cursor.position();
                    cursor.set_position(offset_to_value);

                    let size = v.size();
                    cursor.write_bytes(v.as_str().as_bytes());
                    offset_to_value += size;

                    cursor.set_position(pos);
                }
            }
        }

        cursor.into_inner()
    }

    pub fn get(&self, index: usize) -> &Value {
        &self.values[index]
    }
}

impl Debug for RecordBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg_table = DebugTable::new();
        let mut header = Vec::new();
        let mut row = Vec::new();

        for (i, val) in self.values.iter().enumerate() {
            // let serial_type = &self.column_values[i];
            let dyn_ref: Box<dyn Debug> = Box::new(val);
            header.push(format!("column {} value", i + 1));
            row.push(dyn_ref);
        }

        header.iter().for_each(|h| dbg_table.add_column(h));

        dbg_table.insert_row(row.iter().map(|v| v.as_ref()).collect());

        dbg_table.fmt(f)
    }
}

/// Comparison of two records. They are compared field by field and when not
/// equal field is found `false` is returned. Otherwise `true` is returned.
pub fn records_equal(r1: &Record, r2: &Record) -> bool {
    let len1 = r1.len();
    let len2 = r2.len();

    if len1 != len2 {
        return false;
    }

    for i in 0..len1 {
        if r1.get_value(i) != r2.get_value(i) {
            return false;
        }
    }

    true
}

/// Comparison of two records. They are compared field by field and when not
/// equal field is found given ordering is returned.
pub fn compare_records(r1: &Record, r2: &Record) -> std::cmp::Ordering {
    let len1 = r1.len();
    let len2 = r2.len();
    let min_len = len1.min(len2);

    // Compare field by field (lexicographic order)
    for i in 0..min_len {
        let v1 = r1.get_value(i);
        let v2 = r2.get_value(i);

        match v1.cmp(&v2) {
            std::cmp::Ordering::Equal => continue,
            other => return other,
        }
    }

    // If all compared fields are equal, shorter record comes first
    len1.cmp(&len2)
}

#[cfg(test)]
mod tests {
    use crate::sql::types::text::Text;

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

        let record = Record::from_owned(buffer);

        println!("{:?}", record);

        Ok(())
    }

    #[test]
    fn test_record_builder() -> anyhow::Result<()> {
        let mut builder = RecordBuilder::new();

        builder.add(Value::Text(Text::new("Sample text".repeat(5))));
        builder.add(Value::Null);
        builder.add(Value::Bool(false));
        builder.add(Value::Bool(true));
        builder.add(Value::Int(125));

        let serialized = builder.serialize();
        let record = Record::from_borrowed(&serialized);

        println!("{:?}", serialized);
        println!("{:?}", record);
        println!("{:?}", record.get_value(0));

        Ok(())
    }
}
