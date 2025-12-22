use crate::{
    sql::{
        SqlError,
        types::{ValueRef, parse_value, serial::SerialType},
    },
    utils::bytes::{BytesCursor, VarInt},
};

/// Immutable view to database row.
pub struct Record<'a> {
    payload: &'a [u8],
}

/// Lazly parses values when needed. Helps to optimize parsing,
/// by reducing serializing that sometimes is just not needed.
pub struct RecordCursor {
    /// All parsed types in order. Length of vector represents, how many types
    /// were parsed starting at the beginning.
    serial_types: Vec<SerialType>,
    /// List of all offsets to types. This has the same properties as `serial_types`.
    offsets: Vec<usize>,
    header_size: VarInt,
}

impl RecordCursor {
    pub fn new() -> Self {
        Self {
            serial_types: Vec::new(),
            offsets: Vec::new(),
            header_size: 0,
        }
    }

    pub fn parse_up_to(&mut self, payload: &[u8], index: usize) -> Result<(), SqlError> {
        // type is already parsed, this is no-op
        if self.serial_types.len() > index {
            return Ok(());
        }

        let mut cursor = BytesCursor::new(payload);

        if self.serial_types.is_empty() && self.offsets.is_empty() {
            let (header_size, _) = cursor.read_varint();
            self.header_size = header_size;
            self.offsets.push(self.header_size as usize);
        }

        while self.serial_types.len() <= index {
            let (serial_type_code, _) = cursor.read_varint();

            let serial_type = SerialType::try_from(serial_type_code)?;
            let serial_type_size = serial_type.size();
            self.serial_types.push(serial_type);

            let prev_offset = *self.offsets.last().unwrap();
            self.offsets.push(serial_type_size + prev_offset);
        }

        Ok(())
    }

    pub fn get_value<'a>(
        &mut self,
        payload: &'a [u8],
        index: usize,
    ) -> Result<ValueRef<'a>, SqlError> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
}
