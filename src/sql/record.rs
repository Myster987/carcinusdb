use thiserror::Error;

use crate::sql::types::serial::SerialType;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("record contains unknown column serial type")]
    InvalidSerialType,
}

// /// View to database record. By default `Record` is immutable and when you want
// /// to mutate it, just call `to_mutable`.
// ///
// /// # Field description:
// ///
// /// - data -> reference to bytes containing whole record information
// /// - header_size -> varint that stores whole size of header including itself in bytes
// /// - column_types -> decoded serial types of values in record (see `SerialType`)
// pub struct RecordCursor<'a> {
//     data: Cow<'a, [u8]>,
//     header_size: usize,
//     serial_types: Vec<SerialType>,
// }

// impl<'a> RecordCursor<'a> {
//     /// Returns number of entires in record.
//     pub fn len(&self) -> usize {
//         self.serial_types.len()
//     }

//     /// Takes slice of bytes that contains record information and returns decoded value.
//     pub fn try_from_bytes(data: &'a [u8]) -> Result<Self> {
//         let mut cursor = Cursor::new(data);

//         let (header_size, n) = read_varint(&mut cursor);
//         let header_size = header_size as usize;
//         let mut to_read = header_size - n as usize;

//         let mut serial_types = vec![];

//         while to_read > 0 {
//             let (type_code, n) = read_varint(&mut cursor);
//             let serial_type = SerialType::try_from_varint(type_code)?;
//             serial_types.push(serial_type);
//             to_read -= n as usize;
//         }

//         Ok(Self {
//             data: Cow::Borrowed(data),
//             header_size,
//             serial_types,
//         })
//     }

//     /// Calculates offset to value at given offset.
//     fn get_value_offset(&self, index: usize) -> usize {
//         let offset = self.header_size
//             + self.serial_types[..index]
//                 .iter()
//                 .map(|c| c.size())
//                 .sum::<usize>();
//         offset
//     }

//     /// Returns reference to raw bytes of value at given index.
//     pub fn get_value_raw(&self, index: usize) -> &[u8] {
//         let offset = self.get_value_offset(index);
//         let type_size = self.serial_types[index].size();

//         &self.data[offset..offset + type_size]
//     }

//     /// Returns reference to decoded value at given index.
//     pub fn get_value(&self, index: usize) -> ValueRef<'_> {
//         let offset = self.get_value_offset(index);
//         let serial_type = &self.serial_types[index];

//         value_ref_from_bytes(&self.data[offset..offset + serial_type.size()], serial_type)
//     }

//     pub fn to_mutable(self) -> RecordMutable {
//         let mut values = Vec::with_capacity(self.len());

//         for i in 0..self.len() {
//             values.push(self.get_value(i).into());
//         }

//         RecordMutable { values }
//     }
// }

pub struct Record<'a> {
    payload: &'a [u8],
}

pub struct RecordCursor {
    serial_types: Vec<SerialType>,
    offsets: Vec<usize>,
    header_size: usize,
}

impl RecordCursor {
    pub fn try_from_payload(payload: &[u8]) -> Result<Self> {
        todo!()
    }

    pub fn empty() -> Self {
        Self {
            serial_types: Vec::new(),
            offsets: Vec::new(),
            header_size: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            serial_types: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
            header_size: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
