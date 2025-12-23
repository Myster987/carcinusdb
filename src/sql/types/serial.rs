use crate::{
    sql::{
        SqlError,
        types::{AsValueRef, ValueRef},
    },
    utils::bytes::{VarInt, encode_to_varint},
};

#[derive(Debug)]
pub struct SerialType(VarInt);

impl SerialType {
    const NULL: Self = Self(0);
    const BOOL_FALSE: Self = Self(1);
    const BOOL_TRUE: Self = Self(2);
    const I8: Self = Self(3);
    const I16: Self = Self(4);
    const I32: Self = Self(5);
    const I64: Self = Self(6);

    pub fn null() -> Self {
        Self::NULL
    }
    pub fn bool_false() -> Self {
        Self::BOOL_FALSE
    }
    pub fn bool_true() -> Self {
        Self::BOOL_TRUE
    }

    pub fn int8() -> Self {
        Self::I8
    }

    pub fn int16() -> Self {
        Self::I16
    }
    pub fn int32() -> Self {
        Self::I32
    }
    pub fn int64() -> Self {
        Self::I64
    }
    pub fn blob(size: VarInt) -> Self {
        Self(size * 2 + 12)
    }
    pub fn text(size: VarInt) -> Self {
        Self(size * 2 + 13)
    }

    pub fn code(&self) -> VarInt {
        self.0
    }

    pub fn kind(&self) -> SerialTypeKind {
        match self.0 {
            0 => SerialTypeKind::Null,
            1 => SerialTypeKind::BoolFalse,
            2 => SerialTypeKind::BoolTrue,
            3 => SerialTypeKind::Int8,
            4 => SerialTypeKind::Int16,
            5 => SerialTypeKind::Int32,
            6 => SerialTypeKind::Int64,
            n if n >= 12 => match n % 2 {
                0 => SerialTypeKind::Blob,
                1 => SerialTypeKind::Text,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    pub fn size(&self) -> usize {
        match self.kind() {
            SerialTypeKind::Null => 0,
            SerialTypeKind::BoolFalse => 0,
            SerialTypeKind::BoolTrue => 0,
            SerialTypeKind::Int8 => 1,
            SerialTypeKind::Int16 => 2,
            SerialTypeKind::Int32 => 4,
            SerialTypeKind::Int64 => 8,
            SerialTypeKind::Blob => (self.0 as usize - 12) / 2,
            SerialTypeKind::Text => (self.0 as usize - 13) / 2,
        }
    }

    pub fn to_varint(&self) -> Vec<u8> {
        encode_to_varint(self.0)
    }
}

/// Possible types of columns in record format. Altered a bit, because why not.
/// [Format details](https://www.sqlite.org/fileformat.html#record_format).
/// Each filed is documented with `N` which represent type encoded as varint.
#[derive(Debug)]
pub enum SerialTypeKind {
    /// N = 0
    Null,
    /// N = 1
    BoolFalse,
    /// N = 2
    BoolTrue,
    /// N = 3
    Int8,
    /// N = 4
    Int16,
    /// N = 5
    Int32,
    /// N = 6
    Int64,
    /// N >= 12 and even -> len = (N-12) / 2
    Blob,
    /// N >= 13 and odd -> len = (N-13) / 2
    Text,
}

impl<T: AsValueRef> From<T> for SerialType {
    fn from(value: T) -> Self {
        let value = value.as_value_ref();
        match value {
            ValueRef::Null => SerialType::null(),
            ValueRef::Bool(v) => {
                if v {
                    SerialType::bool_true()
                } else {
                    SerialType::bool_false()
                }
            }
            ValueRef::Int(v) => match v {
                -128..=127 => SerialType::int8(),
                -32768..=32767 => SerialType::int16(),
                -2147483648..=2147483647 => SerialType::int32(),
                -9223372036854775808..=9223372036854775807 => SerialType::int64(),
            },
            ValueRef::Blob(v) => SerialType::blob(v.len() as VarInt),
            ValueRef::Text(v) => SerialType::text(v.len() as VarInt),
        }
    }
}

impl TryFrom<u64> for SerialType {
    type Error = SqlError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if (7..12).contains(&value) {
            return Err(Self::Error::InvalidSerialType);
        }
        Ok(Self(value))
    }
}
