use std::fmt::Display;

use crate::sql::{
    SqlError,
    types::{
        serial::{SerialType, SerialTypeKind},
        text::{AnyText, Text, TextKind, TextRef},
    },
};

pub mod blob;
pub mod serial;
pub mod text;

#[derive(Debug)]
pub enum ValueType {
    Null,
    Bool,
    Int,
    Blob,
    Text,
    Error,
}

#[derive(Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Blob(Vec<u8>),
    Text(Text),
}

#[derive(Debug, Clone, Copy)]
pub enum ValueRef<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Blob(&'a [u8]),
    Text(TextRef<'a>),
}

impl Value {
    pub fn as_ref<'a>(&'a self) -> ValueRef<'a> {
        match self {
            Value::Null => ValueRef::Null,
            Value::Bool(v) => ValueRef::Bool(*v),
            Value::Int(v) => ValueRef::Int(*v),
            Value::Blob(v) => ValueRef::Blob(v),
            Value::Text(v) => ValueRef::Text(TextRef::new(v.as_str(), v.kind())),
        }
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Bool(_) => ValueType::Bool,
            Value::Int(_) => ValueType::Int,
            Value::Blob(_) => ValueType::Blob,
            Value::Text(_) => ValueType::Text,
        }
    }

    pub fn as_blob(&self) -> &[u8] {
        self.try_as_blob()
            .expect("Must be called for only Value::Blob")
    }

    pub fn as_mut_blob(&mut self) -> &mut [u8] {
        self.try_as_blob_mut()
            .expect("Must be called for only Value::Blob")
    }

    pub fn try_as_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    pub fn try_as_blob_mut(&mut self) -> Option<&mut [u8]> {
        match self {
            Self::Blob(v) => Some(v.as_mut_slice()),
            _ => None,
        }
    }

    pub fn as_text(&self) -> &str {
        self.try_as_text().expect("Called on value that type ")
    }

    pub fn try_as_text(&self) -> Option<&str> {
        match self {
            Self::Text(v) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn try_as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn try_as_int(&self) -> Option<i64> {
        match self {
            Self::Int(v) => Some(*v),
            _ => None,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Bool(v) => f.write_str(if *v { "TRUE" } else { "FALSE" }),
            Self::Int(v) => write!(f, "{v}"),
            Self::Text(v) => write!(f, "\"{}\"", v.as_str()),
            Self::Blob(v) => write!(f, "BLOB_BYTES({})", v.len()),
        }
    }
}

pub trait AsValueRef {
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a>;
}

impl<'b> AsValueRef for ValueRef<'b> {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        *self
    }
}

impl AsValueRef for Value {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        self.as_ref()
    }
}

impl AsValueRef for &mut Value {
    #[inline]
    fn as_value_ref<'a>(&'a self) -> ValueRef<'a> {
        self.as_ref()
    }
}

/// Takes buffer to beginning of a value and `serial_type`. Returns reference
/// to a decoded value in result type.
pub fn parse_value<'a>(
    buffer: &'a [u8],
    serial_type: &SerialType,
) -> Result<ValueRef<'a>, SqlError> {
    match serial_type.kind() {
        SerialTypeKind::Null => Ok(ValueRef::Null),
        SerialTypeKind::BoolFalse => Ok(ValueRef::Bool(false)),
        SerialTypeKind::BoolTrue => Ok(ValueRef::Bool(true)),
        SerialTypeKind::Int8 => {
            if buffer.len() < 1 {
                return Err(SqlError::InvalidValue("INT8"));
            }
            let val = buffer[0];
            Ok(ValueRef::Int(val as i64))
        }
        SerialTypeKind::Int16 => {
            if buffer.len() < 2 {
                return Err(SqlError::InvalidValue("INT16"));
            }
            let val = u16::from_le_bytes([buffer[0], buffer[1]]);
            Ok(ValueRef::Int(val as i64))
        }
        SerialTypeKind::Int32 => {
            if buffer.len() < 4 {
                return Err(SqlError::InvalidValue("INT32"));
            }
            let val = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
            Ok(ValueRef::Int(val as i64))
        }
        SerialTypeKind::Int64 => {
            if buffer.len() < 8 {
                return Err(SqlError::InvalidValue("INT64"));
            }
            let val = i64::from_le_bytes([
                buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6],
                buffer[7],
            ]);
            Ok(ValueRef::Int(val))
        }
        SerialTypeKind::Blob => {
            let size = serial_type.size();
            if buffer.len() < size {
                return Err(SqlError::InvalidValue("BLOB"));
            }
            let val = &buffer[..size];
            Ok(ValueRef::Blob(val))
        }
        SerialTypeKind::Text => {
            let size = serial_type.size();
            if buffer.len() < size {
                return Err(SqlError::InvalidValue("TEXT"));
            }
            // SAFETY: if value was encoded with type Text, then we can be sure.
            // that it is a valid string and we can skip validation.
            let text = unsafe { str::from_utf8_unchecked(&buffer[..size]) };
            let val = TextRef::new(text, TextKind::PlainText);
            Ok(ValueRef::Text(val))
        }
    }
}
