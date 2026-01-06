use std::fmt::Display;

use crate::sql::{
    self,
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
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_value_ref())
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.as_value_ref().eq(&other.as_value_ref())
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_value_ref().partial_cmp(&other.as_value_ref())
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_value_ref().cmp(&other.as_value_ref())
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

impl AsValueRef for &Value {
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
pub fn parse_value<'a>(buffer: &'a [u8], serial_type: &SerialType) -> sql::Result<ValueRef<'a>> {
    match serial_type.kind() {
        SerialTypeKind::Null => Ok(ValueRef::Null),
        SerialTypeKind::BoolFalse => Ok(ValueRef::Bool(false)),
        SerialTypeKind::BoolTrue => Ok(ValueRef::Bool(true)),
        SerialTypeKind::Int8 => {
            if buffer.len() < 1 {
                return Err(sql::Error::InvalidValue("INT8"));
            }
            let val = buffer[0];
            Ok(ValueRef::Int(val as i64))
        }
        SerialTypeKind::Int16 => {
            if buffer.len() < 2 {
                return Err(sql::Error::InvalidValue("INT16"));
            }
            let val = u16::from_le_bytes([buffer[0], buffer[1]]);
            Ok(ValueRef::Int(val as i64))
        }
        SerialTypeKind::Int32 => {
            if buffer.len() < 4 {
                return Err(sql::Error::InvalidValue("INT32"));
            }
            let val = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
            Ok(ValueRef::Int(val as i64))
        }
        SerialTypeKind::Int64 => {
            if buffer.len() < 8 {
                return Err(sql::Error::InvalidValue("INT64"));
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
                return Err(sql::Error::InvalidValue("BLOB"));
            }
            let val = &buffer[..size];
            Ok(ValueRef::Blob(val))
        }
        SerialTypeKind::Text => {
            let size = serial_type.size();
            if buffer.len() < size {
                return Err(sql::Error::InvalidValue("TEXT"));
            }
            // SAFETY: if value was encoded with type Text, then we can be sure.
            // that it is a valid string and we can skip validation.
            let text = unsafe { str::from_utf8_unchecked(&buffer[..size]) };
            let val = TextRef::new(text, TextKind::PlainText);
            Ok(ValueRef::Text(val))
        }
    }
}

impl<'a> ValueRef<'a> {
    pub fn to_owned(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(v) => Value::Bool(*v),
            Self::Int(v) => Value::Int(*v),
            Self::Blob(v) => Value::Blob(v.to_vec()),
            Self::Text(v) => Value::Text(Text::new(v.to_string())),
        }
    }

    pub fn to_value_type(&self) -> ValueType {
        match self {
            Self::Null => ValueType::Null,
            Self::Bool(_) => ValueType::Bool,
            Self::Int(_) => ValueType::Int,
            Self::Blob(_) => ValueType::Blob,
            Self::Text(_) => ValueType::Text,
        }
    }
}

impl<'a> Display for ValueRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Bool(v) => f.write_str(if *v { "TRUE" } else { "FALSE" }),
            Self::Int(v) => write!(f, "{v}"),
            Self::Text(v) => write!(f, "\"{}\"", v.as_str()),
            Self::Blob(v) => write!(f, "{}", String::from_utf8_lossy(v)),
        }
    }
}

impl<'a> PartialEq<ValueRef<'a>> for ValueRef<'a> {
    fn eq(&self, other: &ValueRef<'a>) -> bool {
        match (self, other) {
            (Self::Bool(bool_left), Self::Bool(bool_rigth)) => bool_left == bool_rigth,
            (Self::Bool(bool_left), Self::Int(int_right)) => &(*bool_left as i64) == int_right,
            (Self::Int(int_left), Self::Bool(bool_rigth)) => int_left == &(*bool_rigth as i64),
            (Self::Int(int_left), Self::Int(int_right)) => int_left == int_right,
            (Self::Int(_) | Self::Bool(_), Self::Text(_) | Self::Blob(_)) => false,
            (Self::Text(_) | Self::Blob(_), Self::Int(_) | Self::Bool(_)) => false,
            (Self::Text(text_left), Self::Text(text_right)) => {
                text_left.as_bytes() == text_right.as_bytes()
            }
            (Self::Blob(blob_left), Self::Blob(blob_right)) => blob_left.eq(blob_right),
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl<'a> Eq for ValueRef<'a> {}

impl<'a> PartialOrd<ValueRef<'a>> for ValueRef<'a> {
    fn partial_cmp(&self, other: &ValueRef<'a>) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Bool(bool_left), Self::Bool(bool_rigth)) => bool_left.partial_cmp(bool_rigth),
            (Self::Bool(bool_left), Self::Int(int_rigth)) => {
                (*bool_left as i64).partial_cmp(int_rigth)
            }
            (Self::Int(int_left), Self::Bool(bool_rigth)) => {
                int_left.partial_cmp(&(*bool_rigth as i64))
            }
            (Self::Int(int_left), Self::Int(int_rigth)) => int_left.partial_cmp(int_rigth),
            // Numeric to Text/Blob
            (Self::Int(_) | Self::Bool(_), Self::Text(_) | Self::Blob(_)) => {
                Some(std::cmp::Ordering::Less)
            }
            (Self::Text(_) | Self::Blob(_), Self::Int(_) | Self::Bool(_)) => {
                Some(std::cmp::Ordering::Greater)
            }
            (Self::Text(text_left), Self::Text(text_rigth)) => {
                text_left.as_bytes().partial_cmp(text_rigth.as_bytes())
            }
            // Text to Blob
            (Self::Text(_), Self::Blob(_)) => Some(std::cmp::Ordering::Less),
            (Self::Blob(_), Self::Text(_)) => Some(std::cmp::Ordering::Greater),

            (Self::Blob(blob_left), Self::Blob(blob_rigth)) => blob_left.partial_cmp(blob_rigth),
            (Self::Null, Self::Null) => Some(std::cmp::Ordering::Equal),
            (Self::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Self::Null) => Some(std::cmp::Ordering::Greater),
        }
    }
}

impl<'a> Ord for ValueRef<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::record::RecordBuilder;

    use super::*;

    #[test]
    fn test_value_comparison() -> anyhow::Result<()> {
        let mut test_record = RecordBuilder::new();

        test_record.add(Value::Int(123));
        test_record.add(Value::Bool(true));
        test_record.add(Value::Text(Text::new("123".into())));

        println!("{:?}", test_record);

        println!("{:?}", test_record.get(0));
        println!("{:?}", test_record.get(1));
        println!("{:?}", test_record.get(2));

        assert!(*test_record.get(0) == Value::Int(123));
        println!("{:?}", test_record.get(0).cmp(&Value::Int(125)));
        println!(
            "{:?}",
            test_record
                .get(2)
                .cmp(&Value::Text(Text::new("124".into())))
        );

        Ok(())
    }
}
