pub struct Column {
    name: String,
    data_type: DataType,
    primary_key: bool,
}

impl Column {
    pub fn new(name: String, data_type: DataType, is_primary_key: bool) -> Self {
        Self {
            name,
            data_type,
            primary_key: is_primary_key,
        }
    }

    pub fn size_of(&self) -> usize {
        self.data_type.size_of()
    }
}

#[derive(Debug)]
pub enum DataType {
    SmallInt,
    Int,
    BigInt,
    Boolean,
    VarChar(usize),
}

impl DataType {
    pub fn size_of(&self) -> usize {
        match self {
            DataType::SmallInt => 2,
            DataType::Int => 4,
            DataType::BigInt => 8,
            DataType::Boolean => 1,
            DataType::VarChar(len) => utf8_length_prefix(*len),
        }
    }
}

/// When encoding in utf8, it can use from 1 to 4 bytes per character, so when saving to file we need to assume worst case scenario and calculate prefixes for   
pub fn utf8_length_prefix(max_length: usize) -> usize {
    match max_length {
        0..64 => 1,
        64..16384 => 2,
        _ => 4,
    }
}
