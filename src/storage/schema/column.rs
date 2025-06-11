use crate::storage::{Oid, schema::utf8_length_prefix};

pub struct Column {
    table_oid: Oid,
    name: String,
    data_type: DataType,
    non_null: bool,
    primary_key: bool,
}

impl Column {
    pub fn new(
        table_oid: Oid,
        name: String,
        data_type: DataType,
        non_null: bool,
        is_primary_key: bool,
    ) -> Self {
        Self {
            table_oid,
            name,
            data_type,
            non_null,
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
