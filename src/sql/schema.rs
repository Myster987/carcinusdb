use std::collections::HashMap;

use thiserror::Error;

use crate::{
    sql::{
        record::{Record, RecordBuilder},
        types::{Value, ValueType, text::Text},
    },
    storage::PageNumber,
};

pub const ROW_ID_COLUMN: &str = "row_id";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("table \"{0}\" not found in database.")]
    TableNotFound(String),
}

pub struct Catalog {
    tables: HashMap<String, TableMetadata>,
}

impl Catalog {
    pub fn get_table(&self, name: &str) -> Result<&TableMetadata> {
        self.tables
            .get(name)
            .ok_or(Error::TableNotFound(name.to_string()))
    }
}

pub struct IndexMetadata {
    pub root: PageNumber,
    pub name: String,
    pub column: Column,
    pub schema: Schema,
    pub unique: bool,
}

pub struct TableMetadata {
    pub root: PageNumber,
    pub name: String,
    pub schema: Schema,
    pub indexes: Vec<IndexMetadata>,
}

pub struct Schema {
    columns: Vec<Column>,
    index: HashMap<String, usize>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        let index = columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();

        Self { columns, index }
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn index_of(&self, column: &str) -> Option<usize> {
        self.index.get(column).copied()
    }

    fn has_row_id(&self) -> bool {
        if let Some(col) = self.columns.first() {
            col.name == ROW_ID_COLUMN
        } else {
            false
        }
    }

    pub fn push(&mut self, column: Column) {
        self.index.insert(column.name.clone(), self.len());
        self.columns.push(column);
    }

    pub fn prepend_row_id(&mut self) {
        debug_assert!(
            self.columns.first().map(|c| c.name.as_str()) != Some(ROW_ID_COLUMN),
            "schema already has {}",
            ROW_ID_COLUMN
        );
        let row_id_col = Column::new(
            ROW_ID_COLUMN,
            ValueType::Int,
            ColumnPropertiesBuilder::new()
                .not_null()
                .primary_key()
                .build(),
            None,
        );

        self.columns.insert(0, row_id_col);
        self.index = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
    }
}

pub struct Column {
    name: String,
    data_type: ValueType,
    properties: ColumnProperties,
    default: Option<Value>,
}

impl Column {
    pub fn new(
        name: &str,
        data_type: ValueType,
        properties: ColumnProperties,
        default: Option<Value>,
    ) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            properties,
            default,
        }
    }

    pub fn to_record(&self) -> Record<'static> {
        let mut builder = RecordBuilder::new();

        builder.add(Value::Text(Text::new(self.name.clone())));
        builder.add(Value::Int(self.data_type as u8 as i64));
        builder.add(Value::Int(self.properties.flags as i64));
        builder.add(self.default.clone().unwrap_or(Value::Null));

        builder.serialize_to_record()
    }

    pub fn from_record(record: Record<'_>) -> Self {
        let name = record.get_value(0).to_text().to_owned();
        let data_type = ValueType::from(record.get_value(1).to_int() as u8);
        let properties = ColumnProperties::from(record.get_value(2).to_int() as u8);
        let default = match record.get_value(3).to_owned() {
            Value::Null => None,
            val => Some(val),
        };

        Self::new(&name, data_type, properties, default)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnProperties {
    flags: u8,
}

impl ColumnProperties {
    pub fn new(flags: u8) -> Self {
        Self { flags }
    }
}

impl ColumnProperties {
    const PRIMARY_KEY: u8 = 1;
    const NULL: u8 = 1 << 1;

    pub fn is_primary_key(&self) -> bool {
        self.flags & Self::PRIMARY_KEY != 0
    }

    pub fn set_primary_key(&mut self) {
        self.flags |= Self::PRIMARY_KEY;
    }

    pub fn is_null(&self) -> bool {
        self.flags & Self::NULL != 0
    }

    pub fn set_null(&mut self) {
        self.flags |= Self::NULL;
    }

    pub fn set_not_null(&mut self) {
        self.flags &= !Self::NULL;
    }
}

impl From<u8> for ColumnProperties {
    fn from(value: u8) -> Self {
        Self::new(value)
    }
}

impl Default for ColumnProperties {
    fn default() -> Self {
        let mut column_properties = Self::new(0);
        column_properties.set_null();
        column_properties
    }
}

pub struct ColumnPropertiesBuilder {
    column_properties: ColumnProperties,
}

impl ColumnPropertiesBuilder {
    pub fn new() -> Self {
        Self {
            column_properties: ColumnProperties::new(0),
        }
    }

    pub fn primary_key(mut self) -> Self {
        self.column_properties.set_primary_key();
        self
    }

    pub fn null(mut self) -> Self {
        self.column_properties.set_null();
        self
    }

    pub fn not_null(mut self) -> Self {
        self.column_properties.set_not_null();
        self
    }

    pub fn build(self) -> ColumnProperties {
        self.column_properties
    }
}
