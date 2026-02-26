use std::collections::HashMap;

use crate::{
    sql::{
        record::{Record, RecordBuilder},
        types::{Value, ValueType, text::Text},
    },
    storage::PageNumber,
};

pub const ROW_ID_COLUMN: &str = "row_id";

pub struct Schema {
    columns: Vec<Column>,
    index: HashMap<String, usize>,
    b_tree_root: PageNumber,
}

impl Schema {
    pub fn new(columns: Vec<Column>, b_tree_root: PageNumber) -> Self {
        let index = columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();

        Self {
            columns,
            b_tree_root,
            index,
        }
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

    pub fn preped_row_id(&mut self) {
        debug_assert!(
            self.columns[0].name != ROW_ID_COLUMN,
            "schema already has {}",
            ROW_ID_COLUMN
        );

        // let properties = ColumnPropertiesBuilder::new().

        // let row_id_col = Column::new(ROW_ID_COLUMN, ValueType::Int)
    }
}

pub struct Column {
    name: String,
    data_type: ValueType,
    properties: ColumnProperties,
}

impl Column {
    pub fn new(name: &str, data_type: ValueType, properties: ColumnProperties) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            properties,
        }
    }

    pub fn to_record(&self) -> Record<'static> {
        let mut builder = RecordBuilder::new();

        builder.add(Value::Text(Text::new(self.name.clone())));
        builder.add(Value::Int(self.data_type as u8 as i64));
        builder.add(Value::Int(self.properties.flags as i64));

        builder.serialize_to_record()
    }

    pub fn from_record(record: Record<'_>) -> Self {
        let name = record.get_value(0).to_text().to_owned();
        let data_type = ValueType::from(record.get_value(1).to_int() as u8);
        let properties = ColumnProperties::from(record.get_value(2).to_int() as u8);

        Self::new(&name, data_type, properties)
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
        self.flags ^= Self::NULL;
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
