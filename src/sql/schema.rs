use std::collections::HashMap;

use dashmap::DashMap;
use thiserror::Error;

use crate::{
    sql::{
        parser::{
            self,
            statement::{Constrains, Create, Statement},
        },
        types::{Value, ValueType},
    },
    storage::{
        self, PageNumber,
        btree::{BTreeCursor, DatabaseCursor},
        wal::transaction::ReadTx,
    },
};

pub const ROW_ID_COLUMN: &str = "row_id";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("table \"{0}\" not found in database.")]
    TableNotFound(String),
}

pub struct Catalog {
    tables: DashMap<String, TableMetadata>,
}

impl Catalog {
    pub fn get_table(&self, name: &str) -> Result<TableMetadata> {
        self.tables
            .get(name)
            .map(|t| t.value().clone())
            .ok_or(Error::TableNotFound(name.to_string()))
    }

    pub fn from_cursor<Tx: ReadTx>(mut master_cursor: BTreeCursor<Tx>) -> storage::Result<Self> {
        let tables = DashMap::new();
        let mut pending_indexes = Vec::new();

        while let Ok(moved) = master_cursor.next()
            && moved
        {
            let record = master_cursor.try_record()?;

            let r#type = record.get_value(0);
            let root_page = record.get_value(3).to_int() as PageNumber;
            let sql = record.get_value(4);

            match r#type.to_text() {
                "table" => {
                    match parser::parse(sql.to_text()).map_err(|_| storage::Error::Corruped)? {
                        Statement::Create(Create::Table { name, columns }) => {
                            let schema =
                                Schema::new(columns.into_iter().map(|col| col.into()).collect());
                            let table = TableMetadata::new(root_page, name.clone(), schema, vec![]);

                            tables.insert(name, table)
                        }
                        _ => return Err(storage::Error::Corruped),
                    };
                }
                "index" => {
                    match parser::parse(sql.to_text()).map_err(|_| storage::Error::Corruped)? {
                        Statement::Create(Create::Index {
                            name,
                            table,
                            column,
                            unique,
                        }) => {
                            pending_indexes.push(PendingIndex {
                                root: root_page,
                                name,
                                table,
                                column,
                                unique,
                            });
                        }
                        _ => return Err(storage::Error::Corruped),
                    }
                }
                _ => return Err(storage::Error::Corruped),
            }
        }

        for pending in pending_indexes {
            let mut table = tables
                .get_mut(&pending.table)
                .ok_or(storage::Error::Corruped)?;

            let index = table
                .index_of(&pending.column)
                .ok_or(storage::Error::Corruped)?;

            let column = table.schema.columns[index].clone();

            let index = IndexMetadata::new(
                pending.root,
                pending.name,
                column,
                table.schema.clone(),
                pending.unique,
            );

            table.indexes.push(index);
        }

        Ok(Self { tables })
    }
}

struct PendingIndex {
    root: PageNumber,
    name: String,
    table: String,
    column: String,
    unique: bool,
}

#[derive(Debug, Clone)]
pub struct IndexMetadata {
    pub root: PageNumber,
    pub name: String,
    pub column: Column,
    pub schema: Schema,
    pub unique: bool,
}

impl IndexMetadata {
    pub fn new(
        root: PageNumber,
        name: String,
        column: Column,
        schema: Schema,
        unique: bool,
    ) -> Self {
        Self {
            root,
            name,
            column,
            schema,
            unique,
        }
    }

    pub fn index_of(&self, column: &str) -> Option<usize> {
        self.schema.index_of(column)
    }
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub root: PageNumber,
    pub name: String,
    pub schema: Schema,
    pub indexes: Vec<IndexMetadata>,
}

impl TableMetadata {
    pub fn new(
        root: PageNumber,
        name: String,
        schema: Schema,
        indexes: Vec<IndexMetadata>,
    ) -> Self {
        Self {
            root,
            name,
            schema,
            indexes,
        }
    }

    pub fn index_of(&self, column: &str) -> Option<usize> {
        self.schema.index_of(column)
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
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

    pub fn empty() -> Self {
        Self::new(vec![])
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|col| col.name.to_owned()).collect()
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

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: ValueType,
    pub properties: ColumnProperties,
    pub default: Option<Value>,
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
}

impl From<parser::statement::Column> for Column {
    fn from(value: parser::statement::Column) -> Self {
        Self {
            name: value.name,
            data_type: value.data_type.into(),
            properties: value.constrains.into(),
            default: None,
        }
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
    const UNIQUE: u8 = 1 << 2;

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

    pub fn is_unique(&self) -> bool {
        self.flags & Self::UNIQUE != 0
    }

    pub fn set_unique(&mut self) {
        self.flags |= Self::UNIQUE;
    }
}

impl From<u8> for ColumnProperties {
    fn from(value: u8) -> Self {
        Self::new(value)
    }
}

impl From<Vec<Constrains>> for ColumnProperties {
    fn from(value: Vec<Constrains>) -> Self {
        // set to be null by default.
        let mut properties = ColumnProperties::new(Self::NULL);

        for constrain in value {
            match constrain {
                Constrains::PrimaryKey => {
                    properties.set_not_null();
                    properties.set_primary_key()
                }
                Constrains::NotNull => properties.set_not_null(),
                Constrains::Unique => properties.set_unique(),
            }
        }

        properties
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

    pub fn unique(mut self) -> Self {
        self.column_properties.set_unique();
        self
    }

    pub fn build(self) -> ColumnProperties {
        self.column_properties
    }
}
