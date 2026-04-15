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
    },
    tcp::protocol,
    utils::bytes::VarInt,
};

pub const ROW_ID_COLUMN: &str = "row_id";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("table \"{0}\" not found in database.")]
    TableNotFound(String),
}

/// Collection of all tables in database that are currently loaded.
#[derive(Debug)]
pub struct Catalog {
    tables: DashMap<String, TableMetadata>,
}

impl Catalog {
    /// Returns `TableMetadata`, if table with given `name` exists.
    pub fn get_table(&self, name: &str) -> Result<TableMetadata> {
        self.tables
            .get(name)
            .map(|t| t.value().clone())
            .ok_or(Error::TableNotFound(name.to_string()))
    }

    /// Inserts new table into catalog with given `name` identifier.
    pub fn insert_table(&self, name: String, table: TableMetadata) {
        self.tables.insert(name, table);
    }

    /// Inserts new index for table with given `table_name`.
    pub fn insert_index(&self, table_name: &str, index: IndexMetadata) -> Result<()> {
        let mut table = self
            .tables
            .get_mut(table_name)
            .ok_or(Error::TableNotFound(table_name.to_string()))?;

        table.value_mut().indexes.push(index);

        Ok(())
    }

    /// Recreates catalog from b-tree cursor (must be master table).
    pub fn from_cursor(mut master_cursor: BTreeCursor) -> storage::Result<Self> {
        let tables = DashMap::new();
        let mut pending_indexes = Vec::new();

        while let Ok(moved) = master_cursor.next()
            && moved
        {
            let record = master_cursor.try_record()?;

            // master table row format
            // 0. type
            // 1. name
            // 2. table_name
            // 3. root_page
            // 4. sql

            let r#type = record.get_value(0);
            let root_page = record.get_value(3).to_int() as PageNumber;
            let sql = record.get_value(4);

            match r#type.to_text() {
                "table" => {
                    match parser::parse(sql.to_text()).map_err(|_| storage::Error::Corrupted)? {
                        Statement::Create(Create::Table { name, columns }) => {
                            let schema =
                                Schema::new(columns.into_iter().map(|col| col.into()).collect());
                            let table = TableMetadata::new(root_page, name.clone(), schema, vec![]);

                            tables.insert(name, table)
                        }
                        _ => return Err(storage::Error::Corrupted),
                    };
                }
                "index" => {
                    match parser::parse(sql.to_text()).map_err(|_| storage::Error::Corrupted)? {
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
                        _ => return Err(storage::Error::Corrupted),
                    }
                }
                _ => return Err(storage::Error::Corrupted),
            }
        }

        for pending in pending_indexes {
            let mut table = tables
                .get_mut(&pending.table)
                .ok_or(storage::Error::Corrupted)?;

            let index = table
                .index_of(&pending.column)
                .ok_or(storage::Error::Corrupted)?;

            let column = table.schema.columns[index].clone();
            let column_index = table.index_of(&column.name).unwrap();

            let index = IndexMetadata::new(
                pending.root,
                pending.name,
                column,
                column_index,
                // table.schema.clone(),
                pending.unique,
            );

            table.indexes.push(index);
        }

        Ok(Self { tables })
    }
}

/// Temporary storage for index metadata. Used by `Catalog::from_cursor`.
struct PendingIndex {
    root: PageNumber,
    name: String,
    table: String,
    column: String,
    unique: bool,
}

/// Holds necessary information that is needed to use given table index.
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    /// Root of index b-tree.
    pub root: PageNumber,
    /// Name of this index.
    pub name: String,
    /// Column that it is used on.
    pub column: Column,
    /// Index of column that it is used one relative to schema.
    pub column_index: usize,
    /// If keys must be unique (for now always true).
    pub unique: bool,
}

impl IndexMetadata {
    pub fn new(
        root: PageNumber,
        name: String,
        column: Column,
        column_index: usize,
        // schema: Schema,
        unique: bool,
    ) -> Self {
        Self {
            root,
            name,
            column,
            column_index,
            // schema,
            unique,
        }
    }
}

/// Holds necessary information that is needed to use given table.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// Root of table b-tree.
    pub root: PageNumber,
    /// Table name.
    pub name: String,
    /// Table schema.
    pub schema: Schema,
    /// Indexes that are on this table.
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

    /// Returns index of column in schema with given name.
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

    pub fn get(&self, name: &str) -> &Column {
        self.columns.iter().find(|&col| col.name == name).unwrap()
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

    pub fn from_name_and_data_type(name: &str, data_type: ValueType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            properties: ColumnProperties::default(),
            default: None,
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

impl protocol::TcpWrite for Column {
    fn push_to_buffer<T: AsRef<[u8]> + AsMut<[u8]> + Extend<u8>>(
        &self,
        src: &mut crate::utils::bytes::BytesCursor<T>,
    ) {
        let column_name_len = self.name.len();

        src.put_varint(column_name_len as VarInt);
        src.put_bytes(self.name.as_bytes());
        src.put_u8(self.data_type.into());
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

    pub fn is_not_null(&self) -> bool {
        !self.is_null()
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
