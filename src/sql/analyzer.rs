use std::collections::HashSet;

use thiserror::Error;

use crate::{
    database::CARCINUSDB_MASTER_TABLE,
    sql::{
        parser::statement::{Constrains, Create, Statement},
        schema::{Catalog, ROW_ID_COLUMN},
    },
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // master
    #[error(
        "attempted to modify master \"{CARCINUSDB_MASTER_TABLE}\" table. this operation is forbidden."
    )]
    MasterTableModification,

    // create
    #[error("table \"{0}\" already exists. use different name or delete existing one.")]
    TableAlreadyExists(String),
    #[error("duplicated column names. {reason}")]
    ContainsDuplicateNames { reason: String },
    #[error("table contains multiple primary keys (support not planed).")]
    MultiplePrimaryKeys,
    #[error("table \"{0}\" not found")]
    TableNotFound(String),
    #[error("table contains {expected} columns, but attempted to insert {got}")]
    ColumnCountMismatch { expected: usize, got: usize },
    #[error("index not marked as unique (not supported yet)")]
    UniqueIndexNotSupported,
    #[error("index \"{0}\" already exists. use different name or delete existing one.")]
    IndexAlreadyExists(String),
    #[error("attempted to use column \"{0}\", that doesn't exist.")]
    ColumnNotFound(String),

    // schema
    #[error(transparent)]
    Schema(#[from] crate::sql::schema::Error),
}

pub fn analyze(statement: &Statement, catalog: &mut Catalog) -> Result<()> {
    match statement {
        Statement::Create(Create::Table { name, columns }) => {
            if let Ok(_) = catalog.get_table(name) {
                return Err(Error::TableAlreadyExists(name.to_owned()));
            };

            let mut has_primary_key = false;
            let mut duplicates = HashSet::new();

            for col in columns {
                if !duplicates.insert(&col.name) {
                    return Err(Error::ContainsDuplicateNames {
                        reason: "table can't contain duplicated names.".to_string(),
                    });
                }

                if col.constrains.contains(&Constrains::PrimaryKey) {
                    if has_primary_key {
                        return Err(Error::MultiplePrimaryKeys);
                    }
                    has_primary_key = true;
                }
            }
        }

        Statement::Create(Create::Index {
            name,
            table,
            column,
            unique,
        }) => {
            if !unique {
                return Err(Error::UniqueIndexNotSupported);
            }

            let table_metadata = catalog.get_table(table)?;

            if !table_metadata
                .schema
                .column_names()
                .iter()
                .any(|col| col == column)
            {
                return Err(Error::ColumnNotFound(column.to_owned()));
            }

            if table_metadata
                .indexes
                .iter()
                .any(|index| &index.name == name)
            {
                return Err(Error::IndexAlreadyExists(name.to_owned()));
            }
        }

        Statement::Insert {
            into,
            columns,
            values,
        } => {
            let table_metadata = catalog.get_table(into)?;

            if into == CARCINUSDB_MASTER_TABLE {
                return Err(Error::MasterTableModification);
            }

            let mut columns = columns.as_slice();

            let table_column_names;

            if columns.is_empty() {
                table_column_names = table_metadata.schema.column_names();
                columns = table_column_names.as_slice();
                if columns[0] == ROW_ID_COLUMN {
                    columns = &table_column_names[1..];
                }
            }

            let mut duplicates = HashSet::new();

            for col in columns {
                if table_metadata.index_of(col).is_none() {
                    return Err(Error::ColumnNotFound(col.to_owned()));
                }
                if !duplicates.insert(col) {
                    return Err(Error::D);
                }
            }

            let column_mismatch = values.iter().position(|v| v.len() != columns.len());

            if let Some(bad_len) = column_mismatch {
                return Err(Error::ColumnCountMismatch {
                    expected: columns.len(),
                    got: bad_len,
                });
            }

            // if columns.len() != v
        }
        _ => todo!(),
    }

    Ok(())
}
