use std::collections::HashSet;

use thiserror::Error;

use crate::{
    database::CARCINUSDB_MASTER_TABLE,
    sql::{
        parser::statement::{Constrains, Create, Statement},
        schema::Catalog,
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
    // - table
    #[error("table {0} already exists. use different name or delete existing one.")]
    TableAlreadyExists(String),
    #[error("table can't containt duplicated column names.")]
    TableContainsDuplicateNames,
    #[error("table contains multiple primary keys (support not planed).")]
    MultiplePrimaryKeys,
    #[error("table {0} not fou")]
    TableNotFound(String),

    // - index
    #[error("index not marked as unique (not supported yet)")]
    UniqueIndexNotSupported,
    #[error("index {0} already exists. use different name or delete existing one.")]
    IndexAlreadyExists(String),

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
                    return Err(Error::TableContainsDuplicateNames);
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

            let columns = columns.as_slice();
        }
        _ => todo!(),
    }

    Ok(())
}
