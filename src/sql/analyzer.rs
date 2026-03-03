use std::{collections::HashSet, ops::Deref};

use thiserror::Error;

use crate::{
    database::CARCINUSDB_MASTER_TABLE,
    sql::{
        parser::statement::{
            BinaryOperator, Constrains, Create, DataType, Expression, Statement, UnaryOperator,
        },
        schema::{Catalog, ROW_ID_COLUMN, Schema, TableMetadata},
        types::{Value, ValueType},
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
    #[error("attempted to modify hidden \"row_id\" column.")]
    RowIdAssignment,

    // schema
    #[error(transparent)]
    Schema(#[from] crate::sql::schema::Error),

    // types
    #[error(transparent)]
    TypeError(#[from] TypeError),
}

#[derive(Debug, Error)]
pub enum TypeError {
    // CannotApplyUnary {
    //     operator: UnaryOperator,
    //     value: Value,
    // }
    #[error("cannot apply this binary operator: {left} {operator} {right}")]
    CannotApplyBinary {
        left: Expression,
        operator: BinaryOperator,
        right: Expression,
    },
    #[error("expected type: {expected:?}, but found: {found}")]
    ExpectedType {
        expected: ValueType,
        found: Expression,
    },
    #[error("unexpected expression: {expr}")]
    UnexpectedExpression { expr: Expression },
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

                if col.name == ROW_ID_COLUMN {
                    return Err(Error::RowIdAssignment);
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
                    return Err(Error::ContainsDuplicateNames {
                        reason: "attempted to insert while selecting duplicated columns"
                            .to_string(),
                    });
                }
            }

            let column_mismatch = values.iter().position(|v| v.len() != columns.len());

            if let Some(bad_len) = column_mismatch {
                return Err(Error::ColumnCountMismatch {
                    expected: columns.len(),
                    got: bad_len,
                });
            }

            let table_len = if table_metadata.schema.columns[0].name == ROW_ID_COLUMN {
                table_metadata.schema.len() - 1
            } else {
                table_metadata.schema.len()
            };

            if table_len != columns.len() {
                return Err(Error::ColumnCountMismatch {
                    expected: table_len,
                    got: columns.len(),
                });
            }

            for row in values {
                for (expr, col) in row.iter().zip(columns) {}
            }
        }
        _ => todo!(),
    }

    Ok(())
}

fn analyze_assignment(
    table: &TableMetadata,
    column: &str,
    value: &Expression,
    allow_identifier: bool,
) -> Result<()> {
    if column == ROW_ID_COLUMN {
        return Err(Error::RowIdAssignment);
    }

    let index = table
        .index_of(column)
        .ok_or(Error::ColumnNotFound(column.to_owned()))?;

    let expected_data_type = table.schema.columns[index].data_type;

    let found_data_type = if allow_identifier {
        analyze_expression(&table.schema, value)
    } else {
        analyze_expression(&Schema::empty(), value)
    };

    todo!()
}

fn analyze_expression(schema: &Schema, expr: &Expression) -> Result<ValueType> {
    Ok(match expr {
        Expression::Value(val) => val.value_type(),

        Expression::Identifier(ident) => {
            let index = schema
                .index_of(ident)
                .ok_or(Error::ColumnNotFound(ident.to_owned()))?;

            schema.columns[index].data_type
        }

        Expression::UnaryOperation { operator, expr } => {
            if !matches!(analyze_expression(schema, expr)?, ValueType::Int) {
                return Err(Error::TypeError(TypeError::ExpectedType {
                    expected: ValueType::Int,
                    found: *expr.clone(),
                }));
            }
            ValueType::Null
        }

        Expression::BinaryOperation {
            left,
            operator,
            rigth,
        } => {
            let left_data_type = analyze_expression(schema, left)?;
            let right_data_type = analyze_expression(schema, rigth)?;

            if left_data_type != right_data_type {
                return Err(Error::TypeError(TypeError::CannotApplyBinary {
                    left: *left.clone(),
                    operator: *operator,
                    right: *rigth.clone(),
                }));
            }

            match operator {
                BinaryOperator::Eq
                | BinaryOperator::Neq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => ValueType::Bool,

                BinaryOperator::And | BinaryOperator::Or if left_data_type == ValueType::Bool => {
                    ValueType::Bool
                }

                BinaryOperator::Add
                | BinaryOperator::Sub
                | BinaryOperator::Div
                | BinaryOperator::Mul
                    if left_data_type == ValueType::Int =>
                {
                    ValueType::Int
                }

                _ => {
                    return Err(Error::TypeError(TypeError::CannotApplyBinary {
                        left: *left.clone(),
                        operator: *operator,
                        right: *rigth.clone(),
                    }));
                }
            }
        }

        Expression::Nested(expr) => analyze_expression(schema, expr)?,

        Expression::Wildcard => {
            return Err(Error::TypeError(TypeError::UnexpectedExpression {
                expr: Expression::Wildcard,
            }));
        }
    })
}
