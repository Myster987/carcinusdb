use std::fmt::{Display, Write};

use crate::sql::types::Value;

/// SQL statement.
#[derive(Debug)]
pub enum Statement {
    Select {
        columns: Vec<Expression>,
        from: String,
        r#where: Option<Expression>,
        order_by: Option<Vec<Expression>>,
    },
    Insert {
        into: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Update {
        table: String,
        columns: Vec<Assignment>,
        r#where: Option<Expression>,
    },
    Delete {
        from: String,
        r#where: Option<Expression>,
    },
    Create(Create),

    Drop(Drop),

    BeginTransaction,

    Rollback,

    Commit,

    Explain(Box<Self>),
}

impl Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Select {
                columns,
                from,
                r#where,
                order_by,
            } => {
                write!(f, "SELECT {} FROM {}", fmt_join(columns, ", "), from)?;

                if let Some(where_expression) = r#where {
                    write!(f, " WHERE {where_expression}")?;
                }

                if let Some(order_by) = order_by {
                    write!(f, " ORDER BY {}", &fmt_join(order_by, ", "))?;
                }
                Ok(())
            }
            Self::Insert {
                into,
                columns,
                values,
            } => {
                write!(f, "INSERT INTO {into}")?;
                if let Some(columns) = columns {
                    write!(f, " ({})", fmt_join(columns, ", "))?;
                }
                let mut fmt_values = vec![];

                for row in values {
                    fmt_values.push(format!("({})", fmt_join(row, ", ")));
                }

                write!(f, " VALUES {}", &fmt_values.join(", "))?;

                Ok(())
            }
            Self::Update {
                table,
                columns,
                r#where,
            } => {
                write!(f, "UPDATE {table} SET {}", fmt_join(columns, ", "))?;

                if let Some(where_expression) = r#where {
                    write!(f, " WHERE {where_expression}")?;
                }

                Ok(())
            }
            Self::Delete { from, r#where } => {
                write!(f, "DELETE FROM {from}")?;

                if let Some(where_expression) = r#where {
                    write!(f, " WHERE {where_expression}")?;
                }

                Ok(())
            }
            Self::Create(create) => {
                write!(f, "{}", create)
            }

            Self::Drop(drop) => write!(f, "{}", drop),

            Self::BeginTransaction => f.write_str("BEGIN TRANSACTION"),

            Self::Rollback => f.write_str("ROLLBACK"),

            Self::Commit => f.write_str("COMMIT"),

            Self::Explain(statement) => write!(f, "EXPLAIN {statement}"),
        }
    }
}

/// Used in select, insert, update, delete.
#[derive(Debug)]
pub enum Expression {
    Identifier(String),

    Value(Value),

    Wildcard,

    BinaryOperation {
        left: Box<Expression>,
        operator: BinaryOperator,
        rigth: Box<Expression>,
    },
    UnaryOperation {
        operator: UnaryOperator,
        expr: Box<Expression>,
    },
    Nested(Box<Expression>),
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(identifier) => write!(f, "{identifier}"),
            Self::Value(value) => write!(f, "{value}"),
            Self::Wildcard => f.write_char('*'),
            Self::BinaryOperation {
                left,
                operator,
                rigth,
            } => write!(f, "{left} {operator} {rigth}"),
            Self::UnaryOperation { operator, expr } => write!(f, "{operator}{expr}"),
            Self::Nested(expr) => write!(f, "({expr})"),
        }
    }
}

#[derive(Debug)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    Add,
    Sub,
    Mul,
    Div,
    And,
    Or,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Eq => "=",
            Self::Neq => "!=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::And => "AND",
            Self::Or => "OR",
        })
    }
}

#[derive(Debug)]
pub enum UnaryOperator {
    Plus,
    Minus,
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plus => "+",
            Self::Minus => "-",
        })
    }
}

/// SQL data types.
#[derive(Debug)]
pub enum DataType {
    Int,
    UnsignedInt,
    BigInt,
    UnsignedBig,
    Boolean,
    VarChar(usize),
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int => f.write_str("INT"),
            Self::UnsignedInt => f.write_str("INT UNSIGNED"),
            Self::BigInt => f.write_str("BIGINT"),
            Self::UnsignedBig => f.write_str("BIGINT UNSIGNED"),
            Self::Boolean => f.write_str("BOOL"),
            Self::VarChar(len) => write!(f, "VARCHAR({len})"),
        }
    }
}

#[derive(Debug)]
pub enum Constrains {
    PrimaryKey,
    Unique,
    NotNull,
}

impl Display for Constrains {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::PrimaryKey => "PRIMARY KEY",
            Self::Unique => "UNIQUE",
            Self::NotNull => "NOT NULL",
        })
    }
}

/// UPDATE helper.
#[derive(Debug)]
pub struct Assignment {
    pub identifier: String,
    pub value: Expression,
}

impl Display for Assignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.identifier, self.value)
    }
}

/// SQL column type.
#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constrains: Vec<Constrains>,
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_owned(),
            data_type,
            constrains: vec![],
        }
    }

    // pub fn primary_key(name: &str, data_type: DataType) -> Self {
    //     Self {
    //         name: name.to_owned(),
    //         data_type,
    //         constrains: vec![Constrains::PrimaryKey],
    //     }
    // }

    // pub fn unique(name: &str, data_type: DataType) -> Self {
    //     Self {
    //         name: name.to_owned(),
    //         data_type,
    //         constrains: vec![Constrains::Unique],
    //     }
    // }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;

        for constrain in &self.constrains {
            write!(f, " {constrain}")?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum Create {
    Database(String),
    Table {
        name: String,
        columns: Vec<Column>,
    },
    Index {
        name: String,
        table: String,
        column: String,
        unique: bool,
    },
}

impl Display for Create {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(name) => write!(f, "CREATE DATABASE {name}"),
            Self::Table { name, columns } => {
                write!(f, "CREATE TABLE {name} ({})", fmt_join(columns, ", "))
            }
            Self::Index {
                name,
                table,
                column,
                unique,
            } => write!(
                f,
                "CREATE{} INDEX {name} ON {table} {column}",
                if *unique { " UNIQUE" } else { "" }
            ),
        }
    }
}

#[derive(Debug)]
pub enum Drop {
    Database(String),
    Table(String),
}

impl Display for Drop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(name) => write!(f, "DROP DATABASE {name}"),
            Self::Table(name) => write!(f, "DROP TABLE {name}"),
        }
    }
}

pub fn fmt_join<'t, T: Display + 't>(
    values: impl IntoIterator<Item = &'t T>,
    separator: &str,
) -> String {
    let mut joined = String::new();

    let mut iter = values.into_iter();

    if let Some(value) = iter.next() {
        write!(joined, "{value}").unwrap();
    }

    for value in iter {
        joined.push_str(separator);
        write!(joined, "{value}").unwrap();
    }

    joined
}
