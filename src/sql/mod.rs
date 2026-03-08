use std::sync::Arc;

use thiserror::Error;

use crate::sql::{
    analyzer::analyze,
    optimizer::optimize,
    parser::{parse, statement::Statement, token::Token},
    prepare::prepare,
    schema::Catalog,
};

pub mod analyzer;
pub mod optimizer;
pub mod parser;
pub mod prepare;
pub mod record;
pub mod schema;
pub mod types;

pub fn pipeline(input: &str, catalog: &Arc<Catalog>) -> Result<Statement> {
    let mut statement = parse(input)?;

    analyze(&statement, &*catalog)?;
    optimize(&mut statement)?;
    prepare(&mut statement, &*catalog)?;

    Ok(statement)
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // parsing
    #[error("invalid query. Error near {0} character.")]
    InvalidQuery(usize),
    #[error("string was opened, but nerver closed.")]
    StringNotClosed,
    #[error("unexpected token: {0}.")]
    UnexpectedToken(char),

    #[error("expected: {expected} but found: {found}.")]
    Expected { expected: Token, found: Token },
    #[error("expected one of: {expected:?} but found: {found}.")]
    ExpectedOneOf { expected: Vec<Token>, found: Token },

    #[error("number out of range")]
    NumberOutOfRange,
    #[error("unexpected eof")]
    UnexpectedEof,

    // types
    #[error("given value doesn't match any serial type")]
    InvalidSerialType,
    #[error("invalid value of type {0}")]
    InvalidValue(&'static str),

    // record
    #[error("corrupted record")]
    CorruptedRecord(#[from] crate::utils::Error),

    // analyzer
    #[error(transparent)]
    AnalyzerError(#[from] analyzer::Error),

    // type error
    #[error(transparent)]
    TypeError(#[from] analyzer::TypeError),

    // schema
    #[error(transparent)]
    SchemaError(#[from] schema::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline() -> anyhow::Result<()> {
        let sql = "SELECT * FROM test WHERE test.id = 1 + 2 - 4;";

        let mut statement = parse(sql)?;
        optimize(&mut statement)?;

        println!("{:?}", statement);

        Ok(())
    }
}
