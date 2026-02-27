use thiserror::Error;

use crate::sql::parser::{Parser, statement::Statement, token::Token};

pub mod analyzer;
pub mod executor;
pub mod parser;
pub mod record;
pub mod schema;
pub mod types;

pub fn pipeline(input: &str) -> Result<Statement> {
    let statement = Parser::new(input)?.parse_statement()?;

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
}
