use thiserror::Error;

use crate::sql::token::Token;

pub mod parser;
pub mod record;
pub mod statement;
pub mod token;
pub mod tokenizer;
pub mod types;

pub type SqlResult<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
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
}
