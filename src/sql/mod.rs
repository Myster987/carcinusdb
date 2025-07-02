use thiserror::Error;

pub mod parser;
pub mod statement;
pub mod token;
pub mod tokenizer;

pub type SqlResult<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid query. Error near {0} character.")]
    InvalidQuery(usize),
    #[error("string was opened, but nerver closed.")]
    StringNotClosed,
    #[error("unexpected token: {0}.")]
    UnexpectedToken(char),
}
