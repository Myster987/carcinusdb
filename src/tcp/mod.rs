use thiserror::Error;

pub mod protocol;
pub mod server;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("request is incomplete")]
    Incomplete,

    #[error("request is corrupted")]
    Corrupted,

    #[error(transparent)]
    SqlError(#[from] crate::sql::Error),

    #[error("internal IO error")]
    IoError(#[from] std::io::Error),
}
