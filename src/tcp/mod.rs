use thiserror::Error;

pub mod client;
pub mod protocol;
pub mod server;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("request is incomplete")]
    Incomplete,

    #[error("request is corrupted")]
    Corrupted,

    #[error("connection was closed before program finished")]
    ConnectionClosed,

    #[error(transparent)]
    SqlError(#[from] crate::sql::Error),

    #[error("internal IO error")]
    IoError(#[from] std::io::Error),
}
