use thiserror::Error;

pub mod protocol;
pub mod server;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    SqlError(#[from] crate::sql::Error),
}
