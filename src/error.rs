use thiserror::Error;

pub type DatabaseResult<T> = std::result::Result<T, DatabaseError>;

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("invalid bytes")]
    InvalidBytes,

    // io
    #[error("invalid hostname: {hostname}\nmessage: {msg}")]
    InvalidHostname { msg: String, hostname: String },
    #[error("provided path is not file: {0}")]
    InvalidFilePath(String),
    #[error("invalid port number: {0}")]
    InvalidPort(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    // env
    #[error(transparent)]
    Env(#[from] std::env::VarError),

    // internal
    #[error(transparent)]
    UtilsError(#[from] crate::utils::Error),

    #[error(transparent)]
    StorageError(#[from] crate::storage::Error),
    

    // other
    #[error("unknown database error")]
    Unknown,
}
