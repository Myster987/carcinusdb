use thiserror::Error;

pub mod buffer;
pub mod bytes;
pub mod cast;

/// Utilities `Result` type.
pub type Result<T> = std::result::Result<T, Error>;

/// Utilities `Error`.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid aligment")]
    InvalidAligment,
    #[error("size mismatch")]
    SizeMismatch,
    #[error("invalid allocation. {0}")]
    InvalidAllocation(String),

    #[error(transparent)]
    Allocator(#[from] std::alloc::LayoutError),
    #[error("unknown error")]
    Unknown,
}
