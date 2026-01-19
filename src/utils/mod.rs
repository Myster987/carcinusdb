use thiserror::Error;

pub mod buffer;
pub mod bytes;
pub mod cast;
// pub mod concurrency;
pub mod debug_table;
pub mod io;

/// Utilities `Result` type.
pub type Result<T> = std::result::Result<T, Error>;

/// Utilities `Error`.
#[derive(Debug, Error)]
pub enum Error {
    // bytes
    #[error("invalid aligment. make sure to only cast types when each type is aligned")]
    InvalidAligment,
    #[error("size mismatch")]
    SizeMismatch,
    #[error("invalid bytes")]
    InvalidBytes,
    #[error("varint is invalid, either overflowed or incompleted")]
    InvalidVarInt,
    #[error("attempted to access outside buffer. buffer overflow")]
    OutOfSpace,

    // alloc
    #[error("invalid allocation. {0}")]
    InvalidAllocation(String),
    #[error(transparent)]
    Allocator(#[from] std::alloc::LayoutError),

    #[error("unknown error")]
    Unknown,
}
