use thiserror::Error;

pub mod buffer_pool;
pub mod cache;
pub mod page;
pub mod pager;
pub mod allocator;
// pub mod schema;
// pub mod wal;

pub type Oid = u32;

/// Used for pages.
pub type PageNumber = u32;
/// Used for indexing inside page.
pub type SlotNumber = u16;
/// Used for transactions.
pub type TransactionId = u32;
/// Used for WAL frames. Represents offset to frame in bytes and also id.
pub type FrameNumber = u64;

pub const PAGE_NUMBER_SIZE: usize = std::mem::size_of::<PageNumber>();
pub const SLOT_SIZE: usize = std::mem::size_of::<SlotNumber>();

pub type StorageResult<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // heap
    #[error("page {0} not found")]
    PageNotFound(PageNumber),
    #[error("invalid page type")]
    InvalidPageType,

    // cache
    #[error(transparent)]
    Cache(#[from] cache::CacheError),

    // utils
    #[error(transparent)]
    Utils(#[from] crate::utils::Error),

    // io
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
