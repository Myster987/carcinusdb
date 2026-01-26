use thiserror::Error;

use crate::utils::io::BlockNumber;

pub mod btree;
pub mod buffer_pool;
pub mod cache;
pub mod page;
pub mod pager;
pub mod wal;

/// Used for pages.
pub type PageNumber = BlockNumber;
/// Used for indexing inside page.
pub type SlotNumber = u16;
/// Used for WAL frames.
pub type FrameNumber = BlockNumber;

pub const PAGE_NUMBER_SIZE: usize = std::mem::size_of::<PageNumber>();
pub const SLOT_SIZE: usize = std::mem::size_of::<SlotNumber>();

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // heap
    #[error("page {0} not found")]
    PageNotFound(PageNumber),
    #[error("invalid page type")]
    InvalidPageType,
    #[error("page number must be grater than 0")]
    PageNumberOutOfRange,
    #[error("page is corrupted. Exptected state doesn't match")]
    Corruped,
    #[error("cell slot number is out of range")]
    CellIndexOutRange,

    // wal
    #[error("invalid checksum. data is corrupted")]
    InvalidChecksum,
    #[error("page {0} not found in WAL")]
    PageNotFoundInWal(PageNumber),
    #[error("could not begin transaction correctly. please try again")]
    RetryTransaction,

    // b-tree
    #[error("attempted to insert key, that already exists.")]
    DuplicateKey,

    // cache
    #[error(transparent)]
    Cache(#[from] cache::Error),

    // utils
    #[error(transparent)]
    Utils(#[from] crate::utils::Error),

    // io
    #[error(
        "read operation didn't completed successfully. Expected to read: {expected}, but got: {read}"
    )]
    PartialRead { expected: usize, read: usize },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
