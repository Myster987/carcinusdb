use thiserror::Error;

pub mod buffer_pool;
pub mod cache;
pub mod page;
pub mod pager;
// pub mod schema;
// pub mod wal;

pub type Oid = u32;

pub type PageNumber = u32;
pub type TransactionId = u32;
pub type SlotNumber = u16;

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

    // utils
    #[error(transparent)]
    Utils(#[from] crate::utils::Error),

    // io
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
