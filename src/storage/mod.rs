use thiserror::Error;

pub mod btree;
pub mod buffer_pool;
pub mod cache;
pub mod file_system_manager;
pub mod heap;
pub mod page;
pub mod pager;
pub mod schema;

pub type PageNumber = u32;
pub type SlotNumber = u16;

pub const PAGE_NUMBER_SIZE: usize = std::mem::size_of::<PageNumber>();
pub const SLOT_SIZE: usize = std::mem::size_of::<SlotNumber>();

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // heap
    #[error("page {0} not found")]
    PageNotFound(PageNumber),

    // io
    #[error(transparent)]
    Io(#[from] std::io::Error)
}
