//! Each table is stored in data/ directory.
//! Table and indexes are divided into block of [BLOCK_SIZE]   

pub mod manager;
pub mod page;
pub mod pager;

pub type PageNumber = u32;
pub type SlotNumber = u16;

/// Size of each block that table will be divided into (DEFAULT 1GB).
pub const BLOCK_SIZE: usize = 2_usize.pow(30) * 1;

pub const SLOT_SIZE: usize = std::mem::size_of::<SlotNumber>();
