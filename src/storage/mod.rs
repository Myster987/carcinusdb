//! # Structure
//! /database
//! - metadata (contains genaral information about db)
//! - schema (contains information about tables, indexes, relations, etc..)
//! - table_id/
//!     - free space map (FSM)
//!     - index
//!     - block.0
//!     - block.0
//!
use std::mem;
use thiserror::Error;

pub mod heap;
pub mod metadata;
pub mod page;
pub mod schema;

pub const SLOT_SIZE: usize = mem::size_of::<SlotNumber>();


pub type PageNumber = u32;
pub type SlotNumber = u16;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // heap
    #[error("page {0} not found")]
    PageNotFound(PageNumber),
}
