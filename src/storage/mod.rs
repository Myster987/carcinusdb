//! # Structure
//! /database_name
//! - metadata (contains genaral information about db)
//! - schema (contains information about tables, indexes, relations, etc..)
//! - data/
//!     - table_id.0, table_id.1, table_id.2  (table blocks)
//!     - table_id_fsm (table free storage map)
//!     - table_id_index_name.0, table_id_index_name.1, table_id_index_name.2 (index blocks)
//!     - table_id_index_name_fsm (index free storage map)

use heap::PageNumber;
use thiserror::Error;

pub mod heap;
pub mod metadata;
pub mod schema;
pub mod file_system_manager;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    // heap
    #[error("page {0} not found")]
    PageNotFound(PageNumber),
}
