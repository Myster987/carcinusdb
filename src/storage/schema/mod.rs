use std::collections::HashMap;

use crate::storage::{schema::{index::Index, table::Table}, Oid};

pub mod column;
pub mod index;
pub mod parse;
pub mod table;

pub struct Schema {
    header: SchemaHeader,
    resources: HashMap<Oid, SchemaResource>
}

pub struct SchemaHeader {
    /// Max length 63 characters
    name: String, 
    /// Number of reources in schema
    num_of_resources: u16,
}

pub struct SchemaResource {
    resource_type: ResourceType,
    size: u16,
}

pub enum ResourceType {
    Table(Table),
    Index(Index),
}

/// When encoding in utf8, it can use from 1 to 4 bytes per character, so when saving to file we need to assume worst case scenario and calculate prefixes for   
pub fn utf8_length_prefix(max_length: usize) -> usize {
    match max_length {
        0..64 => 1,
        64..16384 => 2,
        _ => 4,
    }
}
