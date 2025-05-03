use super::{ResourceId, column::Column};

pub struct Table {
    id: ResourceId,
    name: String,
    columns: Vec<Column>,
}

impl Table {
    pub fn new(id: ResourceId, name: String, columns: Vec<Column>) -> Self {
        Self { id, name, columns }
    }
}
