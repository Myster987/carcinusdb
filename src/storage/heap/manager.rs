use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::storage::schema::ResourceId;

use super::pager::Pager;

pub enum ResourceType {
    Table,
    Index,
}

/// Manages one resource of given type
pub struct ResourceManager {
    id: ResourceId,
    resource_type: ResourceType,
    page_size: usize,
    pagers: HashMap<u16, Arc<Mutex<Pager>>>,
}
