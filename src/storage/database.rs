use std::{fs::File, sync::Arc};

use crate::utils::io::BlockIO;

pub struct DatabaseFile {
    file: Arc<BlockIO<File>>,
}

unsafe impl Send for DatabaseFile {}
unsafe impl Sync for DatabaseFile {}

impl DatabaseFile {
    
}