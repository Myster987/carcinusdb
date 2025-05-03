use std::{
    fs::File,
    io::{BufReader, Cursor, Read},
    path::{Path, PathBuf},
};

use bytes::{Buf, BytesMut};

use crate::error::DatabaseResult;

#[derive(Debug)]
pub struct Database {
    // pager: Pager,
}

impl Database {
    // pub fn init(path: impl AsRef<Path>) -> DatabaseResult<Self> {
    //     let pager = Pager::new(path)?;
    //     // let mut reader = BufReader::new(file);

    //     Ok(Self {
    //     })
    // }

    // pub fn read_page_zero(&self) -> DatabaseResult<PageZero> {
    //     let mut file = File::open(self.work_dir)?;

    // }
}
