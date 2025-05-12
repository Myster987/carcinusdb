use std::path::Path;

use std::fs::File;
use std::sync::atomic::AtomicUsize;

use crate::utils::io::BlockIO;

use crate::{
    error::DatabaseResult,
    os::{Open, OpenOptions},
    storage::PageNumber,
};

use super::page::{OverflowPage, Page};

/// Page is locked from I/O to prevent concurrent access
const PAGE_LOCKED: usize = 0b0010;

pub enum MemPageInner {
    BTree(Page),
    Overflow(OverflowPage)
}

pub struct MemPage {
    pub flags: AtomicUsize,
    inner: MemPageInner
}

/// Is responsive for reading and writing to file
#[derive(Debug)]
pub struct Pager {
    file: BlockIO<File>,
    pub page_size: usize,
}

impl Pager {
    pub fn new(path: impl AsRef<Path>, page_size: usize) -> DatabaseResult<Self> {
        let file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .sync_on_write(true)
            .bypass_cache(true)
            .lock(true)
            .open(&path)?;

        Ok(Self {
            file: BlockIO::new(file, page_size),
            page_size,
        })
    }

    pub fn read(&mut self, page_number: PageNumber, buffer: &mut [u8]) -> DatabaseResult<usize> {
        Ok(self.file.read(page_number, buffer)?)
    }

    pub fn write(&mut self, page_number: PageNumber, buffer: &[u8]) -> DatabaseResult<usize> {
        Ok(self.file.write(page_number, buffer)?)
    }

    pub fn flush(&mut self) -> DatabaseResult<()> {
        Ok(self.file.flush()?)
    }

    pub fn sync(&self) -> DatabaseResult<()> {
        Ok(self.file.sync()?)
    }
}
