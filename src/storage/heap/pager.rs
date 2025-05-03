use std::path::Path;

use std::fs::File;

use crate::utils::io::BlockIO;

use crate::{
    error::DatabaseResult,
    os::{Open, OpenOptions},
    storage::PageNumber,
};

/// Controls 1 block (file)
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
