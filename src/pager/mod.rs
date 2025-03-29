use std::path::Path;

use std::fs::File;

use io::BlockIO;

use crate::{
    error::DatabaseResult,
    os::{FileSystemBlockSize, Fs, Open, OpenOptions},
    storage::PageNumber,
};

mod io;

pub(crate) const DEFAULT_PAGE_SIZE: usize = 4096;

pub struct Pager {
    file: BlockIO<File>,
    pub block_size: usize,
    pub page_size: usize,
}

impl Pager {
    pub fn new(path: impl AsRef<Path>) -> DatabaseResult<Self> {
        let block_size = Fs::block_size(&path)?;
        let file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .sync_on_write(true)
            .bypass_cache(true)
            .lock(true)
            .open(&path)?;

        Ok(Self {
            file: BlockIO::new(file, block_size, DEFAULT_PAGE_SIZE),
            block_size,
            page_size: DEFAULT_PAGE_SIZE,
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
