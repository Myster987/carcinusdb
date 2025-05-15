use std::{fs::File, path::Path};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::{
    os::{Open, OpenOptions},
    storage::PageNumber,
    utils::io::BlockIO,
};

use super::{
    super::{Error, Result},
    {BLOCK_SIZE, BlockId, BlockNumber},
};

pub enum BlockType {
    Table,
    Index,
}

pub struct BlockManager {
    id: BlockId,
    page_size: usize,
    block_type: BlockType,
    blocks: DashMap<BlockNumber, Mutex<Block>>,
}

impl BlockManager {
    pub fn new(id: BlockId, page_size: usize, block_type: BlockType) -> Self {
        Self {
            id,
            page_size,
            block_type,
            blocks: DashMap::new(),
        }
    }

    /// Returns `BlockNumber` relative to page number
    ///
    /// # Example
    ///
    /// lets say that block_0 can hold [0-10_000) pages, block_1 [10_000, 20_000), etc
    /// and this function for page: 12_345 will return block_1 because we know for sure that
    /// this page must be there   
    fn calculate_page_block(&self, page_number: PageNumber) -> BlockNumber {
        page_number / (BLOCK_SIZE / self.page_size) as u32
    }

    /// Returns page offset relative to block it is stored in
    fn calculate_page_offset(&self, page_number: PageNumber) -> PageNumber {
        (page_number) % (BLOCK_SIZE / self.page_size) as u32
    }

    /// Reads given page from disk into buffer
    /// # Note 
    /// One block holds [0; BLOCK_SIZE / PAGE_SIZE) pages
    pub fn read_page(&self, page_number: PageNumber, buffer: &mut [u8]) -> Result<()> {
        let block_num = self.calculate_page_block(page_number);
        let block_ref = self
            .blocks
            .get(&block_num)
            .ok_or(Error::PageNotFound(page_number))?;
        let mut block = block_ref.lock();

        let relative_page_number = self.calculate_page_offset(page_number);

        block.read(relative_page_number, buffer)?;

        Ok(())
    }

    /// Writes page represented as a slice of bytes to disk
    /// # Note
    /// This function will block current thread if block that we want to write to is locked.
    pub fn write_page(&self, page_number: PageNumber, buffer: &[u8]) -> Result<()> {
        let block_num = self.calculate_page_block(page_number);
        let block_ref = self
            .blocks
            .get(&block_num)
            .ok_or(Error::PageNotFound(page_number))?;
        let mut block = block_ref.lock();

        let relative_page_number = self.calculate_page_offset(page_number);

        block.write(relative_page_number, buffer)?;

        Ok(())
    }
}

pub struct Block {
    pub id: usize,
    file: BlockIO<File>,
}

impl Block {
    pub fn new(id: usize, path: impl AsRef<Path>, page_size: usize) -> Result<Self> {
        let file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .sync_on_write(true)
            .bypass_cache(true)
            .lock(true)
            .open(&path)?;

        Ok(Self {
            id,
            file: BlockIO::new(file, page_size),
        })
    }

    pub fn read(&mut self, page_number: PageNumber, buffer: &mut [u8]) -> Result<usize> {
        Ok(self.file.read(page_number, buffer)?)
    }

    pub fn write(&mut self, page_number: PageNumber, buffer: &[u8]) -> Result<usize> {
        Ok(self.file.write(page_number, buffer)?)
    }

    pub fn flush(&mut self) -> Result<()> {
        Ok(self.file.flush()?)
    }

    pub fn sync(&self) -> Result<()> {
        Ok(self.file.sync()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_page() -> anyhow::Result<()> {
        let block_manager = BlockManager::new(1, 4096, BlockType::Table);

        assert!(block_manager.calculate_page_block(1) == 0);

        assert!(block_manager.calculate_page_block(262_144) == 1);

        assert!(block_manager.calculate_page_block(3 * 262_144 + 1) == 3);

        let page_number = 2 * 262_144;

        assert!(
            block_manager.calculate_page_block(page_number) == 2
                && block_manager.calculate_page_offset(page_number) == 0
        );

        Ok(())
    }
}
