use std::cell::UnsafeCell;
use std::path::Path;

use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

use crate::storage::buffer_pool::SharedBufferPool;
use crate::storage::page::DEFAULT_PAGE_SIZE;
use crate::utils::io::BlockIO;

use crate::{
    error::DatabaseResult,
    os::{Open, OpenOptions},
    storage::PageNumber,
};

use super::page::{Page};


/*
pub enum MemPageContent {
    BTree(Page),
    Overflow(OverflowPage),
}

impl From<Page> for MemPageContent {
    fn from(value: Page) -> Self {
        Self::BTree(value)
    }
}
*/

// impl From<OverflowPage> for MemPageContent {
//     fn from(value: OverflowPage) -> Self {
//         Self::Overflow(value)
//     }
// }

pub enum MemPageState {
    Loaded,
    Error,
}

pub struct MemPageInner {
    pub id: PageNumber,
    pub content: Option<Arc<RwLock<Page>>>,
}

/// Represents page in memory
pub struct MemPage {
    inner: MemPageInner
}

impl MemPage {
    pub fn new(id: PageNumber) -> Self {
        Self {
            inner: MemPageInner { id, content: None }
        }
    }

    // pub fn lock_shared()
}

pub type MemPageRef = Arc<MemPage>;

/// Is responsive for reading and writing to file
pub struct Pager {
    /// I/O interface 
    io: BlockIO<File>,
    buffer_pool: SharedBufferPool,
}

impl Pager {
    pub fn new(path: impl AsRef<Path>, buffer_pool_ref: SharedBufferPool) -> DatabaseResult<Self> {
        let file = OpenOptions::default()
            .create(true)
            .read(true)
            .write(true)
            .sync_on_write(true)
            .bypass_cache(true)
            .lock(true)
            .open(&path)?;

        Ok(Self {
            io: BlockIO::new(file, *DEFAULT_PAGE_SIZE),
            buffer_pool: buffer_pool_ref
        })
    }

    pub fn read_page(&mut self, page_number: PageNumber) -> DatabaseResult<MemPageRef> {
        let page = Arc::new(MemPage::new(page_number));
        // page.set_locked();

        // check cache...

        // check wall...

        // read from disk

        let mut buf = self.buffer_pool.get();

        // let page = Sha

        // self.io.read(page_number, buffer.as_slice_mut())?;


        

        // let inner = MemPageContent::

        todo!()
        
    }

    pub fn write(&mut self, page_number: PageNumber, buffer: &[u8]) -> DatabaseResult<usize> {
        Ok(self.io.write(page_number, buffer)?)
    }

    pub fn flush(&mut self) -> DatabaseResult<()> {
        Ok(self.io.flush()?)
    }

    pub fn sync(&self) -> DatabaseResult<()> {
        Ok(self.io.sync()?)
    }
}
