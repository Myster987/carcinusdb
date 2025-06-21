use std::cell::UnsafeCell;
use std::path::Path;

use std::fs::File;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::storage::buffer_pool::BufferPool;
use crate::storage::page::{DEFAULT_PAGE_SIZE, PageType};
use crate::utils::io::BlockIO;

use crate::{
    error::DatabaseResult,
    os::{Open, OpenOptions},
    storage::PageNumber,
};

use super::page::Page;

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
    pub flags: AtomicUsize,
    pub content: Option<Page>,
}

/// Represents page in memory
pub struct MemPage {
    inner: UnsafeCell<MemPageInner>,
}

/// Concurrency will be handled by Pager
pub type MemPageRef = Arc<MemPage>;

/// Page is up to date.
const PAGE_UPTODATE: usize = 0b00001;
/// Page is locked from I/O access.
const PAGE_LOCKED: usize = 0b00010;
/// Page is in I/O error state.
const PAGE_ERROR: usize = 0b00100;
/// Page needs to be flushed to disk.
const PAGE_DIRTY: usize = 0b01000;
/// Page is loaded into memory.
const PAGE_LOADED: usize = 0b10000;

impl MemPage {
    pub fn new(id: PageNumber) -> Self {
        Self {
            inner: UnsafeCell::new(MemPageInner {
                id,
                flags: AtomicUsize::new(0),
                content: None,
            }),
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get(&self) -> &mut MemPageInner {
        unsafe { &mut *self.inner.get() }
    }

    pub fn get_content(&self) -> &mut Page {
        self.get().content.as_mut().unwrap()
    }

    pub fn is_uptodate(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_UPTODATE != 0
    }

    pub fn set_uptodate(&self) {
        self.get().flags.fetch_or(PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn clear_uptodate(&self) {
        self.get().flags.fetch_or(!PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn is_locked(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOCKED != 0
    }

    pub fn set_locked(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn clear_locked(&self) {
        self.get().flags.fetch_or(!PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn is_error(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_ERROR != 0
    }

    pub fn set_error(&self) {
        self.get().flags.fetch_or(PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn clear_error(&self) {
        self.get().flags.fetch_or(!PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn is_dirty(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_DIRTY != 0
    }

    pub fn set_dirty(&self) {
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn clear_dirty(&self) {
        self.get().flags.fetch_or(!PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::SeqCst);
    }

    pub fn clear_loaded(&self) {
        self.get().flags.fetch_or(!PAGE_LOADED, Ordering::SeqCst);
    }

    pub fn is_index(&self) -> bool {
        match self.get_content().page_type() {
            PageType::IndexInternal | PageType::IndexLeaf => true,
            PageType::TableInternal | PageType::TableLeaf => false,
        }
    }
}

/// Is responsive for reading and writing to file
pub struct Pager {
    /// I/O interface
    io: BlockIO<File>,
    buffer_pool: Rc<BufferPool>,
}

impl Pager {
    pub fn begin_open(path: impl AsRef<Path>, buffer_pool_ref: Rc<BufferPool>) -> DatabaseResult<Self> {
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
            buffer_pool: buffer_pool_ref,
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

