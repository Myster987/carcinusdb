use std::cell::UnsafeCell;
use std::path::Path;

use std::fs::File;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::utils::io::BlockIO;

use crate::{
    error::DatabaseResult,
    os::{Open, OpenOptions},
    storage::PageNumber,
};

use super::page::{OverflowPage, Page};

/// Page is up to date
const PAGE_UPTODATE: usize = 0b00001;
/// Page is locked from I/O to prevent concurrent access
const PAGE_LOCKED: usize = 0b00010;
/// Page is in error state
const PAGE_ERROR: usize = 0b00100;
/// Page is dirty. Flush needed
const PAGE_DIRTY: usize = 0b01000;
/// Page is loaded into memory
const PAGE_LOADED: usize = 0b10000;

pub enum MemPageContent {
    BTree(Page),
    Overflow(OverflowPage)
}

impl From<Page> for MemPageContent {
    fn from(value: Page) -> Self {
        Self::BTree(value)
    }
}

impl From<OverflowPage> for MemPageContent {
    fn from(value: OverflowPage) -> Self {
        Self::Overflow(value)
    }
}


pub struct MemPageInner {
    pub flags: AtomicUsize,
    pub content: Option<MemPageContent>
}

/// Represents page loaded into memory
pub struct MemPage {
    inner: UnsafeCell<MemPageInner>
}

impl MemPage {
    pub fn new() -> Self {
        Self { inner: UnsafeCell::new(MemPageInner { flags: AtomicUsize::new(0), content: None }) }
    }

    pub fn get(&self) -> &mut MemPageInner {
        unsafe { &mut *self.inner.get()}
    }

    pub fn get_content(&self) -> &mut MemPageContent {
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

    pub fn lock(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn unclock(&self) {
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
}

pub type MemPageRef = Arc<MemPage>;

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
