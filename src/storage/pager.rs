use std::cell::UnsafeCell;

use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::database::MemDatabaseHeader;
use crate::storage::StorageResult;
use crate::storage::buffer_pool::LocalBufferPool;
use crate::storage::cache::ShardedLruCache;
use crate::storage::page::PageType;
use crate::storage::wal::LocalWal;
use crate::utils::io::{BlockIO, IoResult, Job, ReadJobCallbackArgs, WriteJobCallbackArgs};

use crate::storage::PageNumber;

use super::page::Page;

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

    /// Returns mutable reference to `Page` inside, but panics if it is None.
    pub fn get_content(&self) -> &mut Page {
        self.get().content.as_mut().unwrap()
    }

    pub fn is_uptodate(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_UPTODATE != 0
    }

    pub fn set_uptodate(&self) {
        self.get().flags.fetch_or(PAGE_UPTODATE, Ordering::Release);
    }

    pub fn clear_uptodate(&self) {
        self.get()
            .flags
            .fetch_and(!PAGE_UPTODATE, Ordering::Release);
    }

    pub fn is_locked(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_LOCKED != 0
    }

    pub fn set_locked(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::Release);
    }

    pub fn clear_locked(&self) {
        self.get().flags.fetch_and(!PAGE_LOCKED, Ordering::Release);
    }

    pub fn is_error(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_ERROR != 0
    }

    pub fn set_error(&self) {
        self.get().flags.fetch_or(PAGE_ERROR, Ordering::Release);
    }

    pub fn clear_error(&self) {
        self.get().flags.fetch_and(!PAGE_ERROR, Ordering::Release);
    }

    pub fn is_dirty(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_DIRTY != 0
    }

    pub fn set_dirty(&self) {
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::Release);
    }

    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::Acquire) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::Release);
    }

    pub fn clear_loaded(&self) {
        self.get().flags.fetch_and(!PAGE_LOADED, Ordering::Release);
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
    /// Reference to database header owned by `Database` struct.
    pub db_header: Arc<MemDatabaseHeader>,
    /// I/O interface
    io: Arc<BlockIO<File>>,
    /// Each pager gets it's dedicated local pool
    buffer_pool: LocalBufferPool,
    /// Each pager gets local wal to sync with other pagers
    wal: LocalWal,
    /// Reference to global LRU cache
    pub page_cache: Arc<ShardedLruCache>,
}

impl Pager {
    pub fn new(
        db_header: Arc<MemDatabaseHeader>,
        io: Arc<BlockIO<File>>,
        buffer_pool: LocalBufferPool,
        wal: LocalWal,
        page_cache: Arc<ShardedLruCache>,
    ) -> Self {
        Self {
            db_header,
            io,
            buffer_pool,
            wal,
            page_cache,
        }
    }

    pub fn write_header(&mut self) -> StorageResult<()> {
        let raw_header = self.db_header.into_raw_header();

        self.io.write_header(&raw_header.to_bytes())?;

        Ok(())
    }

    pub fn read_page(&mut self, page_number: PageNumber) -> StorageResult<MemPageRef> {
        // check cache...
        if let Some(cached_page) = self.page_cache.get(&page_number) {
            return Ok(cached_page.clone());
        }

        let page_wrapper = Arc::new(MemPage::new(page_number));

        // check wall...
        if let Ok(Some(page)) =
            self.wal
                .read_frame(page_number, page_wrapper.clone(), &self.buffer_pool)
        {
            return Ok(page);
        }

        // read from disk
        let page = self.read_page_from_disk(page_number, page_wrapper)?;

        self.page_cache.insert(page_number, page.clone())?;

        Ok(page)
    }

    fn read_page_from_disk(
        &mut self,
        page_number: PageNumber,
        page: MemPageRef,
    ) -> StorageResult<MemPageRef> {
        begin_read_page(&page)?;

        // runs after IO operation is completed with given result and arguments supplied
        let complete = Box::new(|read_result, args| complete_read_page(read_result, args));

        let buffer = Arc::new(self.buffer_pool.get());

        let job = Job::new_read(page, buffer.clone(), complete);

        let (page, _) = self.io.read(page_number, buffer, job, 0)?.finish();

        Ok(page)
    }

    pub fn write_page(&mut self, page: MemPageRef) -> StorageResult<MemPageRef> {
        begin_write_page(&page)?;

        self.wal
            .append_frame(page, self.db_header.get_database_size())
    }

    pub fn flush(&mut self) -> StorageResult<()> {
        Ok(self.io.flush()?)
    }

    pub fn sync(&self) -> StorageResult<()> {
        Ok(self.io.sync()?)
    }
}

/// Prepares page for read operation
pub fn begin_read_page(page: &MemPageRef) -> StorageResult<()> {
    page.set_locked();
    Ok(())
}

pub fn begin_write_page(page: &MemPageRef) -> StorageResult<()> {
    page.set_locked();
    Ok(())
}

/// Should be universal to both disk and wal reads. Takes `IoResult` with page
/// and buffer and if it is an error it will set proper flags on page. Otherwise
/// it will set correct state and value for page.
/// ### Note that `page` should be locked before calling this function (use `begin_read_page` to setup page for read)
pub fn complete_read_page(read_result: IoResult<usize>, (page, buffer): ReadJobCallbackArgs) {
    if let Ok(bytes_read) = read_result {
        if bytes_read != buffer.size() {
            page.set_error();
            page.clear_loaded();
            page.clear_locked();
            return;
        }
    }
    if let Err(_) = read_result {
        page.set_error();
        page.clear_locked();
        return;
    }

    let content = Page::new(buffer.clone());

    page.get().content = Some(content);
    page.set_loaded();
    page.clear_locked();
}

pub fn complete_write_page(write_result: IoResult<usize>, (page): WriteJobCallbackArgs) {
    if write_result.is_err() {
        page.set_error();
    } else {
        page.clear_dirty();
        page.clear_loaded();
        let _ = page.get().content.take();
    }

    page.clear_locked();
}
