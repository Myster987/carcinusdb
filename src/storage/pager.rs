use std::cell::{RefCell, UnsafeCell};

use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use crate::storage::buffer_pool::LocalBufferPool;
use crate::storage::cache::LruPageCache;
use crate::storage::page::PageType;
use crate::storage::wal::LocalWal;
use crate::storage::{Error, StorageResult};
use crate::utils::buffer::Buffer;
use crate::utils::io::BlockIO;

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
        self.get().flags.load(Ordering::SeqCst) & PAGE_UPTODATE != 0
    }

    pub fn set_uptodate(&self) {
        self.get().flags.fetch_or(PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn clear_uptodate(&self) {
        self.get().flags.fetch_and(!PAGE_UPTODATE, Ordering::SeqCst);
    }

    pub fn is_locked(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOCKED != 0
    }

    pub fn set_locked(&self) {
        self.get().flags.fetch_or(PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn clear_locked(&self) {
        self.get().flags.fetch_and(!PAGE_LOCKED, Ordering::SeqCst);
    }

    pub fn is_error(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_ERROR != 0
    }

    pub fn set_error(&self) {
        self.get().flags.fetch_or(PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn clear_error(&self) {
        self.get().flags.fetch_and(!PAGE_ERROR, Ordering::SeqCst);
    }

    pub fn is_dirty(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_DIRTY != 0
    }

    pub fn set_dirty(&self) {
        self.get().flags.fetch_or(PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn clear_dirty(&self) {
        self.get().flags.fetch_and(!PAGE_DIRTY, Ordering::SeqCst);
    }

    pub fn is_loaded(&self) -> bool {
        self.get().flags.load(Ordering::SeqCst) & PAGE_LOADED != 0
    }

    pub fn set_loaded(&self) {
        self.get().flags.fetch_or(PAGE_LOADED, Ordering::SeqCst);
    }

    pub fn clear_loaded(&self) {
        self.get().flags.fetch_and(!PAGE_LOADED, Ordering::SeqCst);
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
    /// Each pager gets it's dedicated local pool
    buffer_pool: LocalBufferPool,
    /// Each pager gets local wal to sync with other pagers
    wal: LocalWal,
    /// Reference to global LRU cache
    page_cache: Arc<Mutex<LruPageCache>>,
    page_size: u16,
    reserved_space: u8,
}

impl Pager {
    pub fn begin_open(
        io: BlockIO<File>,
        buffer_pool: LocalBufferPool,
        wal: LocalWal,
        page_cache: Arc<Mutex<LruPageCache>>,
        page_size: u16,
        reserved_space: u8,
    ) -> StorageResult<Self> {
        // let file = OpenOptions::default()
        //     .create(true)
        //     .read(true)
        //     .write(true)
        //     .sync_on_write(true)
        //     .bypass_cache(true)
        //     .lock(true)
        //     .open(&path)?;

        Ok(Self {
            io,
            buffer_pool,
            wal,
            page_cache,
            page_size,
            reserved_space,
        })
    }

    pub fn read_page(&mut self, page_number: PageNumber) -> StorageResult<MemPageRef> {
        // check cache...
        if let Some(cached_page) = self.page_cache.lock().get(&page_number) {
            return Ok(cached_page.clone());
        }

        let page_wrapper = Arc::new(MemPage::new(page_number));

        // check wall...
        if let Ok(Some(page)) =
            self.wal
                .read_frame(page_number, page_wrapper.clone(), self.buffer_pool.clone())
        {
            return Ok(page);
        }

        // read from disk
        let page = self.read_page_from_disk(page_number, page_wrapper)?;

        self.page_cache.lock().insert(page_number, page.clone())?;

        Ok(page)
    }

    fn read_page_from_disk(
        &mut self,
        page_number: PageNumber,
        page: MemPageRef,
    ) -> StorageResult<MemPageRef> {
        let mut buf = self.buffer_pool.get();
        page.set_locked();

        let read_result = self.io.read(page_number, &mut buf[..]);

        complete_read_page(read_result, page, buf)
    }

    pub fn write(&mut self, page_number: PageNumber, buffer: &[u8]) -> StorageResult<usize> {
        Ok(self.io.write(page_number, buffer)?)
    }

    pub fn flush(&mut self) -> StorageResult<()> {
        Ok(self.io.flush()?)
    }

    pub fn sync(&self) -> StorageResult<()> {
        Ok(self.io.sync()?)
    }
}

/// Prepares page for read operation
pub fn begin_read_page(page: MemPageRef) -> StorageResult<MemPageRef> {
    page.set_locked();
    Ok(page)
}

pub fn begin_write_page(page: MemPageRef) -> StorageResult<MemPageRef> {
    page.set_locked();
    Ok(page)
}

/// Should be universal to both disk and wal reads. Takes io::Result and if it
/// is an error it will set proper flags on page. Otherwise it will set
/// correct state and value for page.
/// ### Note that `page` should be locked before calling this function (use `begin_read_page` to setup page for read)
pub fn complete_read_page(
    read_result: std::io::Result<usize>,
    page: MemPageRef,
    buffer: Buffer,
) -> StorageResult<MemPageRef> {
    if let Ok(bytes_read) = read_result {
        if bytes_read != buffer.size() {
            page.set_error();
            page.clear_loaded();
            return Err(Error::PartialRead {
                expected: buffer.size(),
                read: bytes_read,
            });
        }
    }
    if let Err(error) = read_result {
        page.set_error();
        page.clear_locked();
        return Err(error.into());
    }

    let content = Page::new(Arc::new(RefCell::new(buffer)));

    page.get().content = Some(content);
    page.set_loaded();
    page.clear_locked();

    Ok(page)
}

pub fn complete_write_page(
    write_result: std::io::Result<usize>,
    page: MemPageRef,
) -> StorageResult<MemPageRef> {
    if let Err(error) = write_result {
        page.set_error();
        page.clear_locked();
        return Err(error.into());
    }

    let _ = page.get().content.take();

    page.clear_dirty();
    page.clear_loaded();
    page.clear_loaded();

    Ok(page)
}
