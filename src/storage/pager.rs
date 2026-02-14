use std::cell::UnsafeCell;
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashSet;

use crate::database::MemDatabaseHeader;
use crate::storage::btree::BTreeType;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::cache::ShardedClockCache;
use crate::storage::page::{DATABASE_HEADER_PAGE_NUMBER, DATABASE_HEADER_SIZE, PageType};
use crate::storage::wal::transaction::{ReadTx, WriteTx};
use crate::storage::wal::{DEFAULT_CHECKPOINT_SIZE, WriteAheadLog};
use crate::utils::io::BlockIO;

use crate::storage::{self, PageNumber};

use super::page::Page;

pub type MemPageRef = Arc<MemPage>;

pub struct MemPage {
    /// Tracks state of given page in memory.
    /// Bits layout (starting at 0):
    /// - 0 <- error state
    /// - 1 <- dirty state
    /// - 2 <- loaded to memory
    /// - 3 <- locked exclusive
    /// - 4 <- seeks exclusive lock (no new shared locks)
    /// - 5..rest <- count of shared locks (also works as pin counter)
    state: AtomicUsize,
    inner: UnsafeCell<MemPageInner>,
}

impl MemPage {
    pub fn new(id: PageNumber) -> Self {
        Self {
            state: AtomicUsize::new(0),
            inner: UnsafeCell::new(MemPageInner { id, content: None }),
        }
    }

    pub fn from_page(id: PageNumber, page: Page) -> Self {
        Self {
            state: AtomicUsize::new(0),
            inner: UnsafeCell::new(MemPageInner {
                id,
                content: Some(page),
            }),
        }
    }

    /// Retruns mutable reference to `inner`.
    ///
    /// # Safety
    ///
    /// This is very unsafe, so don't use this function directly.
    /// Use safe wrappers insted.
    pub(super) fn inner(&self) -> &mut MemPageInner {
        unsafe { self.inner.get().as_mut().unwrap() }
    }

    /// Once `id` is set, it will never be changed so this is **safe**.
    pub fn id(&self) -> PageNumber {
        self.inner().id
    }
}

// Flags
impl MemPage {
    /// Page is in I/O error state.
    const PAGE_ERROR: usize = 1 << 0;
    /// Page needs to be flushed to disk.
    const PAGE_DIRTY: usize = 1 << 1;
    /// Page is loaded into memory.
    const PAGE_LOADED: usize = 1 << 2;

    pub fn is_error(&self) -> bool {
        self.state.load(Ordering::Acquire) & Self::PAGE_ERROR != 0
    }

    pub fn is_dirty(&self) -> bool {
        self.state.load(Ordering::Acquire) & Self::PAGE_DIRTY != 0
    }

    pub fn is_loaded(&self) -> bool {
        self.state.load(Ordering::Acquire) & Self::PAGE_LOADED != 0
    }

    pub fn set_error(&self) {
        self.state.fetch_or(Self::PAGE_ERROR, Ordering::Release);
    }

    pub fn set_dirty(&self) {
        self.state.fetch_or(Self::PAGE_DIRTY, Ordering::Release);
    }

    pub fn set_loaded(&self) {
        self.state.fetch_or(Self::PAGE_LOADED, Ordering::Release);
    }

    pub fn clear_error(&self) {
        self.state.fetch_and(!Self::PAGE_ERROR, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        self.state.fetch_and(!Self::PAGE_DIRTY, Ordering::Release);
    }

    pub fn clear_loaded(&self) {
        self.state.fetch_and(!Self::PAGE_LOADED, Ordering::Release);
    }
}

// Locks
impl MemPage {
    // Locking state
    const LOCKED_EXCLUSIVE: usize = 1 << 3;
    const SEEKS_LOCK: usize = 1 << 4;

    const SHARED_LOCKS_SHIFT: usize = 5;
    const SHARED_LOCK_ONE: usize = 1 << Self::SHARED_LOCKS_SHIFT;

    pub fn is_locked(&self) -> bool {
        let lock = self.state.load(Ordering::Acquire);

        lock >> 3 != 0
    }

    pub fn pin_count(&self) -> usize {
        let lock = self.state.load(Ordering::Acquire);

        (lock >> Self::SHARED_LOCKS_SHIFT) + lock & Self::LOCKED_EXCLUSIVE
    }

    pub fn lock_shared(self: &Arc<Self>) -> SharedPageGuard {
        loop {
            let current = self.state.load(Ordering::Acquire);

            if current & (Self::LOCKED_EXCLUSIVE | Self::SEEKS_LOCK) != 0 {
                std::hint::spin_loop();
                continue;
            }

            if self
                .state
                .compare_exchange(
                    current,
                    current + Self::SHARED_LOCK_ONE,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return SharedPageGuard { page: self.clone() };
            }
        }
    }

    fn lock_seek(self: &Arc<Self>) {
        loop {
            let current = self.state.load(Ordering::Acquire);

            if current & (Self::LOCKED_EXCLUSIVE | Self::SEEKS_LOCK) != 0 {
                std::hint::spin_loop();
                continue;
            }

            if self
                .state
                .compare_exchange(
                    current,
                    current | Self::SEEKS_LOCK,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn lock_exclusive(self: &Arc<Self>) -> ExclusivePageGuard {
        self.lock_seek();

        loop {
            let current = self.state.load(Ordering::Acquire);

            let exclusive = current & Self::LOCKED_EXCLUSIVE;
            let readers = current >> Self::SHARED_LOCKS_SHIFT;

            if readers == 0 && exclusive == 0 {
                // attempt to clear seek and acquire exclusive
                if self
                    .state
                    .compare_exchange(
                        current,
                        (current & !Self::SEEKS_LOCK) | Self::LOCKED_EXCLUSIVE,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return ExclusivePageGuard { page: self.clone() };
                }
            } else {
                std::hint::spin_loop();
            }
        }
    }

    pub fn unlock_shared(&self) {
        self.state
            .fetch_sub(Self::SHARED_LOCK_ONE, Ordering::Release);
    }

    pub fn unlock_exclusive(&self) {
        self.state
            .fetch_and(!Self::LOCKED_EXCLUSIVE, Ordering::Release);
    }
}

pub struct MemPageInner {
    id: PageNumber,
    pub(super) content: Option<Page>,
}

pub struct SharedPageGuard {
    page: Arc<MemPage>,
}

impl SharedPageGuard {
    pub fn get(&self) -> &Page {
        self.page
            .inner()
            .content
            .as_ref()
            .expect("Page shouldn't be None")
    }

    pub fn id(&self) -> PageNumber {
        self.page.id()
    }

    pub fn is_error(&self) -> bool {
        self.page.is_error()
    }

    pub fn is_dirty(&self) -> bool {
        self.page.is_dirty()
    }

    pub fn is_loaded(&self) -> bool {
        self.page.is_loaded()
    }

    pub fn set_error(&self) {
        self.page.set_error()
    }

    pub fn set_dirty(&self) {
        self.page.set_dirty()
    }

    pub fn set_loaded(&self) {
        self.page.set_loaded()
    }

    pub fn clear_error(&self) {
        self.page.clear_error()
    }

    pub fn clear_dirty(&self) {
        self.page.clear_dirty()
    }

    pub fn clear_loaded(&self) {
        self.page.clear_loaded();
    }
}

impl Deref for SharedPageGuard {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl Drop for SharedPageGuard {
    fn drop(&mut self) {
        self.page.unlock_shared();
    }
}

pub struct ExclusivePageGuard {
    page: Arc<MemPage>,
}

impl ExclusivePageGuard {
    pub fn get(&self) -> &Page {
        self.page
            .inner()
            .content
            .as_ref()
            .expect("Page shouldn't be None")
    }

    pub fn get_mut(&mut self) -> &mut Page {
        self.page
            .inner()
            .content
            .as_mut()
            .expect("Page shouldn't be None")
    }

    pub fn replace(&mut self, page: Page) {
        self.page.inner().content.replace(page);
    }

    pub fn take(&mut self) -> Option<Page> {
        self.page.inner().content.take()
    }

    pub fn id(&self) -> PageNumber {
        self.page.id()
    }

    pub fn is_error(&self) -> bool {
        self.page.is_error()
    }

    pub fn is_dirty(&self) -> bool {
        self.page.is_dirty()
    }

    pub fn is_loaded(&self) -> bool {
        self.page.is_loaded()
    }

    pub fn set_error(&self) {
        self.page.set_error()
    }

    pub fn set_dirty(&self) {
        self.page.set_dirty()
    }

    pub fn set_loaded(&self) {
        self.page.set_loaded()
    }

    pub fn clear_error(&self) {
        self.page.clear_error()
    }

    pub fn clear_dirty(&self) {
        self.page.clear_dirty()
    }

    pub fn clear_loaded(&self) {
        self.page.clear_loaded();
    }
}

impl Deref for ExclusivePageGuard {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl DerefMut for ExclusivePageGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
}

impl Drop for ExclusivePageGuard {
    fn drop(&mut self) {
        self.page.unlock_exclusive();
    }
}

/// Is responsive for reading and writing to file.
pub struct Pager {
    /// Reference to database header owned by `Database` struct.
    pub db_header: Arc<MemDatabaseHeader>,
    /// I/O interface
    io: Arc<BlockIO<File>>,
    /// Each pager gets it's dedicated local pool
    buffer_pool: Arc<BufferPool>,
    /// Each pager gets local wal to sync with other pagers
    pub wal: Arc<WriteAheadLog>,
    /// Reference to global LRU cache
    pub page_cache: Arc<ShardedClockCache>,
    pub dirty_pages: Arc<DashSet<PageNumber>>,
    cache_flush_threshold: usize,
}

impl Pager {
    /// After this percentage of dirty pages in cache, it will flush them
    /// into WAL.
    const DIRTY_PAGES_FLUSH_PERCENTAGE: usize = 50;
    /// Max threshold at which it can trigger flush. For now it's set to
    /// DEFAULT_CHECKPOINT_SIZE.
    const DIRTY_PAGES_FLUSH_THRESHOLD: usize = DEFAULT_CHECKPOINT_SIZE as usize;

    pub fn new(
        db_header: Arc<MemDatabaseHeader>,
        io: Arc<BlockIO<File>>,
        buffer_pool: Arc<BufferPool>,
        wal: Arc<WriteAheadLog>,
        page_cache: Arc<ShardedClockCache>,
        is_initialized: bool,
    ) -> storage::Result<Self> {
        let relative_flush_threshold =
            db_header.default_page_cache_size as usize * Self::DIRTY_PAGES_FLUSH_PERCENTAGE / 100;

        let cache_flush_threshold =
            std::cmp::min(Self::DIRTY_PAGES_FLUSH_THRESHOLD, relative_flush_threshold);

        let pager = Self {
            db_header,
            io,
            buffer_pool,
            wal,
            page_cache,
            dirty_pages: Arc::new(DashSet::new()),
            cache_flush_threshold,
        };

        if !is_initialized {
            pager.init()?;
        }

        Ok(pager)
    }

    pub fn init(&self) -> storage::Result<()> {
        log::info!("Initializing database.");
        let mut tx = self.wal.begin_write_tx()?;

        let _ = self.btree_create(&mut tx, BTreeType::Table)?;

        self.write_header(&mut tx)?;

        self.flush_dirty(&mut tx, true)?;

        self.wal.commit(tx)?;

        self.wal.force_checkpoint()?;

        Ok(())
    }

    pub fn write_header<Tx: WriteTx>(&self, tx: &mut Tx) -> storage::Result<()> {
        let raw_header = self.db_header.into_raw_header();

        let header_page = self.read_page(tx, DATABASE_HEADER_PAGE_NUMBER)?;
        header_page.lock_exclusive().write_db_header(raw_header);

        self.mark_dirty(&header_page);

        Ok(())
    }

    /// Reads given `page_number` in following order:
    ///
    /// - Cache (page in memory)
    /// - WAL (disk read)
    /// - DB (disk read)
    ///
    /// In case of cache miss it will insert page into cache for future use.
    pub fn read_page<Tx: ReadTx>(
        &self,
        tx: &Tx,
        page_number: PageNumber,
    ) -> storage::Result<MemPageRef> {
        // check cache...
        if let Some(cached_page) = self.page_cache.get(&page_number) {
            return Ok(cached_page);
        }

        let mem_page = self.read_page_no_cache(tx, page_number)?;

        self.page_cache.insert(page_number, mem_page.clone())?;

        Ok(mem_page)
    }

    fn read_page_no_cache<Tx: ReadTx>(
        &self,
        tx: &Tx,
        page_number: PageNumber,
    ) -> storage::Result<MemPageRef> {
        let page_wrapper = Arc::new(MemPage::new(page_number));

        {
            // lock for IO operations
            let mut guard = page_wrapper.lock_exclusive();

            // check wall...
            if let Ok(Some(page)) = self.wal.read_frame(tx, page_number, &self.buffer_pool) {
                guard.replace(page);
            } else {
                // read from db file
                let page = self.read_page_from_disk(page_number)?;
                guard.replace(page);
            }
            guard.set_loaded();
        } // unlock

        Ok(page_wrapper)
    }

    /// Reads page from disk at given `page_number`.
    ///
    /// # Safety
    ///
    /// Page that needs to be read should already be write-locked.
    fn read_page_from_disk(&self, page_number: PageNumber) -> storage::Result<Page> {
        // begin_read_page(&page);

        let mut buffer = self.buffer_pool.get();

        let read_result = self.io.read(page_number, &mut buffer, 0);

        // complete_read_page(&read_result, &page, buffer);

        match read_result {
            Ok(bytes_read) => {
                if bytes_read == buffer.size() {
                    let offset = if page_number == DATABASE_HEADER_PAGE_NUMBER {
                        DATABASE_HEADER_SIZE
                    } else {
                        0
                    };
                    Ok(Page::new(offset, buffer))
                } else {
                    Err(storage::Error::PageNotFound(page_number))
                }
            }
            Err(_) => Err(storage::Error::PageNotFound(page_number)),
        }
    }

    pub fn write_page<Tx: WriteTx>(&self, tx: &mut Tx, page: MemPageRef) -> storage::Result<()> {
        // begin_write_page(&page)?;

        let guard = page.lock_exclusive();

        self.wal
            .append_frame(tx, guard, self.db_header.get_database_size())
    }

    /// Allocates new B-tree in database.
    pub fn btree_create<Tx: WriteTx>(
        &self,
        tx: &mut Tx,
        btree_type: BTreeType,
    ) -> storage::Result<PageNumber> {
        log::debug!("Creating new {:?} B-tree.", btree_type);
        let page_type = match btree_type {
            BTreeType::Index => PageType::IndexLeaf,
            BTreeType::Table => PageType::TableLeaf,
        };

        self.alloc_page(tx, page_type)
    }

    /// Allocates new page in db file. By default tries to use page from
    /// freelist and if freelist is empty, then it creates new page at the
    /// end of db file. Page is initialize as B-tree page, so if you want
    /// plain page, use `Self::alloc_empty_page`.
    pub fn alloc_page<Tx: WriteTx>(
        &self,
        tx: &mut Tx,
        page_type: PageType,
    ) -> storage::Result<PageNumber> {
        let first_freelist_page = self.db_header.get_first_freelist_page();

        let free_page = if first_freelist_page == 0 {
            let page_number = self.db_header.add_database_size(1);
            let offset = if page_number == DATABASE_HEADER_PAGE_NUMBER {
                DATABASE_HEADER_SIZE
            } else {
                0
            };

            let new_page = Page::new(offset, self.buffer_pool.get());
            new_page.initialize(page_type);

            let new_page = Arc::new(MemPage::from_page(page_number, new_page));
            self.mark_dirty(&new_page);
            new_page.set_loaded();

            // we have to manually insert new page to cache.
            self.page_cache.insert(page_number, new_page)?;

            page_number
        } else {
            // loads to cache for use.
            let free_page = self.read_page(tx, first_freelist_page)?;

            self.db_header
                .set_first_freelist_page(free_page.lock_shared().freelist_next());
            self.db_header.sub_freelist_pages(1);

            {
                let guard = free_page.lock_exclusive();
                guard.initialize(page_type);
                guard.set_loaded();
            }

            self.mark_dirty(&free_page);

            free_page.id()
        };

        self.write_header(tx)?;

        Ok(free_page)
    }

    /// Allocates empty page in database. Can be used to build linked list of
    /// overflow cells.
    pub fn alloc_empty_page<Tx: WriteTx>(&self, tx: &mut Tx) -> storage::Result<PageNumber> {
        let first_freelist_page = self.db_header.get_first_freelist_page();

        let free_page = if first_freelist_page == 0 {
            let page_number = self.db_header.add_database_size(1);
            let offset = if page_number == DATABASE_HEADER_PAGE_NUMBER {
                DATABASE_HEADER_SIZE
            } else {
                0
            };

            let new_page = Page::new(offset, self.buffer_pool.get());

            let new_page = Arc::new(MemPage::from_page(page_number, new_page));

            self.mark_dirty(&new_page);
            new_page.set_loaded();

            // we have to manually insert new page to cache.
            self.page_cache.insert(page_number, new_page)?;

            page_number
        } else {
            // loads to cache for use.
            let free_page = self.read_page(tx, first_freelist_page)?;

            self.db_header
                .set_first_freelist_page(free_page.lock_shared().freelist_next());
            self.db_header.sub_freelist_pages(1);

            {
                let guard = free_page.lock_exclusive();
                guard.set_loaded();
            }

            self.mark_dirty(&free_page);

            free_page.id()
        };

        self.write_header(tx)?;

        Ok(free_page)
    }

    /// Adds page to global freelist.
    pub fn free_page<Tx: WriteTx>(
        &self,
        tx: &mut Tx,
        page_number: PageNumber,
    ) -> storage::Result<()> {
        let page = self.read_page(tx, page_number)?;

        let prev_head = self.db_header.get_first_freelist_page();

        // set new head to point to prev freelist head.
        {
            let guard = page.lock_exclusive();
            guard.freelist_set(prev_head);
        }

        self.db_header.set_first_freelist_page(page_number);
        self.db_header.add_freelist_pages(1);

        self.mark_dirty(&page);

        self.write_header(tx)
    }

    /// Marks page as dirty to later identify page to flush durring checkpoint
    /// or when cache is full.
    pub fn mark_dirty(&self, page: &MemPageRef) {
        self.dirty_pages.insert(page.id());
        page.set_dirty();
    }

    /// Marks given page as dirty and can trigger cache flush, after it reaches
    /// `cache_flush_threshold`.
    pub fn mark_dirty_auto_flush<Tx: WriteTx>(
        &self,
        tx: &mut Tx,
        page: &MemPageRef,
    ) -> storage::Result<()> {
        self.dirty_pages.insert(page.id());
        page.set_dirty();

        if self.dirty_pages.len() > self.cache_flush_threshold {
            self.flush_dirty(tx, false)?;
        }

        Ok(())
    }

    /// Flushes all pages marked as dirty to WAL.
    pub fn flush_dirty<Tx: WriteTx>(
        &self,
        tx: &mut Tx,
        should_commit: bool,
    ) -> storage::Result<()> {
        let dirty_page_numbers: Vec<_> = self.dirty_pages.iter().map(|pn| *pn).collect();

        log::trace!("Flushing dirty pages: {:?}", dirty_page_numbers);

        let dirty_pages: Vec<_> = dirty_page_numbers
            .iter()
            .map(|pn| self.read_page(tx, *pn).unwrap())
            .collect();

        let mut page_guards: Vec<_> = dirty_pages
            .iter()
            .map(|page| page.lock_exclusive())
            .collect();

        let db_size = if should_commit {
            self.db_header.get_database_size()
        } else {
            0
        };

        self.wal.append_vectored(tx, &mut page_guards, db_size)?;

        dirty_page_numbers.iter().for_each(|pn| {
            self.dirty_pages.remove(&*pn);
        });

        Ok(())
    }

    // pub fn flush(&mut self) -> storage::Result<()> {
    //     Ok(self.io.flush()?)
    // }

    // pub fn sync(&self) -> storage::Result<()> {
    //     Ok(self.io.sync()?)
    // }
}

/// Takes reference to write result and consumes exclusive lock to page. If
/// write result is successful, then it clears dirty state and if `clean` is
/// true, it also removes page from memory.
pub fn complete_write_page(
    write_result: &std::io::Result<usize>,
    page: &mut ExclusivePageGuard,
    clean: bool,
) {
    if write_result.is_err() {
        page.set_error();
    } else {
        page.clear_dirty();
        if clean {
            page.clear_loaded();
            let _ = page.take();
        }
    }
}

#[cfg(test)]
mod tests {}
