use std::cell::UnsafeCell;
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::database::MemDatabaseHeader;
use crate::storage::btree::BTreeType;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::cache::ShardedClockCache;
use crate::storage::page::{DATABASE_HEADER_PAGE_NUMBER, DATABASE_HEADER_SIZE, PageType};
use crate::storage::wal::LocalWal;
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

    pub fn into_pin<T>(self, inner: T) -> PagePin<Self, T> {
        PagePin::new(self, inner)
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

    pub fn into_pin<T>(self, inner: T) -> PagePin<Self, T> {
        PagePin::new(self, inner)
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

/// Wrapper that will hold lock to page until it's dropped.
pub struct PagePin<G, T> {
    pub guard: G,
    inner: Option<T>,
}

impl<G, T> PagePin<G, T> {
    pub fn new(guard: G, inner: T) -> Self {
        Self {
            guard,
            inner: Some(inner),
        }
    }
}

impl<G, T> Deref for PagePin<G, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl<T> DerefMut for PagePin<ExclusivePageGuard, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
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
    wal: LocalWal,
    /// Reference to global LRU cache
    pub page_cache: Arc<ShardedClockCache>,
}

impl Pager {
    pub fn new(
        db_header: Arc<MemDatabaseHeader>,
        io: Arc<BlockIO<File>>,
        buffer_pool: Arc<BufferPool>,
        wal: LocalWal,
        page_cache: Arc<ShardedClockCache>,
    ) -> Self {
        Self {
            db_header,
            io,
            buffer_pool,
            wal,
            page_cache,
        }
    }

    pub fn write_header(&mut self) -> storage::Result<()> {
        let raw_header = self.db_header.into_raw_header();

        let header_page = self.read_page(DATABASE_HEADER_PAGE_NUMBER)?;
        let guard = header_page.lock_exclusive();
        guard.write_db_header(raw_header);

        self.wal.append_frame(guard, raw_header.database_size)
    }

    pub fn read_page(&mut self, page_number: PageNumber) -> storage::Result<MemPageRef> {
        // check cache...
        if let Some(cached_page) = self.page_cache.get(&page_number) {
            return Ok(cached_page);
        }

        let page_wrapper = Arc::new(MemPage::new(page_number));

        {
            // lock for IO operations
            let mut guard = page_wrapper.lock_exclusive();

            // check wall...
            if let Ok(Some(page)) = self.wal.read_frame(page_number, &self.buffer_pool) {
                *guard = page;
            } else {
                // read from disk
                let page = self.read_page_from_disk(page_number)?;
                guard.replace(page);
            }
            guard.set_loaded();
        } // unlock

        self.page_cache.insert(page_number, page_wrapper.clone())?;

        Ok(page_wrapper)
    }

    /// Reads page from disk at given `page_number`.
    ///
    /// # Safety
    ///
    /// Page that needs to be read should already be write-locked.
    fn read_page_from_disk(&mut self, page_number: PageNumber) -> storage::Result<Page> {
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

    pub fn write_page(&mut self, page: MemPageRef) -> storage::Result<()> {
        // begin_write_page(&page)?;

        let guard = page.lock_exclusive();

        self.wal
            .append_frame(guard, self.db_header.get_database_size())
    }

    pub fn btree_create(&mut self, btree_type: BTreeType) -> storage::Result<PageNumber> {
        let page_type = match btree_type {
            BTreeType::Index => PageType::IndexLeaf,
            BTreeType::Table => PageType::TableLeaf,
        };

        self.alloc_page(page_type)
    }

    /// Allocates new page in db file. By default tries to use page from
    /// freelist and if freelist is empty, then it creates new page at the
    /// end of db file.
    pub fn alloc_page(&mut self, page_type: PageType) -> storage::Result<PageNumber> {
        let first_freelist_page = self.db_header.get_first_freelist_page();

        let free_page = if first_freelist_page == 0 {
            let page_number = self.db_header.add_database_size(1);

            let new_page = Page::new(0, self.buffer_pool.get());
            new_page.initialize(page_type);

            let new_page = Arc::new(MemPage::from_page(page_number, new_page));
            new_page.set_dirty();
            new_page.set_loaded();

            // we have to manually insert new page to cache and set it dirty
            self.page_cache.insert(page_number, new_page)?;

            page_number
        } else {
            // loads to cache for use.
            let free_page = self.read_page(first_freelist_page)?;

            self.db_header
                .set_first_freelist_page(free_page.lock_shared().freelist_next());
            self.db_header.sub_freelist_pages(1);

            {
                let guard = free_page.lock_exclusive();
                guard.initialize(page_type);
                guard.set_dirty();
                guard.set_loaded();
            }

            free_page.id()
        };

        self.write_header()?;

        Ok(free_page)
    }

    pub fn flush(&mut self) -> storage::Result<()> {
        Ok(self.io.flush()?)
    }

    pub fn sync(&self) -> storage::Result<()> {
        Ok(self.io.sync()?)
    }
}

// /// Prepares page for read operation
// pub fn begin_read_page(page: &MemPageRef) -> storage::Result<()> {
//     page.set_locked();
//     Ok(())
// }

// pub fn begin_write_page(page: &MemPageRef) -> storage::Result<()> {
//     page.set_locked();
//     Ok(())
// }

// /// Should be universal to both disk and wal reads. Takes `IoResult` with page
// /// and buffer and if it is an error it will set proper flags on page. Otherwise
// /// it will set correct state and value for page.
// /// ### Note that `page` should be locked before calling this function (use `begin_read_page` to setup page for read)
// pub fn complete_read_page(read_result: &io::Result<usize>, page: &MemPageRef, buffer: Buffer) {
//     if let Ok(bytes_read) = read_result {
//         if *bytes_read != buffer.size() {
//             page.set_error();
//             page.clear_loaded();
//             page.clear_locked();
//             return;
//         }
//     }
//     if let Err(_) = read_result {
//         page.set_error();
//         page.clear_locked();
//         return;
//     }

//     let content = Page::new(buffer);

//     page.get().content = Some(content);
//     page.set_loaded();
//     page.clear_locked();
// }

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
mod tests {
    use super::*;

    #[test]
    fn test_pager() -> anyhow::Result<()> {
        let db = crate::database::Database::new("./db-file".into())?;

        let mut pager = db.pager();

        // println!("{:?}", *pager.read_page(new_page)?.lock_shared());

        Ok(())
    }
}
