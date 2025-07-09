use std::{cell::RefCell, collections::HashMap, ptr::NonNull};

use thiserror::Error;

use crate::storage::{PageNumber, pager::MemPageRef};

pub type CacheResult<T> = Result<T, CacheError>;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("key already exists in cache.")]
    KeyExists,
    #[error("page is currently locked.")]
    PageLocked,
    #[error("page {id} is dirty.")]
    Dirty { id: PageNumber },
}

pub type PageCacheKey = PageNumber;

/// LRU cache entry that stores reference to a `MemPage`.
pub struct PageCacheEntry<'a> {
    /// Page number of entry
    key: PageCacheKey,
    /// Shared reference to `MemPage`
    page: MemPageRef<'a>,
    /// Pointer to previous node.
    prev: Option<NonNull<PageCacheEntry<'a>>>,
    /// Pointer to next node.
    next: Option<NonNull<PageCacheEntry<'a>>>,
}

impl<'a> PageCacheEntry<'a> {
    pub fn new(page: MemPageRef<'a>) -> Self {
        Self {
            key: page.get().id,
            page,
            prev: None,
            next: None,
        }
    }
}

/// Simple LRU cache implementation using hashmap and doubly linked list.
pub struct LruPageCache<'a> {
    /// Total capacity of cache (in pages)
    capacity: usize,
    /// Hashmap of cache entries.
    map: RefCell<HashMap<PageCacheKey, NonNull<PageCacheEntry<'a>>>>,
    /// Head of doubly linked list.
    head: RefCell<Option<NonNull<PageCacheEntry<'a>>>>,
    /// Tail of doubly ll.
    tail: RefCell<Option<NonNull<PageCacheEntry<'a>>>>,
}

impl<'a> LruPageCache<'a> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Cache capacity must be grater than 0.");

        Self {
            capacity,
            map: RefCell::new(HashMap::with_capacity(capacity)),
            head: RefCell::new(None),
            tail: RefCell::new(None),
        }
    }

    /// Checks if page is in cache.
    pub fn contains_key(&mut self, key: &PageCacheKey) -> bool {
        self.map.borrow().contains_key(key)
    }

    fn try_insert(&mut self, key: PageCacheKey, page: MemPageRef) -> CacheResult<()> {
        if self.contains_key(&key) {
            return Err(CacheError::KeyExists);
        }

        

        Ok(())
    }

    /// Removes entry from linked list and eventiualy cleans up page from memory (returns `Buffer` to `BufferPool`). 
    fn detach(
        &mut self,
        mut entry: NonNull<PageCacheEntry<'a>>,
        clean_page: bool,
    ) -> CacheResult<()> {
        let entry_mut = unsafe { entry.as_mut() };

        if entry_mut.page.is_locked() {
            return Err(CacheError::PageLocked);
        }
        if entry_mut.page.is_dirty() {
            return Err(CacheError::Dirty {
                id: entry_mut.page.get().id,
            });
        }

        if clean_page {
            entry_mut.page.clear_loaded();
            log::debug!("cleaining up page {}", entry_mut.page.get().id);
            let _ = entry_mut.page.get().content.take();
        }

        self.unlink(entry);

        Ok(())
    }

    /// Disconnects entry from linked list.
    fn unlink(&mut self, mut entry: NonNull<PageCacheEntry<'a>>) {
        let (next, prev) = unsafe {
            let entry_mut = entry.as_mut();
            let next = entry_mut.next;
            let prev = entry_mut.prev;

            entry_mut.next = None;
            entry_mut.prev = None;

            (next, prev)
        };

        match (prev, next) {
            (None, None) => {
                self.head.replace(None);
                self.tail.replace(None);
            }
            (None, Some(mut n)) => {
                unsafe { n.as_mut().prev = None };
                self.head.borrow_mut().replace(n);
            }
            (Some(mut p), None) => {
                unsafe { p.as_mut().next = None };
                self.tail.borrow_mut().replace(p);
            }
            (Some(mut p), Some(mut n)) => unsafe {
                p.as_mut().next = Some(n);
                n.as_mut().prev = Some(p)
            },
        }
    }

    fn touch(&mut self, entry: NonNull<PageCacheEntry<'a>>) {
        todo!()
    }
}
