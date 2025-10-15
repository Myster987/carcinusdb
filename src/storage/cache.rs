use std::{fmt::Debug, ptr::NonNull};

use dashmap::DashMap;
use parking_lot::Mutex;
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
    #[error("cache is already full.")]
    Full,
    #[error("{0}")]
    Internal(String),
}

pub type PageCacheKey = PageNumber;

/// LRU cache entry that stores reference to a `MemPage`.
pub struct PageCacheEntry {
    /// Page number of entry
    key: PageCacheKey,
    /// Shared reference to `MemPage`
    page: MemPageRef,
    /// Pointer to previous node.
    prev: Option<NonNull<PageCacheEntry>>,
    /// Pointer to next node.
    next: Option<NonNull<PageCacheEntry>>,
}

impl PageCacheEntry {
    pub fn new(key: PageCacheKey, page: MemPageRef) -> Self {
        Self {
            key,
            page,
            prev: None,
            next: None,
        }
    }
}

/// In LRU wrapped in mutex to protect head and tail from concurrent access.
pub struct CacheLinkeListState {
    /// Head of doubly linked list.
    head: Option<NonNull<PageCacheEntry>>,
    /// Tail of doubly ll.
    tail: Option<NonNull<PageCacheEntry>>,
}

impl CacheLinkeListState {
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }
}

/// Simple LRU cache implementation using hashmap and doubly linked list.
pub struct LruPageCache {
    /// Total capacity of cache (in pages)
    capacity: usize,
    /// Hashmap of cache entries.
    map: DashMap<PageCacheKey, NonNull<PageCacheEntry>>,
    linked_list: Mutex<CacheLinkeListState>,
}

impl LruPageCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Cache capacity must be grater than 0.");

        Self {
            capacity,
            map: DashMap::with_capacity(capacity),
            linked_list: Mutex::new(CacheLinkeListState::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Checks if page is in cache.
    pub fn contains_key(&self, key: &PageCacheKey) -> bool {
        self.map.contains_key(key)
    }

    /// Returns pointer to entry and copies it (coping pointer is cheap). If entry doesn't exist, it returns None.
    fn get_ptr(&self, key: &PageCacheKey) -> Option<NonNull<PageCacheEntry>> {
        self.map.get(key).map(|r| *r)
    }

    pub fn peek(&self, key: &PageCacheKey, touch: bool) -> Option<MemPageRef> {
        let mut ptr = self.get_ptr(key)?;
        if touch {
            self.unlink(ptr);
            self.touch(ptr);
        }
        let page = unsafe { ptr.as_mut().page.clone() };
        Some(page)
    }

    pub fn get(&self, key: &PageCacheKey) -> Option<MemPageRef> {
        self.peek(key, true)
    }

    pub fn insert(&self, key: PageCacheKey, page: MemPageRef) -> CacheResult<()> {
        self.try_insert(key, page)
    }

    fn try_insert(&self, key: PageCacheKey, page: MemPageRef) -> CacheResult<()> {
        if self.contains_key(&key) {
            return Err(CacheError::KeyExists);
        }

        self.make_room_for(1)?;

        let entry = Box::new(PageCacheEntry::new(key, page));
        let entry_ptr = unsafe { NonNull::new_unchecked(Box::into_raw(entry)) };

        self.touch(entry_ptr);

        self.map.insert(key, entry_ptr);

        Ok(())
    }

    pub fn delete(&self, key: PageCacheKey) -> CacheResult<()> {
        self.try_delete(key, true)
    }

    fn try_delete(&self, key: PageCacheKey, clean_page: bool) -> CacheResult<()> {
        if !self.contains_key(&key) {
            return Ok(());
        }

        let entry = *self.map.get(&key).unwrap();
        // Detach before deleting.
        self.detach(entry, clean_page)?;

        let (_, ptr) = self.map.remove(&key).unwrap();
        unsafe {
            // Creates Box that owns entry and is dropped
            let _ = Box::from_raw(ptr.as_ptr());
        };

        Ok(())
    }

    /// Removes entry from linked list and eventiualy cleans up page from memory (returns `Buffer` to `BufferPool`).
    fn detach(&self, mut entry: NonNull<PageCacheEntry>, clean_page: bool) -> CacheResult<()> {
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
    fn unlink(&self, mut entry: NonNull<PageCacheEntry>) {
        let (next, prev) = unsafe {
            let entry_mut = entry.as_mut();
            let next = entry_mut.next;
            let prev = entry_mut.prev;

            entry_mut.next = None;
            entry_mut.prev = None;

            (next, prev)
        };
        let mut linked_list = self.linked_list.lock();

        match (prev, next) {
            (None, None) => {
                linked_list.head = None;
                linked_list.tail = None;
            }
            (None, Some(mut n)) => {
                unsafe { n.as_mut().prev = None };
                linked_list.head.replace(n);
            }
            (Some(mut p), None) => {
                unsafe { p.as_mut().next = None };
                linked_list.tail.replace(p);
            }
            (Some(mut p), Some(mut n)) => unsafe {
                p.as_mut().next = Some(n);
                n.as_mut().prev = Some(p)
            },
        }
    }

    /// Inserts entry before head. To work correctly use `Self::detach` before.
    fn touch(&self, mut entry: NonNull<PageCacheEntry>) {
        let mut linked_list = self.linked_list.lock();

        if let Some(mut head) = linked_list.head {
            unsafe {
                entry.as_mut().next.replace(head);
                head.as_mut().prev = Some(entry);
            }
        }

        if linked_list.tail.is_none() {
            linked_list.tail.replace(entry);
        }
        linked_list.head.replace(entry);
    }

    fn make_room_for(&self, entries_num: usize) -> CacheResult<()> {
        let linked_list = self.linked_list.lock();

        if entries_num > self.capacity {
            return Err(CacheError::Full);
        }

        let len = self.len();
        let avalible = self.capacity.saturating_sub(len);

        if entries_num <= avalible {
            return Ok(());
        }

        // Now we need to handle case when there are too many entires, so we need to delete oldest ones (closest to the tail)
        let tail = linked_list.tail.ok_or(CacheError::Internal(format!(
            "Page cache of length {} exprected to have a tail",
            self.len()
        )))?;

        let mut to_delete = entries_num - avalible;
        let mut current_entry = Some(tail);

        while to_delete > 0 && current_entry.is_some() {
            let current = current_entry.unwrap();
            let entry = unsafe { current.as_ref() };
            let key_to_delete = entry.key;
            current_entry = entry.prev;

            self.delete(key_to_delete).inspect(|_| to_delete -= 1)?;
        }

        if to_delete > 0 {
            return Err(CacheError::Full);
        }

        Ok(())
    }

    /// Deletes all entries. Sets head and tail to `None`.
    pub fn clear(&self) -> CacheResult<()> {
        let mut linked_list = self.linked_list.lock();
        let mut current = linked_list.head;

        while let Some(mut c) = current {
            let entry = unsafe { c.as_mut() };
            let next = entry.next;

            self.map.remove(&entry.key);

            self.detach(c, true)?;

            assert!(!entry.page.is_dirty());

            unsafe {
                let _ = Box::from_raw(c.as_ptr());
            }

            current = next;
        }

        let _ = linked_list.head.take();
        let _ = linked_list.tail.take();

        assert!(linked_list.head.is_none());
        assert!(linked_list.tail.is_none());
        assert!(self.map.is_empty());

        Ok(())
    }
}

impl Debug for LruPageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let linked_list = self.linked_list.lock();
        let mut debug_vec = vec![];
        let mut current = linked_list.head;

        while let Some(c) = current {
            let entry = unsafe { c.as_ref() };
            debug_vec.push(entry.key.to_string());

            current = entry.next;
        }
        f.write_str(&format!("LruPageCache({})", debug_vec.join(" -> ")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::storage::pager::MemPage;

    use super::*;

    #[test]
    fn main_test() -> anyhow::Result<()> {
        let mut lry_cache = LruPageCache::new(5);

        for i in 1..=5 {
            let page = Arc::new(MemPage::new(i));
            lry_cache.insert(i, page)?;
        }

        let _ = lry_cache.get(&3);

        let i = 6;
        let page = Arc::new(MemPage::new(i));
        lry_cache.insert(i, page)?;

        println!("{:?}", lry_cache);

        lry_cache.delete(5)?;
        let entry = lry_cache.get(&2);

        if let Some(e) = entry {
            println!("{:?}", e.get().flags);
            println!("{:?}", Arc::strong_count(&e));
        }

        println!("{:?}", lry_cache);

        lry_cache.clear()?;
        println!("{:?}", lry_cache);

        Ok(())
    }
}
