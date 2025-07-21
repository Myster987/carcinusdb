use std::{
    cell::RefCell, collections::HashMap, fmt::Debug, io::Cursor, ptr::NonNull, sync::Arc,
    thread::current,
};

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
    pub fn new(key: PageCacheKey, page: MemPageRef<'a>) -> Self {
        Self {
            key,
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

    pub fn len(&self) -> usize {
        self.map.borrow().len()
    }

    /// Checks if page is in cache.
    pub fn contains_key(&mut self, key: &PageCacheKey) -> bool {
        self.map.borrow().contains_key(key)
    }

    /// Returns pointer to entry and copies it (coping pointer is cheap). If entry doesn't exist, it returns None.
    fn get_ptr(&self, key: &PageCacheKey) -> Option<NonNull<PageCacheEntry<'a>>> {
        let map = self.map.borrow();
        map.get(key).copied()
    }

    pub fn peek(&mut self, key: &PageCacheKey, touch: bool) -> Option<MemPageRef<'a>> {
        let mut ptr = self.get_ptr(key)?;
        if touch {
            self.unlink(ptr);
            self.touch(ptr);
        }
        let page = unsafe { ptr.as_mut().page.clone() };
        Some(page)
    }

    pub fn get(&mut self, key: &PageCacheKey) -> Option<MemPageRef<'a>> {
        self.peek(key, true)
    }

    pub fn insert(&mut self, key: PageCacheKey, page: MemPageRef<'a>) -> CacheResult<()> {
        self.try_insert(key, page)
    }

    fn try_insert(&mut self, key: PageCacheKey, page: MemPageRef<'a>) -> CacheResult<()> {
        if self.contains_key(&key) {
            return Err(CacheError::KeyExists);
        }

        self.make_room_for(1)?;

        let entry = Box::new(PageCacheEntry::new(key, page));
        let entry_ptr = unsafe { NonNull::new_unchecked(Box::into_raw(entry)) };

        self.touch(entry_ptr);

        self.map.borrow_mut().insert(key, entry_ptr);

        Ok(())
    }

    pub fn delete(&mut self, key: PageCacheKey) -> CacheResult<()> {
        self.try_delete(key, true)
    }

    fn try_delete(&mut self, key: PageCacheKey, clean_page: bool) -> CacheResult<()> {
        if !self.contains_key(&key) {
            return Ok(());
        }

        let entry = *self.map.borrow_mut().get(&key).unwrap();
        // Detach before deleting.
        self.detach(entry, clean_page)?;

        let ptr = self.map.borrow_mut().remove(&key).unwrap();
        unsafe {
            // Creates Box that owns entry and is dropped
            let _ = Box::from_raw(ptr.as_ptr());
        };

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

    /// Inserts entry before head. To work correctly use `Self::detach` before.
    fn touch(&mut self, mut entry: NonNull<PageCacheEntry<'a>>) {
        if let Some(mut head) = *self.head.borrow_mut() {
            unsafe {
                entry.as_mut().next.replace(head);
                head.as_mut().prev = Some(entry);
            }
        }

        if self.tail.borrow().is_none() {
            self.tail.borrow_mut().replace(entry);
        }
        self.head.replace(Some(entry));
    }

    fn make_room_for(&mut self, entries_num: usize) -> CacheResult<()> {
        if entries_num > self.capacity {
            return Err(CacheError::Full);
        }

        let len = self.len();
        let avalible = self.capacity.saturating_sub(len);

        if entries_num <= avalible {
            return Ok(());
        }

        // Now we need to handle case when there are too many entires, so we need to delete oldest ones (closest to the tail)
        let tail = self.tail.borrow().ok_or(CacheError::Internal(format!(
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
    pub fn clear(&mut self) -> CacheResult<()> {
        let mut current = *self.head.borrow();

        while let Some(mut c) = current {
            let entry = unsafe { c.as_mut() };
            let next = entry.next;

            self.map.borrow_mut().remove(&entry.key);

            self.detach(c, true)?;

            assert!(!entry.page.is_dirty());

            unsafe {
                let _ = Box::from_raw(c.as_ptr());
            }

            current = next;
        }

        let _ = self.head.take();
        let _ = self.tail.take();

        assert!(self.head.borrow().is_none());
        assert!(self.tail.borrow().is_none());
        assert!(self.map.borrow().is_empty());

        Ok(())
    }
}

impl<'a> Debug for LruPageCache<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_vec = vec![];
        let mut current = *self.head.borrow();

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
