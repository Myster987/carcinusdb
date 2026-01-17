use std::{
    collections::HashMap,
    fmt::Debug,
    hash::{BuildHasher, Hash, Hasher, RandomState},
    ptr::NonNull,
};

use parking_lot::Mutex;
use thiserror::Error;

use crate::storage::{PageNumber, pager::MemPageRef};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("key already exists in cache.")]
    KeyExists,
    #[error("page is currently pinned.")]
    PagePinned,
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

pub struct ShardedLruCache<S: BuildHasher = RandomState> {
    shards: Vec<Shard>,
    hasher: S,
}

unsafe impl Send for ShardedLruCache {}
unsafe impl Sync for ShardedLruCache {}

const DEFAULT_SHARD_COUNT: usize = 8;

impl ShardedLruCache {
    pub fn new(capacity: usize) -> Self {
        let per_shard_capacity = (capacity + DEFAULT_SHARD_COUNT - 1) / DEFAULT_SHARD_COUNT;

        Self {
            shards: (0..DEFAULT_SHARD_COUNT)
                .map(|_| Mutex::new(LruPageCache::new(per_shard_capacity)))
                .collect(),
            hasher: RandomState::default(),
        }
    }
}

impl<S: BuildHasher> ShardedLruCache<S> {
    fn get_shard(&self, key: &PageCacheKey) -> &Shard {
        let mut hasher = self.hasher.build_hasher();

        key.hash(&mut hasher);

        let h = hasher.finish() as usize;
        let shard_idx = h % DEFAULT_SHARD_COUNT;

        &self.shards[shard_idx]
    }

    pub fn get(&self, key: &PageCacheKey) -> Option<MemPageRef> {
        self.get_shard(key).lock().get(key)
    }

    pub fn insert(&self, key: PageCacheKey, page: MemPageRef) -> Result<()> {
        self.get_shard(&key).lock().insert(key, page)
    }

    pub fn delete(&self, key: PageCacheKey) -> Result<()> {
        self.get_shard(&key).lock().delete(key)
    }

    pub fn clear(&self) -> Result<()> {
        for shard in &self.shards {
            shard.lock().clear()?;
        }

        Ok(())
    }
}

impl<S: BuildHasher> Drop for ShardedLruCache<S> {
    fn drop(&mut self) {
        self.clear().unwrap();
    }
}

impl Debug for ShardedLruCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut shards_representation = Vec::with_capacity(self.shards.len());

        for (i, shard) in self.shards.iter().enumerate() {
            shards_representation.push(format!("shard {i}: {:?}", shard.lock()));
        }

        f.write_str(&format!(
            "ShardedLruCache({}\n)",
            shards_representation.join(",\n ")
        ))
    }
}

type Shard = Mutex<LruPageCache>;

/// Simple LRU cache implementation using hashmap and doubly linked list.
struct LruPageCache {
    /// Total capacity of cache (in pages)
    capacity: usize,
    /// Hashmap of cache entries.
    map: HashMap<PageCacheKey, NonNull<PageCacheEntry>>,
    /// Head of doubly linked list.
    head: Option<NonNull<PageCacheEntry>>,
    /// Tail of doubly ll.
    tail: Option<NonNull<PageCacheEntry>>,
}

impl LruPageCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Cache capacity must be grater than 0.");

        Self {
            capacity,
            map: HashMap::with_capacity(capacity),
            head: None,
            tail: None,
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

    pub fn peek(&mut self, key: &PageCacheKey, touch: bool) -> Option<MemPageRef> {
        let mut ptr = self.get_ptr(key)?;
        if touch {
            self.unlink(ptr);
            self.touch(ptr);
        }
        let page = unsafe { ptr.as_mut().page.clone() };
        Some(page)
    }

    pub fn get(&mut self, key: &PageCacheKey) -> Option<MemPageRef> {
        self.peek(key, true)
    }

    pub fn insert(&mut self, key: PageCacheKey, page: MemPageRef) -> Result<()> {
        self.try_insert(key, page)
    }

    fn try_insert(&mut self, key: PageCacheKey, page: MemPageRef) -> Result<()> {
        if self.contains_key(&key) {
            return Err(Error::KeyExists);
        }

        self.make_room_for(1)?;

        let entry = Box::new(PageCacheEntry::new(key, page));
        let entry_ptr = unsafe { NonNull::new_unchecked(Box::into_raw(entry)) };

        self.touch(entry_ptr);

        self.map.insert(key, entry_ptr);

        Ok(())
    }

    pub fn delete(&mut self, key: PageCacheKey) -> Result<()> {
        self.try_delete(key, true)
    }

    fn try_delete(&mut self, key: PageCacheKey, clean_page: bool) -> Result<()> {
        if !self.contains_key(&key) {
            return Ok(());
        }

        let entry = *self.map.get(&key).unwrap();
        // Detach before deleting.
        self.detach(entry, clean_page)?;

        let ptr = self.map.remove(&key).unwrap();
        unsafe {
            // Creates Box that owns entry and is dropped
            let _ = Box::from_raw(ptr.as_ptr());
        };

        Ok(())
    }

    /// Removes entry from linked list and eventiualy cleans up page from memory (returns `Buffer` to `BufferPool`).
    fn detach(&mut self, mut entry: NonNull<PageCacheEntry>, clean_page: bool) -> Result<()> {
        let entry_mut = unsafe { entry.as_mut() };

        if entry_mut.page.is_dirty() {
            return Err(Error::Dirty {
                id: entry_mut.page.id(),
            });
        }

        if entry_mut.page.pin_count() > 0 {
            return Err(Error::PagePinned);
        }

        if clean_page {
            let mut guard = entry_mut.page.lock_exclusive();
            log::debug!("cleaining up page {}", entry_mut.page.id());
            let _ = guard.take();
        }

        self.unlink(entry);

        Ok(())
    }

    /// Disconnects entry from linked list.
    fn unlink(&mut self, mut entry: NonNull<PageCacheEntry>) {
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
                self.head = None;
                self.tail = None;
            }
            (None, Some(mut n)) => {
                unsafe { n.as_mut().prev = None };
                self.head.replace(n);
            }
            (Some(mut p), None) => {
                unsafe { p.as_mut().next = None };
                self.tail.replace(p);
            }
            (Some(mut p), Some(mut n)) => unsafe {
                p.as_mut().next = Some(n);
                n.as_mut().prev = Some(p)
            },
        }
    }

    /// Inserts entry before head. To work correctly use `Self::detach` before.
    fn touch(&mut self, mut entry: NonNull<PageCacheEntry>) {
        if let Some(mut head) = self.head {
            unsafe {
                entry.as_mut().next.replace(head);
                head.as_mut().prev = Some(entry);
            }
        }

        if self.tail.is_none() {
            self.tail.replace(entry);
        }
        self.head.replace(entry);
    }

    fn make_room_for(&mut self, entries_num: usize) -> Result<()> {
        if entries_num > self.capacity {
            return Err(Error::Full);
        }

        let len = self.len();
        let avalible = self.capacity.saturating_sub(len);

        if entries_num <= avalible {
            return Ok(());
        }

        // Now we need to handle case when there are too many entires, so we need to delete oldest ones (closest to the tail)
        let tail = self.tail.ok_or(Error::Internal(format!(
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

            // don't bubble up error, just ignore it and look for another page
            // to delete.
            let _ = self.delete(key_to_delete).inspect(|_| to_delete -= 1);
        }

        if to_delete > 0 {
            return Err(Error::Full);
        }

        Ok(())
    }

    /// Deletes all entries. Sets head and tail to `None`.
    pub fn clear(&mut self) -> Result<()> {
        let mut current = self.head;

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

        let _ = self.head.take();
        let _ = self.tail.take();

        assert!(self.head.is_none());
        assert!(self.tail.is_none());
        assert!(self.map.is_empty());

        Ok(())
    }
}

impl Debug for LruPageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_vec = vec![];
        let mut current = self.head;

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
            println!("{:?}", Arc::strong_count(&e));
        }

        println!("{:?}", lry_cache);

        lry_cache.clear()?;
        println!("{:?}", lry_cache);

        Ok(())
    }

    #[test]
    fn test_sharded() -> anyhow::Result<()> {
        let sharded = Arc::new(ShardedLruCache::new(100));
        let mut threads = Vec::new();

        for i in 0..2 {
            let sharded_clone = sharded.clone();
            threads.push(std::thread::spawn(move || {
                for j in 10 * i..10 * (i + 1) {
                    let _ = sharded_clone.insert(j, Arc::new(MemPage::new(j)));
                }
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        println!("{:?}", sharded);

        Ok(())
    }
}
