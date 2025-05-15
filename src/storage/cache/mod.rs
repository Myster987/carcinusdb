use std::ptr::{self, NonNull};

use dashmap::DashMap;

use super::{PageNumber, pager::MemPageRef};

pub struct PageCacheEntry {
    page_number: PageNumber,
    page: MemPageRef,
    next: Option<NonNull<PageCacheEntry>>,
    prev: Option<NonNull<PageCacheEntry>>,
}

impl PageCacheEntry {
    fn as_non_null(&mut self) -> NonNull<Self> {
        NonNull::new(ptr::from_mut(self)).unwrap()
    }
}

pub struct LRUCache {
    len: usize,
    capacity: usize,
    map: DashMap<PageNumber, PageCacheEntry>,
    head: Option<NonNull<PageCacheEntry>>,
    tail: Option<NonNull<PageCacheEntry>>,
}

impl LRUCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            len: 0,
            capacity,
            map: DashMap::with_capacity(capacity),
            head: None,
            tail: None,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_cached(&self, key: &PageNumber) -> bool {
        self.map.contains_key(key)
    }

    pub fn get_ptr(&self, key: &PageNumber) -> Option<NonNull<PageCacheEntry>> {
        if let Some(entry) = self.map.get(key) {
            Some(NonNull::new(ptr::from_ref(&*entry).cast_mut()).unwrap())
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: PageNumber, value: MemPageRef) {
        let mut entry = PageCacheEntry {
            page_number: key.clone(),
            page: value,
            next: None,
            prev: None,
        };

        // creates non-null raw pointer to entry for insertion. This code is safe, entry is initialized above so it will be 100% valid 
        let entry_ptr = unsafe { Some(NonNull::new_unchecked(ptr::from_mut(&mut entry))) };

        match (self.head, self.tail) {
            (None, None) => {
                self.head = entry_ptr;
                self.tail = entry_ptr;
            }
            (Some(mut head_ptr), Some(_)) => {
                // cast non-null raw pointer into mutable reference to maintain correct order of entries
                let head = unsafe { head_ptr.as_mut() };
                entry.next = head.next;
                unsafe { entry.next.unwrap().as_mut().prev = entry_ptr };
                entry.prev = Some(head_ptr);
                head.next = entry_ptr;
            }
            _ => {}
        }

        self.len += 1;

        // we have to delete oldest node from cache (last one/tail)
        if self.len > self.capacity {
            
        }

        self.map.insert(key, entry);
    }

    // fn detach(&mut self, entry: NonNull<PageCacheEntry>) {
    //     let (next, prev) = unsafe {
    //         let page = entry.as_mut();
    //         let next = page.next;
    //         let prev = page.prev;

    //         page.next = None;
    //         page.prev = None;

    //         (next, prev)
    //     }

    //     match (next, prev) {
    //         (None, None) => {
    //             self.head.replace(None);
    //         }
    //     }
    // }
}
