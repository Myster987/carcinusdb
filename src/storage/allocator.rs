use std::{
    fmt::Debug,
    ptr::NonNull,
    sync::Arc,
};

use crossbeam::queue::SegQueue;

use crate::utils::buffer::{BUFFER_ALIGNMENT, alloc_heap, dealloc_heap};

pub struct Node<T> {
    data: T,
    next: Option<Box<Node<T>>>,
}

pub struct LocalStack<T> {
    head: Option<Box<Node<T>>>,
    len: usize,
}

impl<T> LocalStack<T> {
    pub fn new() -> Self {
        Self { head: None, len: 0 }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn push(&mut self, value: T) {
        self.len += 1;
        let node = Box::new(Node {
            data: value,
            next: self.head.take(),
        });
        self.head = Some(node);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.head.take().map(|node| {
            self.len -= 1;
            self.head = node.next;
            node.data
        })
    }
}

impl<T> Drop for LocalStack<T> {
    fn drop(&mut self) {
        let mut current = self.head.take();
        while let Some(mut node) = current {
            current = node.next.take();
        }
    }
}

const DEFAULT_GLOBAL_BATCH_SIZE: usize = 32;
const DEFAULT_LOCAL_BATCH_SIZE: usize = 16;
const DEFAULT_LOCAL_MIN_PAGES: usize = 8;
const DEFAULT_LOCAL_MAX_PAGES: usize = 64;

pub struct GlobalPageAllocator {
    freelist: SegQueue<NonNull<u8>>,
    pub page_size: usize,

    /// Size of global batch allocation
    global_batch_size: usize,
    /// Size of local batch allocation
    local_batch_size: usize,
    /// Min number of pages in local allocator
    local_min_pages: usize,
    /// Max number of pages in local allocator
    local_max_pages: usize,
}

impl GlobalPageAllocator {
    pub fn new(
        page_size: usize,
        page_count: usize,
        global_batch_size: usize,
        local_batch_size: usize,
        local_min_pages: usize,
        local_max_pages: usize,
    ) -> Self {
        let freelist = SegQueue::new();

        for _ in 0..page_count {
            let heap_ptr = unsafe { alloc_heap(page_size, BUFFER_ALIGNMENT) };
            freelist.push(heap_ptr);
        }

        Self {
            freelist,
            page_size,
            global_batch_size,
            local_batch_size,
            local_min_pages,
            local_max_pages,
        }
    }

    pub fn default(page_size: usize, page_count: usize) -> Self {
        Self::new(
            page_size,
            page_count,
            DEFAULT_GLOBAL_BATCH_SIZE,
            DEFAULT_LOCAL_BATCH_SIZE,
            DEFAULT_LOCAL_MIN_PAGES,
            DEFAULT_LOCAL_MAX_PAGES,
        )
    }

    fn max_size(&self) -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            * self.local_max_pages
            * 2
    }

    pub fn len(&self) -> usize {
        self.freelist.len()
    }

    pub fn try_alloc(&self) -> Option<NonNull<u8>> {
        self.freelist.pop()
    }

    pub fn alloc_batch(&self, to_alloc: usize) -> Vec<NonNull<u8>> {
        let mut mem_vec = Vec::with_capacity(to_alloc);

        while mem_vec.len() < to_alloc {
            if let Some(mem) = self.freelist.pop() {
                mem_vec.push(mem);
            } else {
                break;
            }
        }

        if mem_vec.len() < to_alloc {
            for _ in 0..(to_alloc - mem_vec.len()) {
                mem_vec.push(unsafe { alloc_heap(self.page_size, BUFFER_ALIGNMENT) });
            }
            for _ in 0..self.global_batch_size {
                self.freelist
                    .push(unsafe { alloc_heap(self.page_size, BUFFER_ALIGNMENT) });
            }
        }

        mem_vec
    }

    /// Return what is possible to freelist and deallocate rest.
    pub fn dealloc_batch(&self, mut mem: Vec<NonNull<u8>>) {
        while self.len() < self.max_size() {
            if let Some(m) = mem.pop() {
                self.freelist.push(m);
            } else {
                return;
            }
        }
        while let Some(m) = mem.pop() {
            unsafe {
                dealloc_heap(m, self.page_size, BUFFER_ALIGNMENT);
            }
        }
    }

    pub fn dealloc(&self, mem: NonNull<u8>) {
        if self.len() + 1 > self.max_size() {
            unsafe { dealloc_heap(mem, self.page_size, BUFFER_ALIGNMENT) };
        } else {
            self.freelist.push(mem);
        }
    }
}

impl Drop for GlobalPageAllocator {
    fn drop(&mut self) {
        while let Some(mem) = self.freelist.pop() {
            unsafe { dealloc_heap(mem, self.page_size, BUFFER_ALIGNMENT) };
        }
    }
}

impl Debug for GlobalPageAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("GlobalPageAllocator {{ len: {} }}", self.len()))
    }
}

/// Local page allocator for fixed size memory pages. This struct shouldn't be shared accross threads.
pub struct LocalPageAllocator {
    /// Atomic reference to global allocator that will be used as fallback if local freelist is empty.
    pub(super) global_allocator: Arc<GlobalPageAllocator>,
    freelist: LocalStack<NonNull<u8>>,
}

impl LocalPageAllocator {
    pub fn new(global_allocator: Arc<GlobalPageAllocator>, page_count: usize) -> Self {
        let mut freelist = LocalStack::new();

        for _ in 0..page_count {
            let heap_ptr = unsafe { alloc_heap(global_allocator.page_size, BUFFER_ALIGNMENT) };
            freelist.push(heap_ptr);
        }

        Self {
            global_allocator,
            freelist,
        }
    }

    pub fn default(global_allocator: Arc<GlobalPageAllocator>) -> Self {
        Self::new(global_allocator, 0)
    }

    pub fn len(&self) -> usize {
        self.freelist.len()
    }

    pub fn alloc(&mut self) -> NonNull<u8> {
        // make batch allocation from global (refill local) when local allocator is almost empty.
        if self.freelist.len().saturating_sub(1) < self.global_allocator.local_min_pages {
            let batch_size = self.global_allocator.local_batch_size;

            let mut batch_alloc = self.global_allocator.alloc_batch(batch_size);

            for _ in 0..(batch_size - 1) {
                self.freelist.push(batch_alloc.pop().unwrap());
            }

            // last page is returned directly to avoid unnecessert.
            batch_alloc.pop().unwrap()
        } else {
            // by condition up we are guaranted to have spare page.
            self.freelist.pop().unwrap()
        }
    }

    pub fn dealloc(&mut self, mem: NonNull<u8>) {
        if self.freelist.len() + 1 > self.global_allocator.local_max_pages {
            self.global_allocator.dealloc(mem);
        } else {
            self.freelist.push(mem);
        }
    }

    pub fn drain(&mut self) -> Vec<NonNull<u8>> {
        let mut allocator_memory = Vec::with_capacity(self.freelist.len());
        while let Some(mem) = self.freelist.pop() {
            allocator_memory.push(mem);
        }
        allocator_memory
    }
}

impl Drop for LocalPageAllocator {
    fn drop(&mut self) {
        let mem = self.drain();
        self.global_allocator.dealloc_batch(mem);
    }
}

impl Debug for LocalPageAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "LocalPageAllocator {{ len: {} }}",
            self.freelist.len()
        ))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_main() {
        let global_allocator = Arc::new(GlobalPageAllocator::default(128, 0));

        println!("{:?}", global_allocator);

        let mut local_allocator = LocalPageAllocator::new(global_allocator.clone(), 0);

        println!("{:?}", local_allocator);

        local_allocator.alloc();

        println!("{:?}", local_allocator);

        drop(local_allocator);

        println!("{:?}", global_allocator);
    }
}
