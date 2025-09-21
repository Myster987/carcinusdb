use std::{ptr::NonNull, sync::Arc};

use parking_lot::Mutex;

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
        self.len -= 1;
        self.head.take().map(|node| {
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

const MIN_LOCAL: usize = 8;
const MAX_LOCAL: usize = 32;
const BATCH: usize = 16;

pub struct GlobalPageAllocator {
    freelist: Mutex<LocalStack<NonNull<u8>>>,
    page_size: usize,
}

impl GlobalPageAllocator {
    pub fn new(page_size: usize, page_count: usize) -> Self {
        let mut freelist = LocalStack::new();

        for _ in 0..page_count {
            let heap_ptr = unsafe { alloc_heap(page_size, BUFFER_ALIGNMENT) };
            freelist.push(heap_ptr);
        }

        Self {
            freelist: Mutex::new(freelist),
            page_size,
        }
    }

    pub fn default(page_size: usize) -> Self {
        Self::new(page_size, 0)
    }

    fn max_size() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            * MAX_LOCAL
            * 2
    }

    pub fn try_alloc(&self) -> Option<NonNull<u8>> {
        self.freelist.lock().pop()
    }

    pub fn dealloc(&self, mem: NonNull<u8>) {
        let mut freelist = self.freelist.lock();
        if freelist.len + 1 > Self::max_size() {
            unsafe { dealloc_heap(mem, self.page_size, BUFFER_ALIGNMENT) };
        } else {
            freelist.push(mem);
        }
    }

    pub fn drain(&self) {
        let mut freelist = self.freelist.lock();
        while let Some(mem) = freelist.pop() {
            unsafe { dealloc_heap(mem, self.page_size, BUFFER_ALIGNMENT) };
        }
    }
}

impl Drop for GlobalPageAllocator {
    fn drop(&mut self) {
        self.drain();
    }
}

/// Local page allocator for fixed size memory pages. This struct shouldn't be shared accross threads if not wrapped in mutex.
pub struct LocalPageAllocator {
    global_allocator: Arc<GlobalPageAllocator>,
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

    pub fn alloc(&mut self) -> NonNull<u8> {
        // try get from local
        if let Some(mem) = self.freelist.pop() {
            return mem;
        }

        // try get from global
        if let Some(mem) = self.global_allocator.try_alloc() {
            return mem;
        }
        // fallback to allocation
        unsafe { alloc_heap(self.global_allocator.page_size, BUFFER_ALIGNMENT) }
    }

    pub fn dealloc(&mut self, mem: NonNull<u8>) {
        self.freelist.push(mem);
    }

    pub fn drain(&mut self) {
        while let Some(mem) = self.freelist.pop() {
            unsafe { dealloc_heap(mem, self.global_allocator.page_size, BUFFER_ALIGNMENT) };
        }
    }
}

impl Drop for LocalPageAllocator {
    fn drop(&mut self) {
        self.drain();
    }
}

#[cfg(test)]
mod tests {
    
    use super::*;

    #[test]
    fn test_main() {
        println!("global allocator size: {:?}", GlobalPageAllocator::max_size());
    }

}