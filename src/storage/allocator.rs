use std::{fmt::Debug, ptr::NonNull, sync::Arc};

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

const LOCAL_MIN_SIZE: usize = 8;
const LOCAL_MAX_SIZE: usize = 32;
const LOCAL_BATCH_SIZE: usize = 16;
const GLOBAL_BATCH_SIZE: usize = 32;

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
            * LOCAL_MAX_SIZE
            * 2
    }

    pub fn try_alloc(&self) -> Option<NonNull<u8>> {
        self.freelist.lock().pop()
    }

    pub fn alloc_batch(&self, to_alloc: usize) -> Vec<NonNull<u8>> {
        let mut mem_vec = Vec::with_capacity(to_alloc);
        let mut freelist = self.freelist.lock();

        while mem_vec.len() < to_alloc {
            if let Some(mem) = freelist.pop() {
                mem_vec.push(mem);
            } else {
                break;
            }
        }

        if mem_vec.len() < to_alloc {
            for _ in 0..(to_alloc - mem_vec.len()) {
                mem_vec.push(unsafe { alloc_heap(self.page_size, BUFFER_ALIGNMENT) });
            }
            for _ in 0..GLOBAL_BATCH_SIZE {
                freelist.push(unsafe { alloc_heap(self.page_size, BUFFER_ALIGNMENT) });
            }
        }

        mem_vec
    }

    /// Return what is possible to freelist and deallocate rest.
    pub fn dealloc_batch(&self, mut mem: Vec<NonNull<u8>>) {
        let mut freelist = self.freelist.lock();
        while freelist.len() < Self::max_size() {
            if let Some(m) = mem.pop() {
                freelist.push(m);
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
        let mut freelist = self.freelist.lock();
        if freelist.len + 1 > Self::max_size() {
            unsafe { dealloc_heap(mem, self.page_size, BUFFER_ALIGNMENT) };
        } else {
            freelist.push(mem);
        }
    }
}

impl Drop for GlobalPageAllocator {
    fn drop(&mut self) {
        let mut freelist = self.freelist.lock();
        while let Some(mem) = freelist.pop() {
            unsafe { dealloc_heap(mem, self.page_size, BUFFER_ALIGNMENT) };
        }
    }
}

impl Debug for GlobalPageAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "{} {{ len: {} }}",
            std::any::type_name::<Self>(),
            self.freelist.lock().len()
        ))
    }
}

/// Local page allocator for fixed size memory pages. This struct shouldn't be shared accross threads.
pub struct LocalPageAllocator {
    /// Atomic reference to global allocator that will be used as fallback if local freelist is empty.
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
        // make batch allocation from global (refill local) when local allocator is almost empty.
        if self.freelist.len().saturating_sub(1) < LOCAL_MIN_SIZE {
            let mut batch_alloc = self.global_allocator.alloc_batch(LOCAL_BATCH_SIZE);

            for _ in 0..(LOCAL_BATCH_SIZE - 1) {
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
        if self.freelist.len() + 1 > LOCAL_MAX_SIZE {
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
            "{} {{ len: {} }}",
            std::any::type_name::<Self>(),
            self.freelist.len()
        ))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_main() {
        let global_allocator = Arc::new(GlobalPageAllocator::new(128, 0));

        println!("{:?}", global_allocator);
        
        let mut local_allocator = LocalPageAllocator::new(global_allocator.clone(), 0);

        println!("{:?}", local_allocator);
        
        local_allocator.alloc();
        
        println!("{:?}", local_allocator);
        
        drop(local_allocator);
        
        println!("{:?}", global_allocator);
    }
}
