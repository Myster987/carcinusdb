use std::{cell::RefCell, ptr::NonNull, rc::Rc, sync::Arc};

use crate::{
    storage::allocator::{GlobalPageAllocator, LocalPageAllocator},
    utils::buffer::Buffer,
};

/// Global buffer pool that manages local pools and acts as a fallback in case
/// when local pool runs out of memory. All the config is stored in allocator field.
/// This struct shouldn't be used to allocate memory, because it only works as a manager.
#[derive(Debug)]
pub struct GlobalBufferPool {
    allocator: Arc<GlobalPageAllocator>,
}

impl GlobalBufferPool {
    /// Creates new global buffer pool.
    /// ## Params:
    /// - page_size: size of invidual page in bytes
    /// - init_size: initial size in **pages** of pool to pre-alloc
    /// - global_batch_size: number in **pages** to alloc when global pool runs out of memory
    /// - local_batch_size: number in **pages** to alloc when local pool runs out of memory
    /// - local_min_pages: number in **pages** that is allowed to not couse batch alloc
    /// - local_max_pages: number in **pages** that is allowed to not couse batch dealloc
    pub fn new(
        page_size: usize,
        init_size: usize,
        global_batch_size: usize,
        local_batch_size: usize,
        local_min_pages: usize,
        local_max_pages: usize,
    ) -> Self {
        Self {
            allocator: Arc::new(GlobalPageAllocator::new(
                page_size,
                init_size,
                global_batch_size,
                local_batch_size,
                local_min_pages,
                local_max_pages,
            )),
        }
    }

    pub fn default(page_size: usize, init_size: usize) -> Self {
        Self {
            allocator: Arc::new(GlobalPageAllocator::default(page_size, init_size)),
        }
    }

    pub fn local_pool(&self, init_size: usize) -> LocalBufferPool {
        LocalBufferPool::new(self.allocator.clone(), init_size)
    }

    /// Returns total number of allocated buffers.
    pub fn len(&self) -> usize {
        self.allocator.len()
    }
}

#[derive(Debug)]
pub struct LocalBufferPool {
    allocator: Rc<RefCell<LocalPageAllocator>>,
}

impl LocalBufferPool {
    pub fn new(global_allocator: Arc<GlobalPageAllocator>, init_size: usize) -> Self {
        Self {
            allocator: Rc::new(RefCell::new(LocalPageAllocator::new(
                global_allocator,
                init_size,
            ))),
        }
    }

    pub fn len(&self) -> usize {
        self.allocator.borrow().len()
    }

    /// Returns free buffer or allocates new one on heap. By default pool buffer drop function will return them back to pool.
    pub fn get(&self) -> Buffer {
        let (mem, page_size) = {
            let mut allocator = self.allocator.borrow_mut();

            (allocator.alloc(), allocator.global_allocator.page_size)
        };
        let pool = self.allocator.clone();

        let drop_fn = Rc::new(move |ptr| {
            pool.borrow_mut().dealloc(ptr);
        });

        Buffer::from_ptr(mem, page_size, Some(drop_fn))
    }

    fn put(&mut self, mem: NonNull<u8>) {
        self.allocator.borrow_mut().dealloc(mem);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_main() -> anyhow::Result<()> {
        let global_pool = GlobalBufferPool::default(128, 0);

        let local_pool = global_pool.local_pool(1);
        let buffer = local_pool.get();

        println!("buffer: {:?}", buffer);

        println!("len: {}", local_pool.len());

        println!("global len: {}", global_pool.len());
        drop(buffer);
        drop(local_pool);
        println!("global len: {}", global_pool.len());

        Ok(())
    }
}
