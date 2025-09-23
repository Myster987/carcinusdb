use std::sync::Arc;

use crate::{
    storage::allocator::{GlobalPageAllocator, LocalPageAllocator},
    utils::buffer::Buffer,
};

/// Global buffer pool that manages local pools and works as a fallback in case
/// when local pool runs out of memory. All the config is stored in allocator field
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

    pub fn default(page_size: usize) -> Self {
        Self {
            allocator: Arc::new(GlobalPageAllocator::default(page_size)),
        }
    }

    pub fn local_pool(&self, init_size: usize) -> LocalBufferPool {
        LocalBufferPool::new(self.allocator.clone(), init_size)
    }

    /// Returns total number of allocated buffers.
    pub fn len(&self) -> usize {
        self.allocator.len()
    }

    /// Returns free buffer in pool or allocates new one on heap. By default pool buffer drop function will return them back to pool.
    pub fn get(self: &std::sync::Arc<Self>) -> Buffer {
        // self.allocator.
        todo!()
    }

    /// Adds new free buffer to pool.
    pub fn put(&self, buffer_id: usize) {
        // self.pool.lock().dealloc_one(buffer_id);
        todo!()
    }
}

pub struct LocalBufferPool {
    allocator: LocalPageAllocator,
}

impl LocalBufferPool {
    pub fn new(global_allocator: Arc<GlobalPageAllocator>, init_size: usize) -> Self {
        Self {
            allocator: LocalPageAllocator::new(global_allocator, init_size),
        }
    }
}

#[cfg(test)]
mod tests {}
