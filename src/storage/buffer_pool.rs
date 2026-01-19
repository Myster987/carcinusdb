use std::{ptr::NonNull, rc::Rc, sync::Arc};

use crossbeam::queue::SegQueue;

use crate::utils::buffer::{BUFFER_ALIGNMENT, Buffer, alloc_heap};

const DEFAULT_POOL_SIZE: usize = 2000;

/// Simple buffer pool implemented using [crossbeam::queue::SegQueue].
#[derive(Debug)]
pub struct BufferPool {
    /// Size of each buffer.
    page_size: usize,
    /// Total pool capacity.
    capacity: usize,
    /// FIFO queue of memory pointer to free buffers.
    pool: SegQueue<NonNull<u8>>,
}

impl BufferPool {
    pub fn default(page_size: usize) -> Self {
        Self::new(page_size, DEFAULT_POOL_SIZE)
    }

    /// Creates new pool initialized with `capacity` of free buffers. Total
    /// size of pool is = `page_size` * `capacity` bytes.
    pub fn new(page_size: usize, capacity: usize) -> Self {
        let pool = SegQueue::new();

        for _ in 0..capacity {
            let ptr = unsafe { alloc_heap(page_size, BUFFER_ALIGNMENT) };
            pool.push(ptr);
        }

        Self {
            page_size,
            capacity,
            pool,
        }
    }

    /// Returns free buffer. If pool contains free ones, they are simply poped
    /// and drop function will later push them back into pool. Otherwise it
    /// allocates new memory that will get deallocated as soon as buffer is dropped.
    pub fn get(self: &Arc<Self>) -> Buffer {
        if let Some(mem) = self.pool.pop() {
            let pool_ref = self.clone();
            let drop_fn = Rc::new(move |ptr| {
                pool_ref.pool.push(ptr);
            });

            Buffer::from_ptr(mem, self.page_size, Some(drop_fn))
        } else {
            // pool is currently empty alloc some temp pages that won't recycled
            let mem = unsafe { alloc_heap(self.page_size, BUFFER_ALIGNMENT) };
            Buffer::from_ptr(mem, self.page_size, None)
        }
    }
}
