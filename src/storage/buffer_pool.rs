
use std::{ptr::NonNull, sync::Arc};

use parking_lot::Mutex;

use crate::utils::buffer::Buffer;

pub type SharedBufferPool = Arc<BufferPool>;

/// Holds free buffers as pointers to memory
pub struct BufferPool {
    pub free_buffers: Mutex<Vec<NonNull<[u8]>>>,
    page_size: usize,
}

impl BufferPool {
    pub fn new(page_size: usize) -> Self {
        Self {
            free_buffers: Mutex::new(Vec::new()),
            page_size,
        }
    }

    /// Returns free pointer to buffer or allocates new one
    pub fn get(&self) -> NonNull<[u8]> {
        let mut free_buffers = self.free_buffers.lock();
        if let Some(buffer) = free_buffers.pop() {
            buffer
        } else {
            Buffer::<u8>::alloc_page(self.page_size).into_non_null()
        }
    }

    /// Adds new free buffer to pool
    pub fn put(&self, buffer: NonNull<[u8]>) {
        let mut free_buffers = self.free_buffers.lock();
        free_buffers.push(buffer);
    }
}
