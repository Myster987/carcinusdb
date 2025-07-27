use std::pin::Pin;

use parking_lot::Mutex;

use crate::utils::buffer::BufferData;

/// Holds free buffers as pointers to memory.
pub struct BufferPool {
    pub free_buffers: Mutex<Vec<BufferData>>,
    page_size: usize,
}

impl BufferPool {
    pub fn new(page_size: usize) -> Self {
        Self {
            free_buffers: Mutex::new(Vec::new()),
            page_size,
        }
    }

    pub fn len(&self) -> usize {
        self.free_buffers.lock().len()
    }

    /// Returns free pointer to buffer or allocates new one.
    pub fn get(&self) -> BufferData {
        let mut free_buffers = self.free_buffers.lock();
        if let Some(buffer) = free_buffers.pop() {
            buffer
        } else {
            Pin::new(vec![0; self.page_size])
        }
    }

    /// Adds new free buffer to pool.
    pub fn put(&self, buffer: BufferData) {
        let mut free_buffers = self.free_buffers.lock();
        free_buffers.push(buffer);
    }
}
