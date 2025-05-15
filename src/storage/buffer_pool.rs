use std::cell::RefCell;

use crate::utils::buffer::Buffer;

/// Holds free buffers
pub struct BufferPool {
    pub free_buffers: RefCell<Vec<Buffer<u8>>>,
    page_size: usize,
}

impl BufferPool {
    pub fn new(page_size: usize) -> Self {
        Self {
            free_buffers: RefCell::new(Vec::new()),
            page_size,
        }
    }

    /// Returns free buffer or allocates new one
    pub fn get(&self) -> Buffer<u8> {
        let mut free_buffers = self.free_buffers.borrow_mut();
        if let Some(buffer) = free_buffers.pop() {
            buffer
        } else {
            Buffer::alloc_page(self.page_size)
        }
    }

    /// Adds new free buffer to pool
    pub fn put(&self, buffer: Buffer<u8>) {
        let mut free_buffers = self.free_buffers.borrow_mut();
        free_buffers.push(buffer);
    }
}
