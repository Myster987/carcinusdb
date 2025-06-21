use std::{alloc::Layout, cell::RefCell, ptr::NonNull};

use crate::{os::DISK_BLOCK_SIZE, utils::buffer::Buffer};

/// Holds free buffers as pointers to memory
pub struct BufferPool {
    pub free_buffers: RefCell<Vec<NonNull<[u8]>>>,
    page_size: usize,
}

impl BufferPool {
    pub fn new(page_size: usize) -> Self {
        Self {
            free_buffers: RefCell::new(Vec::new()),
            page_size,
        }
    }

    pub fn len(&self) -> usize {
        self.free_buffers.borrow().len()
    }

    /// Returns free pointer to buffer or allocates new one
    pub fn get(&self) -> NonNull<[u8]> {
        let mut free_buffers = self.free_buffers.borrow_mut();
        if let Some(buffer) = free_buffers.pop() {
            buffer
        } else {
            let layout = Layout::from_size_align(self.page_size, *DISK_BLOCK_SIZE)
                .expect("Invalid buffer size or alignment");
            Buffer::<u8>::alloc(layout)
        }
    }

    /// Adds new free buffer to pool
    pub fn put(&self, buffer: NonNull<[u8]>) {
        let mut free_buffers = self.free_buffers.borrow_mut();
        free_buffers.push(buffer);
    }
}
