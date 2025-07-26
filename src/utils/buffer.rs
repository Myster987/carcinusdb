use std::{fmt::Debug, mem::ManuallyDrop, pin::Pin, rc::Rc};

use crate::storage::page::{MAX_PAGE_SIZE, MIN_PAGE_SIZE};

pub type BufferData = Pin<Vec<u8>>;
pub type BufferDropFn = Rc<dyn Fn(BufferData)>;

pub struct Buffer {
    data: ManuallyDrop<BufferData>,
    drop: Option<BufferDropFn>,
}

impl Buffer {
    pub fn alloc(size: usize, drop: Option<BufferDropFn>) -> Self {
        let data = ManuallyDrop::new(Pin::new(vec![0; size]));
        Self { data, drop }
    }

    pub fn alloc_page(size: usize, drop: Option<BufferDropFn>) -> Self {
        assert!(
            (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&size),
            "\"Page\" of size {size} is not between [{}; {}] range",
            MIN_PAGE_SIZE,
            MAX_PAGE_SIZE
        );
        Self::alloc(size, drop)
    }

    pub fn new(data: BufferData, drop: Option<BufferDropFn>) -> Self {
        Self {
            data: ManuallyDrop::new(data),
            drop,
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn reset(&mut self) {
        unsafe { std::ptr::write_bytes(self.data.as_mut_ptr(), 0, self.size()) };
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for Buffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let data = unsafe { ManuallyDrop::take(&mut self.data) };
        if let Some(f) = &self.drop {
            f(data)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    #[test]
    fn test_reset() -> anyhow::Result<()> {
        let mut buffer = Buffer::alloc(20, None);

        buffer.data[..6].copy_from_slice(b"Maciek");

        buffer.reset();

        assert!(buffer.as_ref() == vec![0; 20]);

        Ok(())
    }

    #[test]
    fn test_drop_fn() -> anyhow::Result<()> {
        let pool = Rc::new(RefCell::new(vec![]));

        assert!(pool.borrow().len() == 0);

        let buffer = Buffer::alloc(
            20,
            Some(Rc::new(|ptr| {
                let pool_cp = pool.clone();
                pool_cp.borrow_mut().push(ptr);
            })),
        );

        drop(buffer);

        assert!(pool.borrow().len() == 1);

        Ok(())
    }
}
