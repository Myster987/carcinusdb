use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    fmt::Debug,
    ptr::NonNull,
    rc::Rc,
};

use crate::storage::page::{MAX_PAGE_SIZE, MIN_PAGE_SIZE};

pub type BufferData = NonNull<u8>;
pub type DropFn = Rc<dyn Fn(BufferData)>;

pub const BUFFER_ALIGNMENT: usize = align_of::<u8>();

pub struct Buffer {
    size: usize,
    ptr: BufferData,
    drop: Option<DropFn>,
}

impl Buffer {
    pub fn alloc(size: usize, drop: Option<DropFn>) -> Self {
        let ptr = unsafe { alloc_heap(size, BUFFER_ALIGNMENT) };
        Self { size, ptr, drop }
    }

    /// Allocates buffer on heap
    pub fn alloc_page(size: usize, drop: Option<DropFn>) -> Self {
        assert!(
            (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&size),
            "\"Page\" of size {size} is not between [{}; {}] range",
            MIN_PAGE_SIZE,
            MAX_PAGE_SIZE
        );
        Self::alloc(size, drop)
    }

    pub fn from_ptr(ptr: NonNull<u8>, size: usize, drop: Option<DropFn>) -> Self {
        Self { size, ptr, drop }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    // pub fn is_empty(&self) -> bool {
    //     self.data.is_empty()
    // }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr().cast_const()
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub fn as_non_null(&self) -> NonNull<u8> {
        self.ptr.clone()
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size) }
    }

    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.size) }
    }

    pub fn reset(&mut self) {
        unsafe { std::ptr::write_bytes(self.ptr.as_ptr(), 0, self.size()) };
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

impl<Idx> std::ops::Index<Idx> for Buffer
where
    Idx: std::slice::SliceIndex<[u8]>,
{
    type Output = Idx::Output;

    fn index(&self, index: Idx) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<Idx> std::ops::IndexMut<Idx> for Buffer
where
    Idx: std::slice::SliceIndex<[u8]>,
{
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        &mut self.as_mut_slice()[index]
    }
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Buffer {{ {:?} }}", self.as_slice())
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Some(f) = &self.drop {
            f(self.ptr)
        } else {
            unsafe {
                dealloc_heap(self.ptr, self.size, BUFFER_ALIGNMENT);
            }
        }
    }
}

pub unsafe fn alloc_heap(size: usize, align: usize) -> NonNull<u8> {
    let layout = Layout::from_size_align(size, align).unwrap();
    unsafe { NonNull::new(alloc_zeroed(layout)).unwrap() }
}

pub unsafe fn dealloc_heap(ptr: NonNull<u8>, size: usize, align: usize) {
    let layout = Layout::from_size_align(size, align).unwrap();
    unsafe { dealloc(ptr.as_ptr(), layout) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reset() -> anyhow::Result<()> {
        let mut buffer = Buffer::alloc(20, None);

        buffer[..6].copy_from_slice(b"Maciek");

        println!("{:?}", buffer);
        buffer.reset();
        println!("{:?}", buffer);

        assert!(buffer.as_ref() == vec![0; 20]);

        Ok(())
    }

    #[test]
    fn test_drop_fn() -> anyhow::Result<()> {
        // let pool = Rc::new(RefCell::new(vec![]));

        // assert!(pool.borrow().len() == 0);

        // let buffer = Buffer::alloc(
        //     20,
        //     Some(Rc::new(|ptr| {
        //         let pool_cp = pool.clone();
        //         pool_cp.borrow_mut().push(ptr);
        //     })),
        // );

        // drop(buffer);

        // assert!(pool.borrow().len() == 1);

        Ok(())
    }
}
