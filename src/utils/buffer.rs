use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    fmt::Debug,
    mem::ManuallyDrop,
    ptr::NonNull,
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{
    os::DISK_BLOCK_SIZE,
    storage::page::{CELL_ALIGNMENT, MAX_PAGE_SIZE, MIN_PAGE_SIZE},
};

use super::{Error, Result, cast::is_aligned_to};

// pub struct SharedBuffer<H> {
//     pub buffer: Arc<RwLock<Buffer<H>>>,
// }

// impl<H> SharedBuffer<H> {
//     pub fn new(buffer: Buffer<H>) -> Self {
//         Self {
//             buffer: Arc::new(RwLock::new(buffer)),
//         }
//     }

//     pub fn alloc_page(size: usize) -> Self {
//         Self::new(Buffer::alloc_page(size))
//     }

//     pub fn lock_shared(&self) -> parking_lot::RwLockReadGuard<Buffer<H>> {
//         self.buffer.read()
//     }

//     pub fn lock_exclusive(&self) -> parking_lot::RwLockWriteGuard<Buffer<H>> {
//         self.buffer.write()
//     }
// }

pub type SharedBuffer<H> = Arc<RwLock<Buffer<H>>>;

/// Buffer with header that is building blocks of all pages.
pub struct Buffer<H> {
    /// Pointer to header at the beginning of buffer
    header: NonNull<H>,
    /// Pointer to content right after the header
    pub content: NonNull<[u8]>,
    /// Total size of buffer (size of header + size of content)
    pub size: usize,
}

impl<H> Buffer<H> {
    /// Allocates a pointer for buffer with given `layout`. Note that layout size = `header` + `content`. \
    /// # Fails:
    /// * `layout.size() <= size_of::<H>()` - buffer needs to fit header and some content.
    pub fn try_alloc(layout: Layout) -> Result<NonNull<[u8]>> {
        if layout.size() <= size_of::<H>() {
            return Err(Error::InvalidAllocation(format!(
                "Allocating {} bytes is incorrect. You need at least {} to fit header ({} bytes)",
                layout.size(),
                size_of::<H>() + 1,
                size_of::<H>()
            )));
        }

        let ptr = unsafe { alloc_zeroed(layout) };

        Ok(unsafe { NonNull::new(std::slice::from_raw_parts_mut(ptr, layout.size())).unwrap() })
    }

    /// Same as `try_alloc`.
    pub fn alloc(layout: Layout) -> NonNull<[u8]> {
        Self::try_alloc(layout).unwrap()
    }

    /// Allocates buffer if it's size fits in MIN and MAX `Page` size.
    /// # Panics
    /// - If size is **less or more** than `Page` size should be.
    pub fn alloc_page(size: usize) -> Self {
        assert!(
            (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&size),
            "Page of size {size} is not between {MIN_PAGE_SIZE} AND {MAX_PAGE_SIZE}"
        );
        Self::new(size, *DISK_BLOCK_SIZE)
    }

    pub fn new(size: usize, align: usize) -> Self {
        let layout =
            Layout::from_size_align(size, align).expect("Invalid buffer size or alignment");
        Self::from_non_null(Self::alloc(layout))
    }

    /// Creates `Buffer` from `NonNull<[u8]>` pointer. \
    ///
    /// # Safety
    ///
    /// - `pointer` must be aligned to at least `CELL_ALIGNMENT`. Required by BTree.
    pub unsafe fn try_from_non_null(pointer: NonNull<[u8]>) -> Result<Self> {
        if pointer.len() <= size_of::<H>() {
            return Err(Error::InvalidAllocation(format!(
                "Allocating {} with {} bytes is incorrect. You need at least {} to fit header ({} bytes)",
                std::any::type_name::<Self>(),
                pointer.len(),
                size_of::<H>() + 1,
                size_of::<H>()
            )));
        }

        if !is_aligned_to(pointer.as_ptr() as *const (), CELL_ALIGNMENT) {
            return Err(Error::InvalidAligment);
        }

        let content = NonNull::slice_from_raw_parts(
            unsafe { pointer.byte_add(size_of::<H>()).cast::<u8>() },
            Self::usable_space(pointer.len()) as usize,
        );

        Ok(Self {
            header: pointer.cast(),
            content,
            size: pointer.len(),
        })
    }

    /// Converts pointer to Buffer.
    ///
    /// # Safety
    ///
    /// See `Buffer::try_from_non_null`.
    pub fn from_non_null(pointer: NonNull<[u8]>) -> Self {
        unsafe { Self::try_from_non_null(pointer).unwrap() }
    }

    /// Converts `Buffer<H>` into `Buffer<T>` and doesn't drop owned memory. \
    /// # Fails:
    /// * `size <= size_of::<H>()` - buffer needs to fit header and some content.
    pub fn cast<T>(self) -> Buffer<T> {
        let Self {
            header,
            content: _,
            size,
        } = self;

        assert!(
            size <= size_of::<T>(),
            "Allocating {} with {} bytes is incorrect. You need at least {} to fit header ({} bytes)",
            std::any::type_name::<Self>(),
            size,
            size_of::<H>() + 1,
            size_of::<H>()
        );

        std::mem::forget(self);

        let header = header.cast();

        let content = unsafe {
            NonNull::slice_from_raw_parts(
                header.byte_add(size_of::<T>()).cast(),
                Buffer::<T>::usable_space(size) as usize,
            )
        };

        Buffer {
            header,
            content,
            size,
        }
    }

    /// Calculates usable space in `Buffer` without header.
    pub fn usable_space(size: usize) -> u16 {
        (size - size_of::<H>()) as u16
    }

    /// Returns reference to header.
    pub fn header(&self) -> &H {
        unsafe { self.header.as_ref() }
    }

    /// Returns mutable reference to header.
    pub fn header_mut(&mut self) -> &mut H {
        unsafe { self.header.as_mut() }
    }

    /// Returns slice to `Buffer`'s content.
    pub fn content(&self) -> &[u8] {
        unsafe { self.content.as_ref() }
    }

    /// Returns mutable slice to `Buffer`'s content.
    pub fn content_mut(&mut self) -> &mut [u8] {
        unsafe { self.content.as_mut() }
    }

    /// Returns a [`NonNull`] pointer to the entire buffer memory.
    pub fn as_non_null(&self) -> NonNull<[u8]> {
        NonNull::slice_from_raw_parts(self.header.cast::<u8>(), self.size)
    }

    /// Converts `Buffer` to [`NonNull`] pointer.
    pub fn into_non_null(self) -> NonNull<[u8]> {
        ManuallyDrop::new(self).as_non_null()
    }

    /// Returns byte slice to entire buffer memory.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { self.as_non_null().as_ref() }
    }

    /// Returns mutable byte slice to entire byffer memory.
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { self.as_non_null().as_mut() }
    }

    pub fn reset(&mut self) {
        unsafe { std::ptr::write_bytes(self.header.as_ptr(), 0, self.size) };
    }
}

impl<H> AsRef<[u8]> for Buffer<H> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<H> AsMut<[u8]> for Buffer<H> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl<H> Drop for Buffer<H> {
    /// If `Buffer` is owner, then it deallocates memory, otherwise heap memory is still valid.
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.header.cast().as_ptr(),
                Layout::from_size_align(self.size, *DISK_BLOCK_SIZE).unwrap(),
            );
        }
    }
}

impl<H: Debug> Debug for Buffer<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("size", &self.size)
            .field("header", self.header())
            .field("content", &self.content())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() -> anyhow::Result<()> {
        let mut buffer: Buffer<u8> = Buffer::alloc_page(512);
        
        println!("{:?}", buffer);

        *buffer.header_mut() += 7;
        buffer.content_mut()[..6].copy_from_slice(b"Maciek");

        println!("{:?}", buffer);
        
        buffer.reset();

        println!("{:?}", buffer);
        Ok(())
    }
}
