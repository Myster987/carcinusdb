//! Type casting utilis. Inspired by [`bytemuck`](https://docs.rs/bytemuck) create

use super::{Error, Result};

macro_rules! transmute {
    ($val:expr) => {{ std::mem::transmute_copy(&std::mem::ManuallyDrop::new($val)) }};
}

/// Checks if `ptr` is aligned to `align` value.
pub fn is_aligned_to(ptr: *const (), align: usize) -> bool {
    ptr.align_offset(align) == 0
}

/// Casts type `A` to `B`. \
///
/// # Requirements:
///
/// * `size_of::<A>() == size_of::<B>`
///
pub fn cast<A: Copy, B: Copy>(a: A) -> B {
    unsafe { try_cast(a).unwrap() }
}

/// Converts slice `&[A]` of type `A` to slice `&[B]` of type `B`. \
///
/// # Panics:
/// * if value is `Err` with message provided by `Err`.
///
pub fn cast_slice<A, B>(a: &[A]) -> &[B] {
    unsafe { try_cast_slice(a).unwrap() }
}

/// Converts slice `&mut [A]` of type `A` to slice `&mut [B]` of type `B`. \
///
/// # Panics:
/// * if value is `Err` with message provided by `Err`.
///
pub fn cast_slice_mut<A, B>(a: &mut [A]) -> &mut [B] {
    unsafe { try_cast_slice_mut(a).unwrap() }
}

/// Converts slice of bytes to `&T`. \
///
/// # Fails:
/// * `src.len() != size_of::<T>()`
/// * `type T is not aligned to src`
///
pub fn from_bytes<T: Copy>(src: &[u8]) -> &T {
    unsafe { try_from_bytes(src).unwrap() }
}

/// Converts slice of bytes to `&mut T`. \
///
/// # Fails:
/// * `src.len() != size_of::<T>()`
/// * `type T is not aligned to src`
///
pub fn from_bytes_mut<T: Copy>(src: &mut [u8]) -> &mut T {
    unsafe { try_from_bytes_mut(src).unwrap() }
}

/// Converts `&T` into slice of bytes. \
pub fn bytes_of<T>(src: &T) -> &[u8] {
    unsafe { try_bytes_of(src).unwrap() }
}

/// Converts `&mut T` into mutable slice of bytes. \
pub fn bytes_of_mut<T>(src: &mut T) -> &mut [u8] {
    unsafe { try_bytes_of_mut(src).unwrap() }
}

/// Casts type `A` to `B`. Returns `Result<B>` \
///
/// # Requirements:
/// * `size_of::<A>() == size_of::<B>`
///
pub unsafe fn try_cast<A: Copy, B: Copy>(a: A) -> Result<B> {
    if size_of::<A>() == size_of::<B>() {
        Ok(unsafe { transmute!(a) })
    } else {
        Err(Error::SizeMismatch)
    }
}

/// Converts slice `&[A]` of type `A` to slice `&[B]` of type `B` (output length can change). Returns `Result<B>`. \
///
/// # Requirements:
/// * `input.as_ptr() as usize == output.as_ptr() as usize`
/// * `input.len() * size_of::<A>() == output.len() * size_of::<B>()`
///
/// When casting to struct you mustn't use padding inside struct or it will fail.
pub unsafe fn try_cast_slice<A, B>(a: &[A]) -> Result<&[B]> {
    let input_bytes = size_of_val(a);

    if align_of::<B>() > align_of::<A>() && !is_aligned_to(a.as_ptr() as *const (), align_of::<B>())
    {
        Err(Error::InvalidAligment)
    } else if size_of::<A>() == size_of::<B>() {
        Ok(unsafe { std::slice::from_raw_parts(a.as_ptr() as *const B, a.len()) })
    } else if (size_of::<B>() != 0 && input_bytes % size_of::<B>() == 0)
        || (size_of::<B>() == 0 && input_bytes == 0)
    {
        let new_len = if size_of::<B>() != 0 {
            input_bytes / size_of::<B>()
        } else {
            0
        };
        Ok(unsafe { std::slice::from_raw_parts(a.as_ptr() as *const B, new_len) })
    } else {
        Err(Error::Unknown)
    }
}

/// The same as `try_cast_slice`, but with mutable slices (output length can change)
pub unsafe fn try_cast_slice_mut<A, B>(a: &mut [A]) -> Result<&mut [B]> {
    let input_bytes = size_of_val(a);

    if align_of::<B>() > align_of::<A>() && !is_aligned_to(a.as_ptr() as *const (), align_of::<B>())
    {
        Err(Error::InvalidAligment)
    } else if size_of::<A>() == size_of::<B>() {
        Ok(unsafe { std::slice::from_raw_parts_mut(a.as_mut_ptr() as *mut B, a.len()) })
    } else if (size_of::<B>() != 0 && input_bytes % size_of::<B>() == 0)
        || (size_of::<B>() == 0 && input_bytes == 0)
    {
        let new_len = if size_of::<B>() != 0 {
            input_bytes / size_of::<B>()
        } else {
            0
        };
        Ok(unsafe { std::slice::from_raw_parts_mut(a.as_mut_ptr() as *mut B, new_len) })
    } else {
        Err(Error::Unknown)
    }
}

/// Converts slice of bytes to `&T`. Returns `Result<&T>`. \
///
/// # Fails:
/// * `src.len() != size_of::<T>()`
/// * `type T is not aligned to src`
///
pub unsafe fn try_from_bytes<T: Copy>(src: &[u8]) -> Result<&T> {
    if src.len() != size_of::<T>() {
        Err(Error::SizeMismatch)
    } else if !is_aligned_to(src.as_ptr() as *const (), align_of::<T>()) {
        Err(Error::InvalidAligment)
    } else {
        Ok(unsafe { &*(src.as_ptr() as *const T) })
    }
}

/// Converts slice of bytes to `&mut T`. Returns `Result<&mut T>`. \
///
/// # Fails:
/// * `src.len() != size_of::<T>()`
/// * `type T is not aligned to src`
///
pub unsafe fn try_from_bytes_mut<T: Copy>(src: &mut [u8]) -> Result<&mut T> {
    if src.len() != size_of::<T>() {
        Err(Error::SizeMismatch)
    } else if !is_aligned_to(src.as_ptr() as *const (), align_of::<T>()) {
        Err(Error::InvalidAligment)
    } else {
        Ok(unsafe { &mut *(src.as_ptr() as *mut T) })
    }
}

/// Converts type `&T` into slice of bytes. Returns `Result<&[u8]>`.
pub unsafe fn try_bytes_of<T>(src: &T) -> Result<&[u8]> {
    unsafe { try_cast_slice::<T, u8>(std::slice::from_ref(src)) }
}

/// Converts type `&mut T` into mutable slice of bytes. Returns `Result<&mut [u8]>`.
pub unsafe fn try_bytes_of_mut<T>(src: &mut T) -> Result<&mut [u8]> {
    unsafe { try_cast_slice_mut::<T, u8>(std::slice::from_mut(src)) }
}

/// Converts given slice to 'static lifetime.
pub unsafe fn cast_static<T>(src: &[T]) -> &'static [T] {
    unsafe { core::mem::transmute(src) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestStruct {
        a: i16,
        b: u32,
        c: bool,
    }

    #[test]
    fn test_cast_slice() -> anyhow::Result<()> {
        let a = [1u8, 1, 0, 0];

        let b: &[u16] = unsafe { try_cast_slice(&a)? };
        let c: &[u32] = cast_slice(&a);

        assert_eq!(b, [257, 0]);
        assert_eq!(c, [257]);

        Ok(())
    }

    #[test]
    fn test_bytes_of() -> anyhow::Result<()> {
        let src = TestStruct {
            a: 1,
            b: 2,
            c: false,
        };
        let valid_result = [2, 0, 0, 0, 1, 0, 0, 0];

        let try_bytes_of_test = unsafe { try_bytes_of(&src)? };
        let bytes_of_test = bytes_of(&src);

        assert_eq!(valid_result, try_bytes_of_test);
        assert_eq!(valid_result, bytes_of_test);

        Ok(())
    }

    #[test]
    fn test_from_bytes() -> anyhow::Result<()> {
        let valid_result = TestStruct {
            a: 1,
            b: 2,
            c: false,
        };
        let src = [2, 0, 0, 0, 1, 0, 0, 0];

        let try_from_bytes_test: TestStruct = unsafe { *try_from_bytes(&src)? };
        let from_bytes_test: TestStruct = *from_bytes(&src);

        assert_eq!(valid_result, try_from_bytes_test);
        assert_eq!(valid_result, from_bytes_test);

        Ok(())
    }
}
