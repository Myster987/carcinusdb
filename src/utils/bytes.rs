use bytes::{Buf, BytesMut};

use crate::{
    error::{DatabaseError, DatabaseResult},
    pager::DEFAULT_PAGE_SIZE,
};

pub struct Buffer {
    pub content: BytesMut,
}

impl Buffer {
    pub fn alloc(size: usize) -> Self {
        let mut buffer = BytesMut::with_capacity(size);
        buffer.resize(size, 0);

        Self { content: buffer }
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Buffer::alloc(DEFAULT_PAGE_SIZE)
    }
}

pub fn get_u8(src: &mut impl Buf) -> DatabaseResult<u8> {
    if !src.has_remaining() {
        return Err(DatabaseError::InvalidBytes);
    }
    Ok(src.get_u8())
}

pub fn get_bool(src: &mut impl Buf) -> DatabaseResult<bool> {
    Ok(get_u8(src)? == 0)
}

pub fn get_u16(src: &mut impl Buf) -> DatabaseResult<u16> {
    if !src.has_remaining() {
        return Err(DatabaseError::InvalidBytes);
    }
    Ok(src.get_u16())
}

pub fn get_u32(src: &mut impl Buf) -> DatabaseResult<u32> {
    if !src.has_remaining() {
        return Err(DatabaseError::InvalidBytes);
    }
    Ok(src.get_u32())
}
