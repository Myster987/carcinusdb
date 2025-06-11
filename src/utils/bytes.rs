use bytes::Buf;

use super::{Error, Result};

pub fn get_u8(src: &mut impl Buf) -> Result<u8> {
    if !src.has_remaining() {
        return Err(Error::InvalidBytes);
    }
    Ok(src.get_u8())
}

pub fn get_bool(src: &mut impl Buf) -> Result<bool> {
    Ok(get_u8(src)? == 0)
}

pub fn get_u16(src: &mut impl Buf) -> Result<u16> {
    if !src.has_remaining() {
        return Err(Error::InvalidBytes);
    }
    Ok(src.get_u16())
}

pub fn get_u32(src: &mut impl Buf) -> Result<u32> {
    if !src.has_remaining() {
        return Err(Error::InvalidBytes);
    }
    Ok(src.get_u32())
}

pub fn get_u64(src: &mut impl Buf) -> Result<u64> {
    if !src.has_remaining() {
        return Err(Error::InvalidBytes);
    }
    Ok(src.get_u64())
}
