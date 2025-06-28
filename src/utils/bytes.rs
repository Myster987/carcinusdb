use bytes::Buf;

use super::{Error, Result};

/// Takes `src` that implements [Buf] and advances current position by 1. Returns u8.
pub fn get_u8(src: &mut impl Buf) -> Result<u8> {
    if !src.has_remaining() {
        return Err(Error::InvalidBytes);
    }
    Ok(src.get_u8())
}

/// Takes `src` that implements [Buf] and advences current position by 1. Returns bool.
pub fn get_bool(src: &mut impl Buf) -> Result<bool> {
    Ok(get_u8(src)? == 0)
}

/// Takes `src` that implements [Buf] and advances current position by 2. Returns u16. Uses little endian.
pub fn get_u16(src: &mut impl Buf) -> Result<u16> {
    if !src.has_remaining() {
        return Err(Error::InvalidBytes);
    }
    Ok(src.get_u16_le())
}

/// Takes `src` that implements [Buf] and advances current position by 4. Returns u32. Uses little endian.
pub fn get_u32(src: &mut impl Buf) -> Result<u32> {
    if !src.has_remaining() {
        return Err(Error::InvalidBytes);
    }
    Ok(src.get_u32_le())
}

/// Takes `src` that implements [Buf] and advances current position by 8. Returns u64. Uses little endian.
pub fn get_u64(src: &mut impl Buf) -> Result<u64> {
    if !src.has_remaining() {
        return Err(Error::InvalidBytes);
    }
    Ok(src.get_u64_le())
}

/// Takes `src` that implements [Buf] and advances position by 1-9, depending on size of varint. Returns u64 as varint.
pub fn read_varint(src: &mut impl Buf) -> u64 {
    let mut v: u64 = 0;
    for _ in 0..8 {
        match src.try_get_u8().ok() {
            Some(c) => {
                v = (v << 7) + (c & 0x7f) as u64;
                if (c & 0x80) == 0 {
                    return v;
                }
            }
            None => {}
        }
    }
    v = (v << 8) + src.get_u8() as u64;
    v
}

/// Writes varint to beginning of a buffer. Takes 1-9 bytes. Returns how many bytes were written.
pub fn write_varint(buf: &mut [u8], value: u64) -> usize {
    if value <= 0x7f {
        buf[0] = (value & 0x7f) as u8;
        return 1;
    }

    if value <= 0x3fff {
        buf[0] = (((value >> 7) & 0x7f) | 0x80) as u8;
        buf[1] = (value & 0x7f) as u8;
        return 2;
    }

    let mut value = value;
    if (value & ((0xff000000_u64) << 32)) > 0 {
        buf[8] = value as u8;
        value >>= 8;
        for i in (0..8).rev() {
            buf[i] = ((value & 0x7f) | 0x80) as u8;
            value >>= 7;
        }
        return 9;
    }

    let mut encoded: [u8; 10] = [0; 10];
    let mut bytes = value;
    let mut n = 0;
    while bytes != 0 {
        let v = 0x80 | (bytes & 0x7f);
        encoded[n] = v as u8;
        bytes >>= 7;
        n += 1;
    }
    encoded[0] &= 0x7f;
    for i in 0..n {
        buf[i] = encoded[n - 1 - i];
    }
    n
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn test_varint() -> anyhow::Result<()> {
        let num = 123_456_789;
        let mut buf = vec![0; 20];

        write_varint(&mut buf, num);

        assert!(num == read_varint(&mut buf.as_slice()));

        Ok(())
    }
}
