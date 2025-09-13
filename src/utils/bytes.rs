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

pub fn zigzag_encode(value: i64) -> u64 {
    (value >> 63) as u64 ^ (value << 1) as u64
}

pub fn zigzag_decode(value: u64) -> i64 {
    (value >> 1) as i64 ^ -((value & 1) as i64)
}

/// Standard implementation of reflected CRC32.
pub fn checksum_crc32(bytes: &[u8]) -> u32 {
    const POLY: u32 = 0xEDB88320; // reflected 0x04C11DB7
    let mut crc: u32 = 0xFFFFFFFF;

    for &byte in bytes {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
        }
    }

    !crc // final NOT
}

/// Flips `n` bits in number to 1 and takes vector of positions with offset so that we get precise offset in bits.
/// # Safety
/// This function assumes that there are `n` 0 bits in number.
pub fn flip_n_bits(value: &mut u64, mut n: usize, positions: &mut Vec<usize>, offset: usize) {
    let mut i = 0;

    while n > 0 {
        if (*value >> i) & 1 == 0 {
            *value ^= 1 << i;
            positions.push(offset + i);
            n -= 1;
        }
        i += 1;
    }
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

    #[test]
    fn test_zigzag() -> anyhow::Result<()> {
        let num = 123_456_789;

        let encoded = zigzag_encode(num);

        assert!(num == zigzag_decode(encoded));

        Ok(())
    }

    #[test]
    fn test_crc() -> anyhow::Result<()> {
        let n: u32 = 0;

        println!("{:X}", checksum_crc32("123456789".as_bytes()));

        // println!("{}", crc32(&0x0_u32.to_be_bytes()));

        // for i in 0..4 {
        //     let mask = (n >> i) & 1;
        //     println!("Bit {i}: {}", mask);
        // }

        // let num: u8 = 10;

        // let mut b = 0b_10;
        // for i in 0..size_of_val(&num) * 8 {
        //     let bit = (num >> i) & 1;
        //     b = (b << 1) + bit;
        //     println!("bit: {bit}");
        //     println!("{b:08b}");
        // }

        // println!("{:08b}", 10);
        // println!("{:08b}", 10);

        Ok(())
    }

    #[test]
    fn test_flip_bits() -> anyhow::Result<()> {
        let mut n = 100;

        println!("{:08b}", n);

        // println!("{:?}", flip_n_bits(&mut n, 2));

        println!("{:08b}", n);

        Ok(())
    }
}
