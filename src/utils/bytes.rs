use super::{Error, Result};

/// Used to represent varints. (this is quite missleading because it's used to
/// encode unsigned int, but you can use `zigzag_decode` and signed ints work too).
pub type VarInt = u64;

macro_rules! bytes_try_read_int_impl {
    ($this:ident, $typ:tt::$conv:tt) => {{
        const SIZE: usize = size_of::<$typ>();

        if $this.remaining() < SIZE {
            return Err(Error::OutOfSpace);
        }

        let result = $typ::$conv($this.chunk()[0..SIZE].try_into().unwrap());
        $this.advance(SIZE);
        Ok(result)
    }};
}

macro_rules! bytes_try_write_int_impl {
    ($this:ident, $val:expr) => {{
        let bytes = &$val.to_le_bytes();
        let size = bytes.len();

        if $this.remaining() < size {
            return Err(Error::OutOfSpace);
        }

        $this.chunk_mut()[0..size].copy_from_slice(bytes);
        $this.advance(size);
        Ok(())
    }};
}

/// Used to extract payload from varint (7bits).
const PAYLOAD_MASK: u8 = 0x7F;
/// Used to extract most significant bit (indicates if next byte belongs to varint).
const MOST_SIGNIFICANT_BIT_MASK: u8 = 0x80;

/// Reads varint in little-endian and returns it with number of bytes read.
/// If function returned `None`, then varint was invalid.
pub fn read_varint(buf: &[u8]) -> Result<(VarInt, usize)> {
    let mut varint: VarInt = 0;

    for i in 0..10 {
        let current = *buf.get(i).ok_or(Error::OutOfSpace)?;
        let payload = (current & PAYLOAD_MASK) as VarInt;

        if i == 9 && payload > 1 {
            // overflow
            return Err(Error::InvalidVarInt);
        }

        varint |= payload << (i * 7);

        if current & MOST_SIGNIFICANT_BIT_MASK == 0 {
            return Ok((varint, i + 1));
        }
    }
    Err(Error::InvalidVarInt)
}

/// Returns given number in varint (little-endian) encoding as vector of bytes.
pub fn encode_to_varint(mut value: VarInt) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= MOST_SIGNIFICANT_BIT_MASK;
            bytes.push(byte);
        } else {
            bytes.push(byte);
            break;
        }
    }
    bytes
}

/// Writes given number as varint (little-endian) to beginning of a buffer. Returns
/// how many bytes were written. If `None` was returned, then `buf` was to small.
///
/// # Safety
///
/// Be carefull because even when writing fails, it still can modify buffer.
/// Keep in mind this edge-case when using this function.
fn write_varint(buf: &mut [u8], mut value: VarInt) -> Result<usize> {
    let mut i = 0;

    loop {
        let mut byte = value as u8 & PAYLOAD_MASK;
        value >>= 7;

        if value != 0 {
            byte |= MOST_SIGNIFICANT_BIT_MASK;
        }

        *buf.get_mut(i).ok_or(Error::OutOfSpace)? = byte;
        i += 1;

        if value == 0 {
            return Ok(i);
        }

        if i == 10 {
            // buffer overflowed
            return Err(Error::InvalidVarInt);
        }
    }
}

/// Helper cursor to work with bytes.
pub struct BytesCursor<T> {
    buffer: T,
    /// Current position of cursor in `buffer`.
    position: usize,
}

impl<T> BytesCursor<T> {
    pub fn new(buffer: T) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }

    /// Returns internal `buffer` and consumes `self.
    pub fn into_inner(self) -> T {
        self.buffer
    }

    /// Advances current cursor position by given number of bytes.
    pub fn advance(&mut self, by: usize) {
        self.position += by;
    }

    /// Returns current position of cursor.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Sets new position of cursor.
    pub fn set_position(&mut self, pos: usize) {
        self.position = pos;
    }

    /// Sets current `position` to 0.
    pub fn reset(&mut self) {
        self.position = 0;
    }
}

impl<T: AsRef<[u8]>> BytesCursor<T> {
    /// Returns length of buffer.
    fn len(&self) -> usize {
        self.buffer.as_ref().len()
    }

    /// Returns how many bytes are left to advance.
    fn remaining(&self) -> usize {
        self.len() - self.position
    }

    /// Returns slice of internal buffer from current `position` to end.
    fn chunk(&self) -> &[u8] {
        &self.buffer.as_ref()[self.position..]
    }

    /// Reads `u8` at current buffer position. Advances position by 1.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer, then this function will panic.
    pub fn read_u8(&mut self) -> u8 {
        self.try_read_u8().unwrap()
    }

    /// Reads `u8` at current buffer position. Returns result that indicates if
    /// operation was successful. Advances position by 1.
    pub fn try_read_u8(&mut self) -> Result<u8> {
        if self.remaining() < 1 {
            return Err(Error::OutOfSpace);
        }
        let result = self.chunk()[0];
        self.advance(1);
        Ok(result)
    }

    /// Reads `u16` at current buffer position in little-endian.
    /// Advances position by 2.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or requested value type doesn't
    /// fit in remaining bytes, then this function will panic.
    pub fn read_u16_le(&mut self) -> u16 {
        self.try_read_u16_le().unwrap()
    }

    /// Reads `u16` at current buffer position in little-endian. Returns result
    /// that indicates if operation was successful. Advances position by 2.
    pub fn try_read_u16_le(&mut self) -> Result<u16> {
        bytes_try_read_int_impl!(self, u16::from_le_bytes)
    }

    /// Reads `u32` at current buffer position in little-endian.
    /// Advances position by 4.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or requested value type doesn't
    /// fit in remaining bytes, then this function will panic.
    pub fn read_u32_le(&mut self) -> u32 {
        self.try_read_u32_le().unwrap()
    }

    /// Reads `u32` at current buffer position in little-endian. Returns result
    /// that indicates if operation was successful. Advances position by 4.
    pub fn try_read_u32_le(&mut self) -> Result<u32> {
        bytes_try_read_int_impl!(self, u32::from_le_bytes)
    }

    /// Reads `u64` at current buffer position in little-endian.
    /// Advances position by 8.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or requested value type doesn't
    /// fit in remaining bytes, then this function will panic.
    pub fn read_u64_le(&mut self) -> u64 {
        self.try_read_u64_le().unwrap()
    }

    /// Reads `u64` at current buffer position in little-endian. Returns result
    /// that indicates if operation was successful. Advances position by 8.
    pub fn try_read_u64_le(&mut self) -> Result<u64> {
        bytes_try_read_int_impl!(self, u64::from_le_bytes)
    }

    /// Reads `VarInt` at current buffer position in little-endian.
    /// Advances position by 1-10 depending on varint size.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or requested value type doesn't
    /// fit in remaining bytes, then this function will panic.
    pub fn read_varint(&mut self) -> (VarInt, usize) {
        self.try_read_varint().unwrap()
    }

    /// Reads `VarInt` at current buffer position in little-endian. Returns result
    /// that indicates if operation was successful. Advances position by 1-10
    /// depending on varint size.
    pub fn try_read_varint(&mut self) -> Result<(VarInt, usize)> {
        let (varint, length) = read_varint(self.chunk())?;
        self.advance(length);
        return Ok((varint, length));
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]>> BytesCursor<T> {
    /// Returns mutable slice of internal buffer from current `position` to end.
    fn chunk_mut(&mut self) -> &mut [u8] {
        &mut self.buffer.as_mut()[self.position..]
    }

    /// Wrties `u8` to buffer at current position. Advances position by 1.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or given value doesn't
    /// fit in remaining bytes, then this function will panic.
    pub fn write_u8(&mut self, value: u8) {
        self.try_write_u8(value).unwrap()
    }

    /// Writes `u8` to buffer at current position. Returns result that indicates
    /// if operation was successful. Advances position by 1.
    pub fn try_write_u8(&mut self, value: u8) -> Result<()> {
        if self.remaining() < 1 {
            return Err(Error::OutOfSpace);
        }
        self.chunk_mut()[0] = value;
        self.advance(1);
        Ok(())
    }

    /// Wrties `u16` to buffer in little-endian at current position.
    /// Advances position by 2.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or given value doesn't
    /// fit in remaining bytes, then this function will panic.
    pub fn write_u16_le(&mut self, value: u16) {
        self.try_write_u16_le(value).unwrap();
    }

    /// Writes `u16` to buffer in little-endian at current position. Returns result that indicates
    /// if operation was successful. Advances position by 2.
    pub fn try_write_u16_le(&mut self, value: u16) -> Result<()> {
        bytes_try_write_int_impl!(self, value)
    }

    /// Wrties `u32` to buffer in little-endian at current position.
    /// Advances position by 4.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or given value doesn't
    /// fit in remaining bytes, then this function will panic.
    pub fn write_u32_le(&mut self, value: u32) {
        self.try_write_u32_le(value).unwrap();
    }

    /// Writes `u32` to buffer in little-endian at current position. Returns result that indicates
    /// if operation was successful. Advances position by 4.
    pub fn try_write_u32_le(&mut self, value: u32) -> Result<()> {
        bytes_try_write_int_impl!(self, value)
    }

    /// Wrties `u64` to buffer in little-endian at current position.
    /// Advances position by 8.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or given value doesn't
    /// fit in remaining bytes, then this function will panic.
    pub fn write_u64_le(&mut self, value: u64) {
        self.try_write_u64_le(value).unwrap();
    }

    /// Writes `u64` to buffer in little-endian at current position. Returns result that indicates
    /// if operation was successful. Advances position by 8.
    pub fn try_write_u64_le(&mut self, value: u64) -> Result<()> {
        bytes_try_write_int_impl!(self, value)
    }

    /// Writes `VarInt` to buffer in little-endian at current position.
    /// Advances position by 1-10 depending on varint size.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or requested value doesn't
    /// fit in remaining bytes, then this function will panic.
    pub fn write_varint(&mut self, value: VarInt) -> usize {
        self.try_write_varint(value).unwrap()
    }

    /// Writes `VarInt` to buffer in little-endian at current position. Returns result
    /// that indicates if operation was successful. Advances position by 1-10
    /// depending on varint size.
    pub fn try_write_varint(&mut self, value: VarInt) -> Result<usize> {
        let length = write_varint(self.chunk_mut(), value)?;
        self.advance(length);
        Ok(length)
    }

    /// Writes given `bytes` at current position. Advances position by
    /// `bytes` length.
    ///
    /// # Safety
    ///
    /// If cursor is already at the end of buffer or bytes length doesn't
    /// fit in remaining space, then this function will panic.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.try_write_bytes(bytes).unwrap();
    }

    /// Writes given `bytes` at current position. Returns result that indicates
    /// if operation was successful. Advances position by `bytes` length.
    pub fn try_write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let size = bytes.len();
        if self.remaining() < size {
            return Err(Error::OutOfSpace);
        }
        self.chunk_mut()[..size].copy_from_slice(bytes);
        self.advance(size);
        Ok(())
    }
}

impl<T: AsRef<[u8]> + AsMut<[u8]> + Extend<u8>> BytesCursor<T> {
    /// Extends buffer with single byte. It is added at the ***end*** of current buffer.
    /// If you want to write byte at current position, then use `write_u8`.
    pub fn put_u8(&mut self, value: u8) {
        self.buffer.extend([value].into_iter());
        self.advance(size_of::<u8>());
    }

    /// Extends buffer with u16 in little-endian. It is added at the ***end***
    /// of current buffer. If you want to write u16 at current position,
    /// then use `write_u16`.
    pub fn put_u16_le(&mut self, value: u16) {
        self.buffer.extend(value.to_le_bytes().into_iter());
        self.advance(size_of::<u16>());
    }

    /// Extends buffer with u32 in little-endian. It is added at the ***end***
    /// of current buffer. If you want to write u32 at current position,
    /// then use `write_u32`.
    pub fn put_u32_le(&mut self, value: u32) {
        self.buffer.extend(value.to_le_bytes().into_iter());
        self.advance(size_of::<u32>());
    }

    /// Extends buffer with u64 in little-endian. It is added at the ***end***
    /// of current buffer. If you want to write u64 at current position,
    /// then use `write_u64`.
    pub fn put_u64_le(&mut self, value: u64) {
        self.buffer.extend(value.to_le_bytes().into_iter());
        self.advance(size_of::<u64>());
    }

    /// Extends buffer with varint in little-endian. It is added at the ***end***
    /// of current buffer. If you want to write varint at current position,
    /// then use `write_varint`.
    pub fn put_varint(&mut self, value: VarInt) {
        let varint = encode_to_varint(value);
        let len = varint.len();
        self.buffer.extend(varint.into_iter());
        self.advance(len);
    }

    /// Extends buffer with slice of bytes. It is added at the ***end***
    /// of current buffer. If you want to write bytes at current position,
    /// then use `write_bytes`.
    pub fn put_bytes(&mut self, value: &[u8]) {
        let len = value.len();
        self.buffer.extend(value.iter().copied());
        self.advance(len);
    }
}

/// Returns number of bytes that were used to store varint.
pub fn varint_size(value: VarInt) -> usize {
    encode_to_varint(value).len()
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

/// Reverse byte order in u32.
pub fn byte_swap_u32(value: u32) -> u32 {
    value.swap_bytes()
}

/// Takes two u32 and packs it into sinle u64.
pub fn pack_u64(a: u32, b: u32) -> u64 {
    ((a as u64) << 32) | b as u64
}

/// Takes u64 and unpacks it into two u32.
pub fn unpack_u64(packed: u64) -> (u32, u32) {
    ((packed >> 32) as u32, packed as u32)
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn test_varint() -> anyhow::Result<()> {
        let num = 123_456_789;
        let buf: Vec<u8> = vec![0; 20];
        let mut cursor = BytesCursor::new(buf);

        cursor.write_varint(num);
        cursor.reset();

        assert!(num == cursor.read_varint().0);

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

    #[test]
    fn test_bytes_cursor() -> anyhow::Result<()> {
        let buffer = vec![1, 2, 3];
        let mut cursor = BytesCursor::new(buffer);

        cursor.put_u16_le(10);
        cursor.put_u8(123);

        assert!(vec![1, 2, 3, 10, 0, 123] == cursor.into_inner());

        Ok(())
    }
}
