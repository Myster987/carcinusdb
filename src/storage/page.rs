//! ## 6 bytes header

use std::{io::Cursor, mem};

use bytes::{Buf, BytesMut};

use crate::{
    error::DatabaseResult,
    pager::DEFAULT_PAGE_SIZE,
    utils::bytes::{Buffer, get_bool, get_u16, get_u32},
};

use super::PageNumber;

pub const CONFIG_PAGE_SIZE: usize = 6;

pub const PAGE_HEADER_SIZE: usize = 6;

pub const CELL_HEADER_SIZE: usize = 7;

/// offset, bytes
/// 0   4 - version
/// 4   2 - page size
#[derive(Debug)]
pub struct ConfigPage {
    pub version: u32,
    pub page_size: u16,
}

impl ConfigPage {
    pub fn new(version: u32, page_size: u16) -> Self {
        Self { version, page_size }
    }

    pub fn from_bytes(cursor: &mut Cursor<&[u8]>) -> DatabaseResult<Self> {
        let version = get_u32(cursor)?;
        let page_size = get_u16(cursor)?;

        Ok(Self { version, page_size })
    }
}

pub struct CellHeader {
    /// size of cell
    size: u16,

    pub is_overflow: bool,
    pub left_child: PageNumber,
}

// impl CellHeader {
//     pub fn new()
// }

impl From<&[u8]> for CellHeader {
    fn from(value: &[u8]) -> Self {
        let mut cursor = Cursor::new(value);

        let size = get_u16(&mut cursor).unwrap();
        let is_overflow = get_bool(&mut cursor).unwrap();
        let left_child = get_u32(&mut cursor).unwrap();

        Self {
            size,
            is_overflow,
            left_child,
        }
    }
}

pub struct Cell {
    pub header: CellHeader,

    /// If [`Cellheader::is_overflow`] is true then last 4 bytes point to overflow page
    pub content: BytesMut,
}

impl Cell {
    pub fn new(content: &[u8]) -> Self {
        let header = CellHeader::from(content);

        Self {
            header,
            content: BytesMut::from(content),
        }
    }

    
}

pub struct PageHeader {
    /// Free space in page
    free_space: u16,

    /// Number of items in slot array
    num_slots: u16,

    /// Offset to last inserted cell in page.
    ///
    /// ```text
    /// +--------+------------+-------------------+--------+--------+--------+
    /// | Header | Slot array | Free space        | Cell 3 | Cell 2 | Cell 1 |
    /// +--------+------------+-------------------+--------+--------+--------+
    ///          ^ start                          ^ offset                                 
    ///          |                                |
    ///          +--------------------------------+
    /// ```
    /// True offset is calculated by adding page header size (6 bytes) to `last_used_offset`.\
    /// `Offset` to cells = 6 bytes (48 bits) + `last_used_offset`
    last_used_offset: u16,
}

impl PageHeader {
    pub fn new(size: usize) -> Self {
        Self {
            free_space: Page::usable_space(size) as u16,
            num_slots: 0,
            last_used_offset: Page::usable_space(size) as u16,
        }
    }
}

impl From<&[u8]> for PageHeader {
    fn from(value: &[u8]) -> Self {
        let mut cursor = Cursor::new(value);

        let free_space = get_u16(&mut cursor).unwrap();
        let num_slots = get_u16(&mut cursor).unwrap();
        let last_used_offset = get_u16(&mut cursor).unwrap();

        Self {
            free_space,
            num_slots,
            last_used_offset,
        }
    }
}

/// *Page is B-Tree node representation on disk (Page = Node)* \
/// Layout:
///
/// ```text
///                 SLOT ARRAY                                        CELLS
/// +---------------------------------------------------------------------------------+
/// | Page Header | 01 | 02 | 03 | -> Free space <- |    Cell 3     | Cell 2 | Cell 1 |
/// +---------------------------------------------------------------------------------+
///                 |    |     |                    ^               ^        ^
///                 |    |     |      offsets       |               |        |
///                 |    |     +--------------------+               |        |
///                 |    |                                          |        |
///                 |    +------------------------------------------+        |
///                 |                                                        |
///                 +--------------------------------------------------------+
/// ```
///
pub struct Page {
    pub header: PageHeader,
    buffer: Buffer,
}

impl Page {
    pub fn new(size: usize) -> Self {
        let header = PageHeader::new(size);
        let buffer = Buffer::alloc(size);

        Self { header, buffer }
    }

    pub fn usable_space(size: usize) -> usize {
        size - PAGE_HEADER_SIZE
    }

    pub fn len(&self) -> u16 {
        self.header.num_slots
    }

    pub fn slot_array(&self) -> &[u16] {
        let start = PAGE_HEADER_SIZE;
        let end = start + self.header.num_slots as usize * 2;
        bytemuck::cast_slice(&self.buffer.content.as_ref()[start..end])
    }

    pub fn slot_array_mut(&mut self) -> &mut [u16] {
        let start = PAGE_HEADER_SIZE;
        let end = start + self.header.num_slots as usize * 2;
        bytemuck::cast_slice_mut(&mut self.buffer.content.as_mut()[start..end])
    }

    pub fn get_cell_at_offset(&self, offset: usize) -> Cell {


        todo!()
    }
}

impl Default for Page {
    fn default() -> Self {
        let header = PageHeader::new(DEFAULT_PAGE_SIZE);
        let buffer = Buffer::default();

        Self { header, buffer }
    }
}

impl From<Buffer> for Page {
    fn from(value: Buffer) -> Self {
        assert!(
            value.content.len() == DEFAULT_PAGE_SIZE,
            "Buffer size is invalid. Expected: {DEFAULT_PAGE_SIZE}, got: {}",
            value.content.len()
        );

        let buffer = value.content.as_ref();

        let page_header = PageHeader::from(&buffer[..PAGE_HEADER_SIZE]);

        Self {
            header: page_header,
            buffer: value,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use crate::error::DatabaseError;

    use super::*;

    #[test]
    fn create_test_file() -> anyhow::Result<()> {
        let mut f = std::fs::File::create_new("test.db")?;

        let version: u32 = 1;
        let page_size: u16 = 4096;

        f.write(&version.to_be_bytes())?;
        f.write(&page_size.to_be_bytes())?;

        Ok(())
    }

    #[test]
    fn aligment() -> anyhow::Result<()> {
        let a: &[u8] = &[0, 1, 0];

        let b: &[u16] = bytemuck::try_cast_slice(a).map_err(|_| DatabaseError::Unknown)?;

        println!("u16: {:?}", b);
        println!("u8: {:?}", a);

        Ok(())
    }

    #[test]
    fn buffer_test() -> anyhow::Result<()> {
        let buf = Buffer::alloc(4096);

        let page = Page::from(buf);

        Ok(())
    }
}
