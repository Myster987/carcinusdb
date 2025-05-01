use std::{
    alloc::{Layout, alloc_zeroed},
    io::Cursor,
    mem,
    ptr::{self, NonNull},
};

use crate::{
    utils::{buffer::Buffer, bytes::get_u16},
};

use super::{PageNumber, SLOT_SIZE};

// pub const CONFIG_PAGE_SIZE: usize = mem::size_of::<ConfigPage>();

pub const DEFAULT_PAGE_SIZE: usize = 4096;

pub const PAGE_HEADER_SIZE: usize = mem::size_of::<PageHeader>();
// pub const PAGE_ALIGNMENT: usize = PAGE_SIZE;

pub const MIN_PAGE_SIZE: usize = 512;
pub const MAX_PAGE_SIZE: usize = 64 << 10;

pub const CELL_HEADER_SIZE: usize = mem::size_of::<CellHeader>();
pub const CELL_ALIGNMENT: usize = mem::align_of::<CellHeader>();

/// offset, bytes
/// 0   4 - version
/// 4   2 - page size
// #[derive(Debug)]
// pub struct ConfigPage {
//     pub version: u32,
//     pub page_size: u16,
// }

// impl ConfigPage {
//     pub fn new(version: u32, page_size: u16) -> Self {
//         Self { version, page_size }
//     }

//     pub fn from_bytes(cursor: &mut Cursor<&[u8]>) -> DatabaseResult<Self> {
//         let version = get_u32(cursor)?;
//         let page_size = get_u16(cursor)?;

//         Ok(Self { version, page_size })
//     }
// }

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct CellHeader {
    /// Number of cell's left child
    pub left_child: PageNumber,
    /// Size of cell
    size: u16,
    /// True if `Cell` has overflow
    pub is_overflow: bool,
}

// impl From<&[u8]> for CellHeader {
//     /// Note that this assumes that slice of bytes is size of `CELL_HEADER_SIZE`.
//     fn from(value: &[u8]) -> Self {
//         // let cell_header = *from_bytes(value);
//         let cell_header = cast_slice(value)[0];

//         cell_header
//     }
// }

pub struct Cell {
    pub header: CellHeader,

    /// If [`Cellheader::is_overflow`] is true then last 4 bytes point to overflow page
    pub content: [u8],
}

impl Cell {
    pub fn new(mut content: Vec<u8>) -> Box<Self> {
        let aligned_size = Self::align_to_payload(&content);

        content.resize(aligned_size as usize, 0);

        let mut buf: Buffer<CellHeader> = unsafe {
            let layout =
                Layout::from_size_align((aligned_size as usize) + CELL_HEADER_SIZE, CELL_ALIGNMENT)
                    .unwrap();

            let ptr = NonNull::new(std::slice::from_raw_parts_mut(
                alloc_zeroed(layout),
                layout.size(),
            ))
            .unwrap();

            Buffer::from_non_null(ptr, true)
        };

        buf.header_mut().size = aligned_size;

        buf.cotent_mut().copy_from_slice(&content);

        unsafe {
            Box::from_raw(ptr::slice_from_raw_parts(
                buf.into_non_null().cast::<u8>().as_ptr(),
                aligned_size as usize,
            ) as *mut Cell)
        }
    }

    /// Total size of `Cell` with header.
    pub fn total_size(&self) -> u16 {
        (CELL_HEADER_SIZE + self.content.len()) as u16
    }

    /// Total size of `Cell` with header and slot id.
    pub fn storage_size(&self) -> u16 {
        self.total_size() + (SLOT_SIZE as u16)
    }

    pub fn align_to_payload(payload: &[u8]) -> u16 {
        Layout::from_size_align(payload.len(), CELL_ALIGNMENT)
            .unwrap()
            .pad_to_align()
            .size() as u16
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
            num_slots: 0,
            free_space: Page::usable_space(size) as u16,
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
    buffer: Buffer<PageHeader>,
}

impl Page {
    pub fn new(size: usize) -> Self {
        let buffer = Buffer::alloc_page(size);

        Self { buffer }
    }

    pub fn usable_space(size: usize) -> usize {
        size - PAGE_HEADER_SIZE
    }

    pub fn len(&self) -> u16 {
        self.buffer.header().num_slots
    }

    pub fn size(&self) -> usize {
        self.buffer.size
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // pub fn slot_array(&self) -> &[u16] {
    //     let start = PAGE_HEADER_SIZE;
    //     let end = start + self.header.num_slots as usize * 2;
    //     cast::cast_slice(&self.buffer.content.as_ref()[start..end])
    // }

    // pub fn slot_array_mut(&mut self) -> &mut [u16] {
    //     let start = PAGE_HEADER_SIZE;
    //     let end = start + self.header.num_slots as usize * 2;
    //     cast::cast_slice_mut(&mut self.buffer.content.as_mut()[start..end])
    // }

    // pub fn cell_header_at_offset(&self, offset: usize) -> Cell {
    //     let start = offset - CELL_HEADER_SIZE;
    //     let end = offset;
    //     let slice_of_buffer = &self.buffer.content[start..=end];
    //     let cell_header = CellHeader::from(slice_of_buffer);

    //     todo!()
    // }
}

impl Default for Page {
    fn default() -> Self {
        let buffer = Buffer::default();

        Self { buffer }
    }
}

impl<H> From<Buffer<H>> for Page {
    fn from(value: Buffer<H>) -> Self {
        assert!(
            value.content.len() == PAGE_SIZE,
            "Buffer size is invalid. Expected: {PAGE_SIZE}, got: {}",
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

    use super::*;

    #[test]
    #[ignore = "currently not needed"]
    fn create_test_file() -> anyhow::Result<()> {
        let mut f = std::fs::File::create_new("test.db")?;

        let version: u32 = 1;
        let page_size: u16 = 4096;

        f.write(&version.to_be_bytes())?;
        f.write(&page_size.to_be_bytes())?;

        Ok(())
    }
}
