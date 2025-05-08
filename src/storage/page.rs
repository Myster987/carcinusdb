use std::{
    alloc::{alloc_zeroed, Layout}, collections::HashMap, fmt::Debug, io::Cursor, mem, ptr::{self, NonNull}
};

use crate::utils::{buffer::Buffer, bytes::get_u16, cast};

use super::{PAGE_NUMBER_SIZE, PageNumber, SLOT_SIZE, SlotNumber};

pub const DEFAULT_PAGE_SIZE: usize = 4096;

pub const PAGE_HEADER_SIZE: usize = mem::size_of::<PageHeader>();

pub const MIN_PAGE_SIZE: usize = 512;
pub const MAX_PAGE_SIZE: usize = 64 << 10;

pub const CELL_HEADER_SIZE: usize = mem::size_of::<CellHeader>();
pub const CELL_ALIGNMENT: usize = mem::align_of::<CellHeader>();

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

/// Helps with operations on `Page`
#[derive(Debug)]
pub struct Cell {
    pub header: CellHeader,

    /// If [`Cellheader::is_overflow`] is true then last
    pub content: [u8],
}

impl Cell {
    pub fn new(mut content: Vec<u8>) -> Box<Self> {
        let aligned_size = Self::align_to_payload(&content);

        content.resize(aligned_size as usize, 0);

        // Alignment makes it safe. `content` is aligned to `CELL_ALIGNMENT`, so we don't have to worry about invalid buffer size.
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

        // Creates pointer to heap, because `Cell` size can't by known at compile time.
        // Content is already aligned. which makes this operation safe.
        unsafe {
            Box::from_raw(ptr::slice_from_raw_parts(
                buf.into_non_null().cast::<u8>().as_ptr(),
                aligned_size as usize,
            ) as *mut Cell)
        }
    }

    /// Creates `Cell` by extending `content` with `overflow_page` number
    pub fn new_overflow(mut content: Vec<u8>, overflow_page: PageNumber) -> Box<Self> {
        content.extend_from_slice(&overflow_page.to_le_bytes());

        let mut cell = Self::new(content);
        cell.header.is_overflow = true;

        cell
    }

    pub fn over_page(&self) -> PageNumber {
        if !self.header.is_overflow {
            return 0;
        }

        PageNumber::from_le_bytes(
            self.content[self.content.len() - PAGE_NUMBER_SIZE..]
                .try_into()
                .expect("Invalid page number"),
        )
    }

    /// Total size of `Cell` with header.
    pub fn total_size(&self) -> u16 {
        (CELL_HEADER_SIZE + self.content.len()) as u16
    }

    /// Total size of `Cell` with header and slot id.
    pub fn storage_size(&self) -> u16 {
        self.total_size() + (SLOT_SIZE as u16)
    }

    /// Returns payload size, after it is aligned to `CELL_ALIGNMENT`.
    pub fn align_to_payload(payload: &[u8]) -> u16 {
        Layout::from_size_align(payload.len(), CELL_ALIGNMENT)
            .unwrap()
            .pad_to_align()
            .size() as u16
    }
}

/// Stores information about `Page`
#[derive(Debug, Clone, Copy)]
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

    /// Stores pointer to next `Page` on the same level and allows for better concurrency (B-link-tree by P. Lehman and S. Yao)
    pub next_page: PageNumber,
}

impl PageHeader {
    pub fn new(size: usize) -> Self {
        Self {
            num_slots: 0,
            free_space: Page::usable_space(size) as u16,
            last_used_offset: Page::usable_space(size) as u16,
            next_page: 0,
        }
    }
}

impl From<&[u8]> for PageHeader {
    /// Assumes that `&[u8]` len == size_of `PageHeader`
    fn from(value: &[u8]) -> Self {
        cast::from_bytes::<Self>(value).to_owned()
    }
}

/// *Page is B-Tree node representation on disk (Page = Node)* \
/// # Layout:
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
/// # Overflow:
/// If `Page` contains `Cells` that overflow, it maintains hashmap of slots pointing to overflowing `Cells`.
/// Only chunk of `Cell` is stored in `Page` and it needs to be reassembled by going over
///
pub struct Page {
    buffer: Buffer<PageHeader>,
    overflow: HashMap<SlotNumber, Box<Cell>>,
}

impl Page {
    pub fn alloc(size: usize) -> Self {
        let buffer = Buffer::alloc_page(size);

        Self {
            buffer,
            overflow: HashMap::new(),
        }
    }

    pub fn usable_space(size: usize) -> u16 {
        Buffer::<PageHeader>::usable_space(size)
    }

    /// Number of slots stored in `Page`
    pub fn len(&self) -> u16 {
        self.buffer.header().num_slots
    }

    /// Size of `Page` in bytes (including header)
    pub fn size(&self) -> usize {
        self.buffer.size
    }

    pub fn free_space(&self) -> u16 {
        self.header().free_space
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_overflow(&self) -> bool {
        !self.overflow.is_empty()
    }

    /// Returns max payload that can be inserted into `Page`
    pub fn max_payload_size(usable_space: u16) -> u16 {
        (usable_space - CELL_HEADER_SIZE as u16 - SLOT_SIZE as u16) & !(CELL_ALIGNMENT as u16 - 1)
    }

    /// Returns most optimal item sizes that page can hold
    pub fn ideal_payload_size(page_size: usize, min_cell: usize) -> u16 {
        let ideal_size = Self::max_payload_size(Self::usable_space(page_size) / min_cell as u16);

        ideal_size
    }

    pub fn header(&self) -> &PageHeader {
        self.buffer.header()
    }

    pub fn header_mut(&mut self) -> &mut PageHeader {
        self.buffer.header_mut()
    }

    pub fn slot_array_non_null(&self) -> NonNull<[u16]> {
        NonNull::slice_from_raw_parts(self.buffer.content.cast(), self.header().num_slots as usize)
    }

    pub fn slot_array(&self) -> &[u16] {
        unsafe { self.slot_array_non_null().as_ref() }
    }

    pub fn slot_array_mut(&self) -> &mut [u16] {
        unsafe { self.slot_array_non_null().as_mut() }
    }

    /// Returns pointer to `CellHeader` at given offset.
    ///
    /// # Safety
    ///
    /// This function is marked as unsafe because we cannot guarantee that bytes at given offset will be valid.
    unsafe fn cell_header_at_offset(&self, offset: u16) -> NonNull<CellHeader> {
        unsafe { self.buffer.content.byte_add(offset as usize).cast() }
    }

    // Returns pointer cell at given offset
    unsafe fn cell_at_offset(&self, offset: u16) -> NonNull<Cell> {
        let header = unsafe { self.cell_header_at_offset(offset) };
        let size = unsafe { header.as_ref().size } as usize;

        let cell = ptr::slice_from_raw_parts(header.cast::<u8>().as_ptr(), size) as *mut Cell;

        // Safety: pointer must be non-null
        unsafe { NonNull::new_unchecked(cell) }
    }

    /// Returns cell at given slot index
    fn cell_at_slot(&self, index: SlotNumber) -> NonNull<Cell> {
        let offset = self.slot_array()[index as usize];

        unsafe { self.cell_at_offset(offset) }
    }

    pub fn cell(&self, index: SlotNumber) -> &Cell {
        let cell = self.cell_at_slot(index);
        // Safety same as `Self::cell_at_slot`
        unsafe { cell.as_ref() }
    }

    pub fn cell_mut(&mut self, index: SlotNumber) -> &mut Cell {
        let mut cell = self.cell_at_slot(index);
        // Safety same as `Self::cell_at_slot`
        unsafe { cell.as_mut() }
    }

    pub fn insert(&mut self, index: SlotNumber, cell: Box<Cell>) {}

    fn try_insert(&mut self, index: SlotNumber, cell: Box<Cell>) -> Result<SlotNumber, Box<Cell>> {
        // We can't fit `Cell` in this `Page`.
        if self.header().free_space < cell.storage_size() {
            return Err(cell);
        }

        let avalible_space = {
            let end = self.header().last_used_offset;
            let start = self.header().num_slots * SLOT_SIZE as u16;
            end - start
        };

        if avalible_space < cell.storage_size() {
            // defragmet page (delete dead cells)
            todo!()
        }

        // Offset to beginning of a location where new `cell` will be inserted
        let offset = self.header().last_used_offset - cell.total_size();

        unsafe {
            let header = self.cell_header_at_offset(offset);
            header.write(cell.header);

            let mut content =
                NonNull::slice_from_raw_parts(header.add(1).cast(), cell.header.size as usize);
            content.as_mut().copy_from_slice(&cell.content);
        }

        self.header_mut().last_used_offset = offset;
        self.header_mut().free_space -= cell.storage_size();

        self.header_mut().num_slots += 1;

        if index < self.header_mut().num_slots {
            let end = self.header().num_slots as usize - 1;

            self.slot_array_mut()
                .copy_within(index as usize..end, index as usize + 1);
        }

        self.slot_array_mut()[index as usize] = offset;

        Ok(index)
    }

    pub fn remove(&mut self, index: SlotNumber) -> Box<Cell> {
        let len = self.len();

        self.header_mut().num_slots -= 1;

        todo!()
    }

    /// Reclaims unused space after deleted cells. Current free space doesn't change, but placement of cells  
    ///
    /// ## Before:
    /// ```text
    /// +-------------------------------------------------------------------------------------+
    /// | 01 | 02 | 03 |             |  Cell 3  |     DEL     | Cell 2 |  DEL  |    Cell 1    |
    /// +-------------------------------------------------------------------------------------+
    ///
    /// ```
    /// ## After:
    /// ```text
    /// +-------------------------------------------------------------------------------------+
    /// | 01 | 02 | 03 |  ---  shifted to the right  -->  |  Cell 3  |  Cell 2 |    Cell 1    |
    /// +-------------------------------------------------------------------------------------+
    /// ```
    ///
    fn compact(&mut self) {}
}

impl AsRef<[u8]> for Page {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}
impl AsMut<[u8]> for Page {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut()
    }
}

impl<H> From<Buffer<H>> for Page {
    fn from(buffer: Buffer<H>) -> Self {
        let mut buffer = buffer.cast();

        *buffer.header_mut() = PageHeader::new(buffer.size);

        Self {
            buffer,
            overflow: HashMap::new(),
        }
    }
}
impl Default for Page {
    fn default() -> Self {
        Self::alloc(DEFAULT_PAGE_SIZE)
    }
}
impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
        .field("buffer", &self.buffer)
        .field("overflow", &self.overflow)
        .finish()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn main_test() -> anyhow::Result<()> {
        let mut page = Page::alloc(512);

        page.slot_array_mut();
        
        println!("{:?}", page);

        Ok(())
    }
}
