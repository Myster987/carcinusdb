use std::{
    alloc::{Layout, alloc, alloc_zeroed},
    collections::{BinaryHeap, HashMap},
    fmt::Debug,
    mem::{self, ManuallyDrop},
    ptr::{self, NonNull},
};

use crate::utils::{buffer::Buffer, cast};

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

    /// If [`Cellheader::is_overflow`] is true then last 4 bytes point to overflow page
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

            Buffer::from_non_null(ptr)
        };

        buf.header_mut().size = aligned_size;

        buf.content_mut().copy_from_slice(&content);

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

impl ToOwned for Cell {
    type Owned = Box<Self>;

    fn to_owned(&self) -> Self::Owned {
        let layout = Layout::for_value(self);
        let size = layout.size();

        // allocates new memory on heap (first by creating fake slice and then corecing it into thin pointer)
        let owned = unsafe {
            NonNull::new_unchecked(
                NonNull::new(std::slice::from_raw_parts_mut(alloc(layout), size))
                    .unwrap()
                    .as_ptr() as *mut u8,
            )
        };

        let cell = ptr::from_ref(self);
        // copies data from current heap location into another (casts &self into thin NonNull<u8> pointer and copies header with content of cell)
        unsafe {
            owned.copy_from_nonoverlapping(
                NonNull::new_unchecked(cell.cast_mut().cast()),
                self.total_size() as usize,
            );
            // this is Weird DST part, if we have allocated 50 bytes and DST stores 40 bytes and 10 bytes is header, then size of DST is 40 not 50
            // that's why we have to create fake pointer with size of DST (self.content.len() in this case), because when using self.total_size() it will break
            // all this operations should be safe, because we ensure proper alignmet
            Box::from_raw(
                ptr::slice_from_raw_parts(owned.as_ptr(), self.content.len()) as *mut Self,
            )
        }
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

    pub fn max_allowed_payload_size(&self) -> u16 {
        Self::max_payload_size(Self::usable_space(self.size()))
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

    /// Returns owned cell by coping.
    pub fn owned_cell(&self, index: SlotNumber) -> Box<Cell> {
        self.cell(index).to_owned()
    }

    /// Inserts cell at given index. Can cause overflow.
    pub fn insert(&mut self, index: SlotNumber, cell: Box<Cell>) {
        assert!(
            cell.content.len() <= self.max_allowed_payload_size() as usize,
            "can't store data of size {} when max allowed payload size is {}",
            cell.content.len(),
            self.max_allowed_payload_size()
        );

        assert!(
            index <= self.len(),
            "index out of range for page of length: {}",
            self.len()
        );

        if self.is_overflow() {
            self.overflow.insert(index, cell);
        } else if let Err(cell) = self.try_insert(index, cell) {
            self.overflow.insert(index, cell);
        }
    }

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
            self.compact();
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

    /// Removes given index in slot array and returns owned cell (`Box<Cell>`) that was under this index. Also incresess free space
    pub fn remove(&mut self, index: SlotNumber) -> Box<Cell> {
        let len = self.len();

        let cell = self.owned_cell(index);

        assert!(index < len, "index {index} out of range for length {len}");

        self.slot_array_mut()
            .copy_within(index as usize + 1..len as usize, index as usize);

        self.header_mut().num_slots -= 1;
        self.header_mut().free_space += cell.storage_size();

        cell
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
    fn compact(&mut self) {
        let mut order = BinaryHeap::from_iter(
            self.slot_array()
                .iter()
                .enumerate()
                .map(|(i, offset)| (i, *offset)),
        );

        let mut current_offset = self.size() - PAGE_HEADER_SIZE;

        while let Some((index, offset)) = order.pop() {
            unsafe {
                let cell = self.cell_at_offset(offset);
                let size = cell.as_ref().total_size() as usize;

                current_offset -= size;

                cell.cast::<u8>().copy_to(
                    self.buffer.content.byte_add(current_offset).cast::<u8>(),
                    size,
                );

                self.slot_array_mut()[index] = current_offset as u16;
            }
        }

        self.header_mut().last_used_offset = current_offset as u16;
    }

    pub fn into_buffer(mut self) -> Buffer<PageHeader> {
        let Page { buffer, overflow } = self;

        buffer
    }
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

#[derive(Debug)]
pub struct OverflowPageHeader {
    /// Next overflow page
    pub next: PageNumber,
    /// Number of bytes stored in this page
    pub num_bytes: u16,
}

pub struct OverflowPage {
    buffer: Buffer<OverflowPageHeader>,
}

impl OverflowPage {
    pub fn alloc(size: usize) -> Self {
        Self {
            buffer: Buffer::alloc_page(size),
        }
    }

    pub fn usable_space(size: usize) -> u16 {
        Buffer::<OverflowPageHeader>::usable_space(size)
    }

    pub fn header(&self) -> &OverflowPageHeader {
        self.buffer.header()
    }

    pub fn header_mut(&mut self) -> &mut OverflowPageHeader {
        self.buffer.header_mut()
    }

    /// Returns read-only reference to payload (not whole buffer)
    pub fn payload(&self) -> &[u8] {
        &self.buffer.content()[..self.header().num_bytes as usize]
    }
}

impl AsRef<[u8]> for OverflowPage {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}
impl AsMut<[u8]> for OverflowPage {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut()
    }
}

impl<H> From<Buffer<H>> for OverflowPage {
    fn from(buffer: Buffer<H>) -> Self {
        let mut buffer = buffer.cast();

        *buffer.header_mut() = OverflowPageHeader {
            next: 0,
            num_bytes: 0,
        };

        Self { buffer }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn page_into() -> anyhow::Result<()> {
        let page = Page::alloc(512);
        println!("{:?}", page);
        let buffer = page.into_buffer();
        println!("{:?}", buffer);

        Ok(())
    }

    #[test]
    fn main_test() -> anyhow::Result<()> {
        // let mut page = Page::alloc(512);

        // let s = "maciek";

        // println!("{:?}", page.slot_array_mut());

        // println!("{:?}", page);

        let content = "1".to_string();

        let cell = Cell::new(content.as_bytes().to_vec());
        let owned = cell.to_owned();

        println!("cell: {}, owned: {}", cell.total_size(), owned.total_size());

        println!("{:?}", cell);

        println!("{:?}", owned);

        Ok(())
    }
}
