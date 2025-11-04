use std::{
    collections::HashMap,
    io::{Cursor, IoSlice},
    ptr::NonNull,
    sync::Arc,
};

use crate::{
    storage::{Error, PageNumber, SLOT_SIZE, SlotNumber, StorageResult},
    utils::{
        buffer::{Buffer, DropFn},
        bytes,
    },
};

pub const DATABASE_HEADER_SIZE: usize = size_of::<DatabaseHeader>();

pub const DEFAULT_PAGE_SIZE: u32 = 4096;

const DEFAULT_CACHE_SIZE: u32 = 2000;

pub const MIN_PAGE_SIZE: usize = 512;
pub const MAX_PAGE_SIZE: usize = 64 << 10;

/// Returns usable space (`Page` size - reserved bytes at the end of `Page`).
pub fn usable_space(page_size: usize, reserved: usize) -> usize {
    page_size - reserved
}

pub fn min_cell_size(usable_space: usize) -> usize {
    ((usable_space - 12) * 32 / 255) - 23
}

pub fn max_cell_size(usable_space: usize) -> usize {
    ((usable_space - 12) * 64 / 255) - 23
}

#[repr(C)]
#[derive(Debug)]
pub struct DatabaseHeader {
    /// Version number of db.
    pub version: u32,
    /// Size of each page in database.
    pub page_size: u32,
    /// Nummber of bytes reserved at the end of each `Page`. Default is 0
    pub reserved_space: u16,
    /// Counts how many times database file was changed. Increments on each update.
    pub change_counter: u32,
    /// Size of database in `Pages`.
    pub database_size: PageNumber,
    /// Page number of first freelist trunk page.
    pub freelist_trunk_page: PageNumber,
    /// Total number of freelist pages.
    pub freelist_pages: u32,
    /// Number of `pages` to cache.
    pub default_page_cache_size: u32,
}

impl DatabaseHeader {
    pub fn get_page_size(&self) -> usize {
        if self.page_size == 1 {
            MAX_PAGE_SIZE
        } else {
            self.page_size as usize
        }
    }

    pub fn from_bytes(buffer: &[u8]) -> Self {
        let version = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        let page_size = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
        let reserved_space = u16::from_le_bytes(buffer[8..10].try_into().unwrap());
        let change_counter = u32::from_le_bytes(buffer[10..14].try_into().unwrap());
        let database_size = u32::from_le_bytes(buffer[14..18].try_into().unwrap());
        let freelist_trunk_page = u32::from_le_bytes(buffer[18..22].try_into().unwrap());
        let freelist_pages = u32::from_le_bytes(buffer[22..26].try_into().unwrap());
        let default_page_cache_size = u32::from_le_bytes(buffer[26..30].try_into().unwrap());

        Self {
            version,
            page_size,
            reserved_space,
            change_counter,
            database_size,
            freelist_trunk_page,
            freelist_pages,
            default_page_cache_size,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![0; DATABASE_HEADER_SIZE];
        self.write_to_buffer(&mut buffer);
        buffer
    }

    pub fn write_to_buffer(&self, buffer: &mut [u8]) {
        buffer[0..4].copy_from_slice(&self.version.to_le_bytes());
        buffer[4..8].copy_from_slice(&self.page_size.to_le_bytes());
        buffer[8..10].copy_from_slice(&self.reserved_space.to_le_bytes());
        buffer[10..14].copy_from_slice(&self.change_counter.to_le_bytes());
        buffer[14..18].copy_from_slice(&self.database_size.to_le_bytes());
        buffer[18..22].copy_from_slice(&self.freelist_trunk_page.to_le_bytes());
        buffer[22..26].copy_from_slice(&self.freelist_pages.to_le_bytes());
        buffer[26..30].copy_from_slice(&self.default_page_cache_size.to_le_bytes());
    }
}

impl Default for DatabaseHeader {
    fn default() -> Self {
        Self {
            version: 1,
            page_size: DEFAULT_PAGE_SIZE,
            reserved_space: 0,
            change_counter: 1,
            database_size: 1,
            freelist_trunk_page: 0,
            freelist_pages: 0,
            default_page_cache_size: DEFAULT_CACHE_SIZE,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum PageType {
    IndexInternal = 5,
    TableInternal = 10,
    IndexLeaf = 15,
    TableLeaf = 20,
}

impl TryFrom<u8> for PageType {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            5 => Ok(Self::IndexInternal),
            10 => Ok(Self::TableInternal),
            15 => Ok(Self::IndexLeaf),
            20 => Ok(Self::TableLeaf),
            _ => Err(Error::InvalidPageType),
        }
    }
}

/// *Page is B-Tree node representation on disk (Page = Node).* \
/// Page contains `Buffer`, which by defaul contains drop function that deallocates `Page`. \
/// If this behavior is unwanted, then pass custom drop function as parameter in `Page::alloc`
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
/// # Header
///
/// Depending on `PageType` header is either 8 (Leaf Pages) or 12 (Internal Pages).
///
///
/// ```text
/// page_type           - 1 byte    - offset 0 - represent PageType.
/// first_freeblock     - 2 bytes   - offset 1 - points to start of first freeblock, if set to 0, then there are no freeblocks.
/// num_slots           - 2 bytes   - offset 3 - number of slots in slot array.
/// last_used_offset    - 2 bytes   - offset 5 - offset to last used space. Used to calculate if new data can fit in Page.
/// num_free_fragments  - 1 byte    - offset 7 - number of free fragments. Tiny gaps between cells, to small to fit new data.
/// next_page           - 4 bytes   - offset 8 - present only in Internal Pages. Points to next B-Tree page.
///
/// ```
/// Total size: 12 bytes (Internal Pages) or 8 bytes (Lead Pages) bytes long.
///
/// # Overflow:
/// If `Page` contains `Cells` that overflow, it maintains hashmap of slots pointing to overflowing `Cells`.
/// Only chunk of `Cell` is stored in `Page` and it needs to be reassembled by going over
///
pub struct Page {
    // Total free space in page. -1 if unknown.
    total_free_space: isize,
    // Buffer that contains page content.
    buffer: Arc<Buffer>,
    // Map of overflowing cells.
    overflow: HashMap<SlotNumber, bool>,
}

impl Page {
    pub fn new(buffer: Arc<Buffer>) -> Self {
        let mut page = Self {
            total_free_space: -1,
            buffer,
            overflow: HashMap::new(),
        };

        page.total_free_space = page.total_free_space();

        page
    }

    // pub fn alloc(size: usize, drop: Option<DropFn>) -> Self {
    //     let buf = Buffer::alloc_page(size, drop);
    //     Self::new(Arc::new(buf))
    // }

    // Returns total free space in page including space gap between slot array and last used offset, freeblocks and fragments.
    pub fn total_free_space(&self) -> isize {
        let mut total = self.free_space() as isize + self.free_fragments() as isize;

        let mut current = self.first_freeblock();
        while current != 0 {
            let (next, size) = self.get_freeblock(current);
            total += size as isize;
            current = next;
        }

        total
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_ptr(&self) -> &mut [u8] {
        self.buffer.as_mut_slice()
    }

    fn usable_space(&self) -> usize {
        (self.buffer.size() - self.header_size())
    }

    pub fn as_io_slice(&self) -> IoSlice {
        IoSlice::new(self.as_ptr())
    }

    fn read_u8(&self, pos: usize) -> u8 {
        self.as_ptr()[pos]
    }

    fn read_u16_no_offset(&self, pos: usize) -> u16 {
        let buf = self.as_ptr();
        u16::from_le_bytes([buf[pos], buf[pos + 1]])
    }

    fn read_u16(&self, pos: usize) -> u16 {
        self.read_u16_no_offset(pos)
    }

    fn read_u32_no_offset(&self, pos: usize) -> u32 {
        let buf = self.as_ptr();
        u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
    }

    fn read_u32(&self, pos: usize) -> u32 {
        self.read_u32_no_offset(pos)
    }

    fn write_u8(&self, pos: usize, value: u8) {
        let buf = self.as_ptr();
        buf[pos] = value;
    }

    fn write_u16_no_offset(&self, pos: usize, value: u16) {
        let buf = self.as_ptr();
        buf[pos..pos + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn write_u16(&self, pos: usize, value: u16) {
        self.write_u16_no_offset(pos, value);
    }

    fn write_u32_no_offset(&self, pos: usize, value: u32) {
        let buf = self.as_ptr();
        buf[pos..pos + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn write_u32(&self, pos: usize, value: u32) {
        self.write_u32_no_offset(pos, value);
    }

    pub fn try_page_type(&self) -> Option<PageType> {
        self.read_u8(0).try_into().ok()
    }

    pub fn page_type(&self) -> PageType {
        self.try_page_type().unwrap()
    }

    pub fn first_freeblock(&self) -> u16 {
        self.read_u16(1)
    }

    pub fn set_first_freeblock(&self, value: u16) {
        self.write_u16(1, value);
    }

    pub fn last_used_offset(&self) -> u16 {
        self.read_u16(5)
    }

    pub fn set_last_used_offset(&self, value: u16) {
        self.write_u16(5, value);
    }

    pub fn free_fragments(&self) -> u8 {
        self.read_u8(7)
    }

    pub fn set_free_fragments(&self, value: u8) {
        self.write_u8(7, value)
    }

    pub fn add_free_fragment(&self, value: u8) {
        self.set_free_fragments(self.free_fragments() + value)
    }

    /// Returns next `PageNumber` if `PageType` is internal or None if `Page` is Lead
    pub fn try_next_page(&self) -> Option<PageNumber> {
        match self.page_type() {
            PageType::IndexInternal | PageType::TableInternal => Some(self.read_u32(8)),
            PageType::IndexLeaf | PageType::TableLeaf => None,
        }
    }

    /// Returns number of slots in `Page`.
    pub fn len(&self) -> u16 {
        self.read_u16(3)
    }

    pub fn set_len(&self, value: u16) {
        self.write_u16(3, value);
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_overflow(&self) -> bool {
        !self.overflow.is_empty()
    }

    /// Depending on `PageType`, this function returns 12 or 8.
    pub fn header_size(&self) -> usize {
        match self.page_type() {
            PageType::IndexInternal | PageType::TableInternal => 12,
            PageType::IndexLeaf | PageType::TableLeaf => 8,
        }
    }

    /// Returns avalible space without header.
    pub fn storage_space(&self) -> u16 {
        (self.buffer.size() - self.header_size()) as u16
    }

    /// Total space that will be used by cell.
    pub fn storage_size(&self, cell_size: u16) -> u16 {
        SLOT_SIZE as u16 + cell_size
    }

    /// Free space between slot array and last cell.
    pub fn free_space(&self) -> u16 {
        self.last_used_offset() - ((self.header_size()) as u16 + self.len() * 2)
    }

    fn push_slot(&self, value: u16) {
        let len = self.len();
        let offset = self.header_size() + len as usize * 2;

        self.write_u16(offset, value);
        self.set_len(len + 1);
    }

    /// Returns freeblock at given offset (offset to next freeblock, size of current freeblock).
    fn get_freeblock(&self, offset: u16) -> (u16, u16) {
        let next_freeblock = self.read_u16(offset as usize);
        let freeblock_size = self.read_u16(offset as usize + 2);
        (next_freeblock, freeblock_size)
    }

    fn take_freeblock(&self, cell_size: u16) -> Option<u16> {
        let mut prev = 0;
        let mut current = self.first_freeblock();

        while current != 0 {
            let (next, current_size) = self.get_freeblock(current);

            if current_size >= cell_size {
                let diff = current_size - cell_size;
                if diff < 4 {
                    if prev == 0 {
                        self.set_first_freeblock(next);
                    } else {
                        self.write_u16(prev as usize, next);
                    }
                    self.add_free_fragment(diff as u8);
                    return Some(current);
                } else {
                    // split freeblock
                    let new_current = current + current_size - cell_size;

                    self.write_u16(current as usize + 2, current_size - cell_size);

                    return Some(new_current);
                }
            }
            prev = current;
            current = next;
        }

        None
    }

    // /// Allocates space for cell and if operation was successfull, returns offset.
    // pub fn try_insert(
    //     &self,
    //     slot_id: SlotNumber,
    //     cell: BTreeCell,
    // ) -> Result<SlotNumber, BTreeCell> {
    //     assert!(cell_size < 4);

    //     let free_space = self.free_space();
    //     let last_used_offset = self.last_used_offset();

    //     if free_space >= 2 {
    //         if let Some(freeblock) = self.take_freeblock(cell_size) {
    //             return Some(freeblock);
    //         }
    //     }
    //     todo!()
    // }

    /// Shifts cells to right to get rid off fragmentation.
    ///
    /// Before:
    ///
    /// ```text
    ///   HEADER   SLOT ARRAY    FREE SPACE                      CELLS
    ///  +------+----+----+----+------------+--------+---------+--------+---------+--------+
    ///  |      | O1 | O2 | O3 | ->      <- | CELL 3 |   DEL   | CELL 2 |   DEL   | CELL 1 |
    ///  +------+----+----+----+------------+--------+---------+--------+---------+--------+
    /// ```
    ///
    /// After:
    ///
    /// ```text
    ///   HEADER   SLOT ARRAY              FREE SPACE                       CELLS
    ///  +------+----+----+----+--------------------------------+--------+--------+--------+
    ///  |      | O1 | O2 | O3 | ->                          <- | CELL 3 | CELL 2 | CELL 1 |
    ///  +------+----+----+----+--------------------------------+--------+--------+--------+
    /// ```
    /// # Algorithm
    ///
    /// We can eliminate fragmentation in-place (without copying the page) by
    /// simply moving the cells that have the largest offset first. In the
    /// figures above, we would move CELL 1, then CELL 2 and finally CELL 3.
    /// This makes sure that we don't write one cell on top of another or we
    /// corrupt the data otherwise.
    fn defragment(&self) {}

    /// Returns offset to Cell at given slot index. Note that in order to work correctly, valid index is needed otherwise it will return weird number.
    pub fn slot_at(&self, idx: usize) -> SlotNumber {
        let slot_offset = self.header_size() + idx * 2;
        self.read_u16(slot_offset)
    }

    pub fn cell_get(
        &self,
        idx: usize,
        min_cell_size: usize,
        max_cell_size: usize,
        usable_space: usize,
    ) -> StorageResult<BTreeCell> {
        let buf = self.as_ptr();

        let offset_to_cell = self.slot_at(idx);

        // buf lifetime is change to 'static to avoid later headache. But we need to be carefull with this reference.
        let buf = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(buf) };

        read_btree_cell(
            buf,
            self.page_type(),
            offset_to_cell as u64,
            min_cell_size,
            max_cell_size,
            usable_space,
        )
    }
}

pub struct SlotArray {
    // Pointer to beginning of slot array
    ptr: NonNull<u8>,
    // Length of slot array
    len: usize,
}

impl SlotArray {
    pub fn new(ptr: NonNull<u8>, len: usize) -> Self {
        Self { ptr, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn slot_array(&self) -> &[u16] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().cast(), self.len) }
    }

    #[inline]
    fn slot_array_mut(&mut self) -> &mut [u16] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().cast(), self.len) }
    }

    /// Reads slot at given index.
    pub fn get(&self, index: usize) -> u16 {
        assert!(index < self.len, "Index out of range");

        self.slot_array()[index].to_le()
    }

    /// Overwrites given slot with new value.
    pub fn set(&mut self, index: usize, value: u16) {
        assert!(index < self.len, "Index out of range");
        self.slot_array_mut()[index] = value.to_le();
    }

    /// Inserts new slot into array and extends length of it.
    ///
    /// # Safety
    ///
    /// You need to ensure that array can grow without any problems, because
    /// it would otherwise overwrite some memory.
    pub fn insert(&mut self, index: usize, value: u16) {
        assert!(index <= self.len, "Index out of range");
        self.len += 1;

        // if index isn't the last one, shift all slots to right
        if index < self.len {
            let end = self.len - 1;
            self.slot_array_mut().copy_within(index..end, index + 1);
        }

        self.slot_array_mut()[index] = value.to_le();
    }

    pub fn push(&mut self, value: u16) {
        self.insert(self.len, value);
    }
}

/// Wraps possible types of cell stored in `Page`. On disk each variable is stored in little endian except varints.
pub enum BTreeCell {
    IndexInternalCell(IndexInternalCell),
    IndexLeafCell(IndexLeafCell),
    TableInternalCell(TableInternalCell),
    TableLeafCell(TableLeafCell),
}

pub struct IndexInternalCell {
    /// Left child of BTree Page
    pub left_child: PageNumber,
    /// Total size of Cell including overflow Pages
    payload_size: u64,
    /// Pointer to content of cell living in Page. It doesn't include overflowing payload
    payload: &'static [u8],
    /// If cell is overflowing then it points to first overflowing page
    first_overflow: Option<PageNumber>,
}

pub struct IndexLeafCell {
    /// Total size of Cell including overflow Pages
    payload_size: u64,
    /// Pointer to content of cell living in Page. It doesn't include overflowing payload
    payload: &'static [u8],
    /// If cell is overflowing then it points to first overflowing page
    first_overflow: Option<PageNumber>,
}

pub struct TableInternalCell {
    /// Unique ID of row
    row_id: i64,
    /// Left child of BTree Page
    pub left_child: PageNumber,
}

pub struct TableLeafCell {
    /// Unique ID of row
    row_id: i64,
    /// Total size of Cell including overflow Pages
    payload_size: u64,
    /// Pointer to content of cell living in Page. It doesn't include overflowing payload
    payload: &'static [u8],
    /// If cell is overflowing then it points to first overflowing page
    first_overflow: Option<PageNumber>,
}

/// Creates `BTreeCell` based on provided `PageType`.
pub fn read_btree_cell(
    buf: &'static [u8],
    page_type: PageType,
    pos: u64,
    min_cell_size: usize,
    max_cell_size: usize,
    usable_space: usize,
) -> StorageResult<BTreeCell> {
    let mut cursor = Cursor::new(buf);
    cursor.set_position(pos as u64);

    match page_type {
        PageType::IndexInternal => {
            let left_child = bytes::get_u32(&mut cursor)?;
            let payload_size = bytes::read_varint(&mut cursor);

            let (is_overflowing, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            let first_overflow = if is_overflowing {
                Some(bytes::get_u32(&mut cursor)?)
            } else {
                None
            };

            let start = cursor.position() as usize;
            let end = start + local_payload_size;

            let payload = &cursor.into_inner()[start..end];

            let cell = IndexInternalCell {
                left_child,
                payload_size,
                payload,
                first_overflow,
            };

            Ok(BTreeCell::IndexInternalCell(cell))
        }
        PageType::IndexLeaf => {
            let payload_size = bytes::read_varint(&mut cursor);

            let (is_overflowing, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            let first_overflow = if is_overflowing {
                Some(bytes::get_u32(&mut cursor)?)
            } else {
                None
            };

            let start = cursor.position() as usize;
            let end = start + local_payload_size;

            let payload = &cursor.into_inner()[start..end];

            let cell = IndexLeafCell {
                payload_size,
                payload,
                first_overflow,
            };

            Ok(BTreeCell::IndexLeafCell(cell))
        }
        PageType::TableInternal => {
            let row_id = bytes::zigzag_decode(bytes::read_varint(&mut cursor));
            let left_child = bytes::get_u32(&mut cursor)?;
            let cell = TableInternalCell { row_id, left_child };
            Ok(BTreeCell::TableInternalCell(cell))
        }
        PageType::TableLeaf => {
            let row_id = bytes::zigzag_decode(bytes::read_varint(&mut cursor));
            let payload_size = bytes::read_varint(&mut cursor);

            let (is_overflowing, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            let first_overflow = if is_overflowing {
                Some(bytes::get_u32(&mut cursor)?)
            } else {
                None
            };

            let start = cursor.position() as usize;
            let end = start + local_payload_size;

            let payload = &cursor.into_inner()[start..end];

            let cell = TableLeafCell {
                row_id,
                payload_size,
                payload,
                first_overflow,
            };
            Ok(BTreeCell::TableLeafCell(cell))
        }
    }
}

/// Calculates if cell overflows and returns size of payload that is stored in cell with overflow pointer (not includes overflow content).
pub fn cell_overflows(
    payload_size: usize,
    min_cell_size: usize,
    max_cell_size: usize,
    usable_space: usize,
) -> (bool, usize) {
    if payload_size <= max_cell_size {
        return (false, payload_size);
    }
    let mut to_store = min_cell_size + ((payload_size - min_cell_size) % (usable_space - 4));
    if to_store > max_cell_size {
        to_store = min_cell_size
    }
    (true, to_store + 4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_max() -> anyhow::Result<()> {
        // let page = Page::alloc(4096, None);
        // page.as_ptr()[0] = 5;

        // println!("Min cell size: {}", page.min_cell_size());
        // println!("Max cell size: {}", page.max_cell_size());

        Ok(())
    }
}
