use std::{
    collections::HashMap,
    io::{Cursor, IoSlice},
    sync::Arc,
};

use crate::{
    storage::{Error, PageNumber, SlotNumber, StorageResult},
    utils::{
        buffer::{Buffer, DropFn},
        bytes,
    },
};

pub const DATABASE_HEADER_SIZE: usize = size_of::<DatabaseHeader>();

pub const DEFAULT_PAGE_SIZE: u16 = 4096;

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
    pub page_size: u16,
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
        let page_size = u16::from_le_bytes(buffer[4..6].try_into().unwrap());
        let reserved_space = u16::from_le_bytes(buffer[6..8].try_into().unwrap());
        let change_counter = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
        let database_size = u32::from_le_bytes(buffer[12..16].try_into().unwrap());
        let freelist_trunk_page = u32::from_le_bytes(buffer[16..20].try_into().unwrap());
        let freelist_pages = u32::from_le_bytes(buffer[20..24].try_into().unwrap());
        let default_page_cache_size = u32::from_le_bytes(buffer[24..28].try_into().unwrap());

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
        buffer[4..6].copy_from_slice(&self.page_size.to_le_bytes());
        buffer[6..8].copy_from_slice(&self.reserved_space.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.change_counter.to_le_bytes());
        buffer[12..16].copy_from_slice(&self.database_size.to_le_bytes());
        buffer[16..20].copy_from_slice(&self.freelist_trunk_page.to_le_bytes());
        buffer[20..24].copy_from_slice(&self.freelist_pages.to_le_bytes());
        buffer[24..28].copy_from_slice(&self.default_page_cache_size.to_le_bytes());
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
    buffer: Arc<Buffer>,
    overflow: HashMap<SlotNumber, bool>,
}

impl Page {
    pub fn new(buffer: Arc<Buffer>) -> Self {
        Self {
            buffer,
            overflow: HashMap::new(),
        }
    }

    pub fn alloc(size: usize, drop: Option<DropFn>) -> Self {
        let buf = Buffer::alloc_page(size, drop);
        Self::new(Arc::new(buf))
    }

    #[allow(clippy::mut_from_ref)]
    pub fn as_ptr(&self) -> &mut [u8] {
        self.buffer.as_mut_slice()
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

    pub fn last_used_offset(&self) -> u16 {
        self.read_u16(5)
    }

    pub fn free_fragments(&self) -> u8 {
        self.read_u8(7)
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

    /// Free space between slot array and last cell.
    pub fn free_space(&self) -> u16 {
        self.last_used_offset() - ((self.header_size()) as u16 + self.len() * 2)
    }

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
}
