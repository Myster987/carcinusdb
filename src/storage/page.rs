use std::{cell::RefCell, collections::HashMap, io::Cursor, sync::Arc};

use crate::{
    storage::{Error, PageNumber, SlotNumber, StorageResult},
    utils::{
        buffer::{Buffer, DropFn},
        bytes,
    },
};

pub const DEFAULT_PAGE_SIZE: u16 = 4096;

const DEFAULT_CACHE_SIZE: i32 = -2000;

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

#[derive(Debug)]
pub struct DatabaseHeader {
    /// Version number of db.
    version: u32,
    /// Size of each page in database.
    page_size: u16,
    /// Nummber of bytes reserved at the end of each `Page`. Default is 0
    pub reserved_space: u8,
    /// Counts how many times database file was changed. Increments on each update.
    change_counter: u32,
    /// Size of database in `Pages`.
    pub database_size: PageNumber,
    /// Page number of first freelist trunk page.
    pub freelist_trunk_page: PageNumber,
    /// Total number of freelist pages.
    pub freelist_pages: u32,
    /// Increments when schema changes.
    schema_cookie: u32,
    /// Cache size is stored as negative number, and it means that it holds X KiB of memory.
    pub default_page_cache_size: i32,
}

impl DatabaseHeader {
    pub fn get_page_size(&self) -> usize {
        if self.page_size == 1 {
            MAX_PAGE_SIZE
        } else {
            self.page_size as usize
        }
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
            schema_cookie: 0,
            default_page_cache_size: DEFAULT_CACHE_SIZE,
        }
    }
}

/// Writes database header to buffer. Takes uses 31 bytes at the beginning.
pub fn write_database_header(buf: &mut [u8], header: &DatabaseHeader) {
    buf[..4].copy_from_slice(&header.version.to_le_bytes());
    buf[4..6].copy_from_slice(&header.page_size.to_le_bytes());
    buf[6] = header.reserved_space;
    buf[7..11].copy_from_slice(&header.change_counter.to_le_bytes());
    buf[11..15].copy_from_slice(&header.database_size.to_le_bytes());
    buf[15..19].copy_from_slice(&header.freelist_trunk_page.to_le_bytes());
    buf[19..23].copy_from_slice(&header.freelist_pages.to_le_bytes());
    buf[23..27].copy_from_slice(&header.schema_cookie.to_le_bytes());
    buf[27..31].copy_from_slice(&header.default_page_cache_size.to_le_bytes());
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
    buffer: Arc<RefCell<Buffer>>,
    overflow: HashMap<SlotNumber, bool>,
}

impl Page {
    pub fn new(buffer: Arc<RefCell<Buffer>>) -> Self {
        Self {
            buffer,
            overflow: HashMap::new(),
        }
    }

    pub fn alloc(offset: usize, size: usize, drop: Option<DropFn>) -> Self {
        let buf = Buffer::alloc_page(size, drop);
        Self::new(Arc::new(RefCell::new(buf)))
    }

    pub fn as_ptr(&self) -> &mut [u8] {
        let buf = self.buffer.as_ptr();
        unsafe { buf.as_mut().unwrap().as_mut_slice() }
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
        (self.buffer.borrow().size() - self.header_size()) as u16
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
