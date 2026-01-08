use std::{
    borrow::Cow,
    cell::UnsafeCell,
    collections::{BinaryHeap, HashMap},
    fmt::Debug,
    io::IoSlice,
};

use crate::{
    sql::{
        record::Record,
        types::{parse_value, serial::SerialType},
    },
    storage::{self, Error, PageNumber, SLOT_SIZE, SlotNumber, btree::BTreeKey, pager::Pager},
    utils::{
        buffer::{Buffer, DropFn},
        bytes::{self, BytesCursor},
        cast,
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

impl PageType {
    pub fn is_leaf(&self) -> bool {
        match self {
            Self::IndexLeaf | Self::TableLeaf => true,
            _ => false,
        }
    }

    pub fn is_internal(&self) -> bool {
        !self.is_leaf()
    }
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
/// Depending on `PageType` header is either 12 (Leaf Pages) or 16 (Internal Pages).
///
/// | Field Name        | Size  | Offset    | Description |
/// | ----------------- | ----- | --------- | ----------- |
/// | page_type         | 1     | 0         | represent PageType.
/// | first_freeblock   | 2     | 1         |  points to start of first freeblock, if set to 0, then there are no freeblocks.
/// | length            | 2     | 3         | number of slots in slot array.
/// | last_used_offset  | 2     | 5         | offset to last used space. Used to calculate if new data can fit in Page.
/// | free_fragments    | 1     | 7         | number of free fragments. Tiny gaps between cells, to small to fit new data.
/// | rigth_sibling     | 4     | 8         | points to rigth sibling of this page (at the same level in B-Tree).
/// | rigth_child       | 4     | 12        | present only in Internal Pages. Points to next B-Tree page > current.
///
/// Total size: 16 bytes (Internal Pages) or 12 bytes (Leaf Pages) bytes long.
///
/// # Overflow:
/// If `Page` contains `Cells` that overflow, it maintains hashmap of slots pointing to overflowing `Cells`.
/// Only chunk of `Cell` is stored in `Page` and it needs to be reassembled by going over
///
pub struct Page {
    /// Offset for something like `DatabaseHeader` so actual page structure
    /// looks like this:
    ///
    /// ```
    /// +-----------------+------------------------------------+
    /// | Some content... | Rest of Page                       |
    /// +-----------------+------------------------------------+
    ///                   ^
    ///                   |
    ///                   +--- offset
    /// ```
    offset: usize,
    /// Buffer that contains page content.
    buffer: Buffer,
    // Map of overflowing cells. Wrapped in `UnsafeCell`
    // because concurrency is managed by pager.
    overflow: UnsafeCell<HashMap<SlotNumber, BTreeCell>>,
}

impl Page {
    const PAGE_TYPE_OFFSET: usize = 0;
    const FIRST_FREEBLOCK_OFFSET: usize = Self::PAGE_TYPE_OFFSET + size_of::<PageType>();
    const LENGTH_OFFSET: usize = Self::FIRST_FREEBLOCK_OFFSET + size_of::<SlotNumber>();
    const LAST_USED_OFFSET: usize = Self::LENGTH_OFFSET + size_of::<SlotNumber>();
    const FREE_FRAGMENTS_OFFSET: usize = Self::LAST_USED_OFFSET + size_of::<SlotNumber>();
    const RIGTH_SIBLING_OFFSET: usize = Self::FREE_FRAGMENTS_OFFSET + size_of::<u8>();
    const RIGTH_CHILD_OFFSET: usize = Self::RIGTH_SIBLING_OFFSET + size_of::<PageNumber>();

    const HIGH_KEY_SLOT: SlotNumber = 0;

    // overflow page
    const NEXT_OVERFLOW_OFFSET: usize = 0;
    const PAYLOAD_SIZE_OFFSET: usize = Self::NEXT_OVERFLOW_OFFSET + size_of::<PageNumber>();
    const PAYLOAD_OFFSET: usize = Self::PAYLOAD_SIZE_OFFSET + size_of::<SlotNumber>();

    // header sizes
    pub const LEAF_PAGE_HEADER_SIZE: usize = 12;
    pub const INTERNAL_PAGE_HEADER_SIZE: usize = 16;

    pub fn new(offset: usize, buffer: Buffer) -> Self {
        Self {
            offset,
            buffer,
            overflow: UnsafeCell::new(HashMap::new()),
        }
    }

    #[cfg(not(debug_assertions))]
    pub fn alloc(size: usize, drop: Option<DropFn>) -> Self {
        let buf = Buffer::alloc_page(size, drop);
        Self::new(0, buf)
    }

    #[cfg(debug_assertions)]
    pub fn alloc(size: usize, drop: Option<DropFn>) -> Self {
        // allow any page size
        let buf = Buffer::alloc(size, drop);
        Self::new(0, buf)
    }

    // #[allow(clippy::mut_from_ref)]
    pub fn as_ptr(&self) -> &mut [u8] {
        &mut self.buffer.as_mut_slice()[self.offset..]
    }

    fn usable_space(&self) -> usize {
        self.buffer.size() - self.header_size()
    }

    /// Amount of payload that will be stored if cell overflows.
    fn min_cell_size(&self) -> usize {
        min_cell_size(self.usable_space())
    }

    /// Max allowed payload that will be stored if cell doesn't overflow.
    fn max_cell_size(&self) -> usize {
        max_cell_size(self.usable_space())
    }

    fn total_free_space(&self) -> u16 {
        let total = self.free_space() + self.free_fragments() as u16;

        let freeblocks: u16 = self.freeblock_list().iter().sum();

        total + freeblocks
    }

    pub fn as_io_slice(&self) -> IoSlice {
        IoSlice::new(self.buffer.as_slice())
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_leaf(&self) -> bool {
        match self.page_type() {
            PageType::IndexLeaf | PageType::TableLeaf => true,
            _ => false,
        }
    }

    fn overflow_map(&self) -> &mut HashMap<SlotNumber, BTreeCell> {
        unsafe { self.overflow.get().as_mut().unwrap() }
    }

    pub fn is_overflow(&self) -> bool {
        !self.overflow_map().is_empty()
    }

    /// Depending on `PageType`, this function returns 12 or 8.
    pub fn header_size(&self) -> usize {
        match self.page_type() {
            PageType::IndexInternal | PageType::TableInternal => Self::INTERNAL_PAGE_HEADER_SIZE,
            PageType::IndexLeaf | PageType::TableLeaf => Self::LEAF_PAGE_HEADER_SIZE,
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

    pub fn slot_array(&self) -> SlotArray {
        SlotArray::new(self.as_ptr(), self.header_size())
    }

    pub fn freeblock_list(&self) -> FreeblockList {
        FreeblockList::new(self.as_ptr())
    }

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
    fn defragment(&self) {
        let mut slots = self.slot_array();

        let mut min_heap = BinaryHeap::from_iter(
            slots
                .slot_array()
                .iter()
                .enumerate()
                .map(|(i, &val)| (val, i)),
        );

        let mut current_offset = self.buffer.size();

        while let Some((offset, i)) = min_heap.pop() {
            let cell_size = self.get_cell_size(offset);

            let start = offset as usize;
            let end = start + cell_size as usize;

            current_offset -= cell_size as usize;

            self.as_ptr().copy_within(start..end, current_offset);

            slots.set(i as u16, current_offset as u16);
        }

        self.set_last_used_offset(current_offset as u16);
        self.set_first_freeblock(0);
    }

    fn get_cell_size(&self, offset: u16) -> u16 {
        let min_cell_size = min_cell_size(self.usable_space());
        let max_cell_size = max_cell_size(self.usable_space());

        local_btree_cell_size(
            &self.as_ptr(),
            self.page_type(),
            offset,
            min_cell_size,
            max_cell_size,
            self.usable_space(),
        )
    }

    /// Returns cell at given offset.
    ///
    /// # Fails
    ///
    ///  If there is no cell at this offset, this function will return error.
    fn get_cell_at_offset(&self, offset: u16) -> storage::Result<BTreeCell> {
        let min_cell_size = min_cell_size(self.usable_space());
        let max_cell_size = max_cell_size(self.usable_space());

        // buf lifetime is change to 'static to avoid later headache. But we need to be carefull with this reference.
        let page_buffer = unsafe { cast::cast_static(&self.as_ptr()) };

        read_btree_cell(
            page_buffer,
            self.page_type(),
            offset,
            min_cell_size,
            max_cell_size,
            self.usable_space(),
        )
    }

    pub fn get_cell(&self, index: SlotNumber) -> storage::Result<BTreeCell> {
        let slot_number = index + if self.has_high_key() { 1 } else { 0 };
        let offset_to_cell = self.slot_array().get(slot_number);

        self.get_cell_at_offset(offset_to_cell)
    }

    pub fn insert_cell(&self, index: SlotNumber, cell: BTreeCell) {
        assert!(
            cell.local_cell_size() <= self.max_cell_size(),
            "can't fit cell"
        );

        if self.is_overflow() {
            self.overflow_map().insert(index, cell);
        } else if let Err(cell) = self.try_insert_cell(index, cell) {
            self.overflow_map().insert(index, cell);
        }
    }

    fn try_insert_cell(&self, index: SlotNumber, cell: BTreeCell) -> Result<SlotNumber, BTreeCell> {
        assert!(index < self.count(), "Index out of range");

        let cell_size = cell.local_cell_size();
        let storage_cell_size = cell_size + SLOT_SIZE;

        let total_free_space = self.total_free_space() as usize;

        if storage_cell_size > total_free_space {
            return Err(cell);
        }

        let physical_slot = index + self.has_high_key() as SlotNumber;

        let mut slot_array = self.slot_array();
        let mut freeblocks = self.freeblock_list();

        let (_, local_payload_size) = cell_overflows(
            cell_size,
            self.min_cell_size(),
            self.max_cell_size(),
            self.usable_space(),
        );

        // look for free space between pages before allocating new one.
        if let Some(offset) = freeblocks.take_freeblock(local_payload_size as u16) {
            write_btree_cell(self.as_ptr(), offset, &cell).expect("writting cell failed");

            slot_array.insert(physical_slot, offset);

            return Ok(physical_slot);
        }

        // defragment if needed
        if local_payload_size + SLOT_SIZE > self.free_space() as usize {
            self.defragment();
        }

        let offset = self.last_used_offset() - local_payload_size as u16;

        write_btree_cell(self.as_ptr(), offset, &cell).expect("writting cell failed");

        slot_array.insert(physical_slot, offset);

        self.set_last_used_offset(offset);

        Ok(physical_slot)
    }

    /// Attempts to replace old cell with new cell at given slot index.
    /// On success, returns old cell that was replaced. Otherwise
    /// returns error and new cell, that wasn't inserted.
    pub fn try_replace(
        &self,
        index: SlotNumber,
        new_cell: BTreeCell,
    ) -> Result<BTreeCell, BTreeCell> {
        let old_cell = self.get_cell(index).unwrap();
        let old_cell_size = old_cell.local_cell_size();
        let new_cell_size = new_cell.local_cell_size();

        let total_free_space = self.total_free_space();

        // there is no way that we can fit this cell so we return it
        if total_free_space as usize + old_cell_size < new_cell_size {
            return Err(new_cell);
        }

        // we can fit new cell in
        if old_cell_size >= new_cell_size {
            let freed_space = old_cell_size - new_cell_size;
            let offset = self.slot_array().get(index) + new_cell_size as u16;

            self.freeblock_list()
                .push_freeblock(offset, freed_space as u16);

            // copy old cell
            let owned_old_cell = old_cell.to_owned();

            let _ = write_btree_cell(self.as_ptr(), offset, &new_cell);

            return Ok(owned_old_cell);
        }

        let owned_old_cell = self.remove(index);

        self.try_insert_cell(index, new_cell)?;

        Ok(owned_old_cell)
    }

    /// Takes index of a cell to remove. Returns owned version of
    /// cell and removes this index from slot array.
    pub fn remove(&self, index: SlotNumber) -> BTreeCell {
        let removed_cell = self.get_cell(index).unwrap().to_owned();
        let offset = self.slot_array().remove(index);

        self.freeblock_list()
            .push_freeblock(offset, removed_cell.local_cell_size() as u16);

        return removed_cell;
    }

    pub fn child(&self, index: SlotNumber) -> PageNumber {
        assert!(
            matches!(
                self.page_type(),
                PageType::IndexInternal | PageType::TableInternal
            ),
            "Invalid page type."
        );
        if index == self.count() {
            self.try_rigth_child().unwrap()
        } else {
            match self.get_cell(index).unwrap() {
                BTreeCell::IndexInternalCell(c) => c.left_child,
                BTreeCell::TableInternalCell(c) => c.left_child,
                _ => unreachable!(),
            }
        }
    }

    #[inline]
    fn has_high_key(&self) -> bool {
        self.try_rigth_sibling().is_some()
    }

    pub fn high_key(&self, pager: &mut Pager) -> storage::Result<Option<BTreeKey<'_>>> {
        if !self.has_high_key() {
            return Ok(None);
        }

        // High key is always at slot 0
        let offset = self.slot_array().get(Self::HIGH_KEY_SLOT);
        let cell = self.get_cell_at_offset(offset)?;

        // SAFETY: key borrows from page, transmute to extend lifetime to page's lifetime
        let key = unsafe { std::mem::transmute(cell.key(pager)?) };

        Ok(Some(key))
    }

    pub fn key_in_range(&self, pager: &mut Pager, key: &BTreeKey) -> storage::Result<bool> {
        if let Some(high_key) = self.high_key(pager)? {
            Ok(*key <= high_key)
        } else {
            Ok(true)
        }
    }

    // /// Set the high key (insert as first cell)
    // pub fn set_high_key(&self, pager: &mut Pager, key: BTreeKey<'static>) -> storage::Result<()> {
    //     if !self.has_high_key() {
    //         return Err(StorageError::InvalidOperation("Page has no right sibling"));
    //     }

    //     // Create a minimal separator cell
    //     let cell = create_separator_cell(key, self.page_type())?;

    //     // If high key already exists, replace it
    //     if self.len() > 0 {
    //         // Remove old high key at slot 0
    //         let old_offset = self.slot_array().get(Self::HIGH_KEY_SLOT);
    //         let old_cell = self.get_cell_at_offset(old_offset)?;

    //         self.slot_array().remove(Self::HIGH_KEY_SLOT);

    //         // Mark old cell space as free
    //         self.freeblock_list()
    //             .push_freeblock(old_offset, old_cell.local_cell_size() as u16);
    //     }

    //     // Insert new high key at slot 0
    //     self.insert_cell(Self::HIGH_KEY_SLOT, cell);

    //     Ok(())
    // }
}

impl Page {
    fn read_u8(&self, pos: usize) -> u8 {
        self.as_ptr()[pos]
    }

    fn read_u16(&self, pos: usize) -> u16 {
        let buf = self.as_ptr();
        u16::from_le_bytes([buf[pos], buf[pos + 1]])
    }

    fn read_u32(&self, pos: usize) -> u32 {
        let buf = self.as_ptr();
        u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]])
    }

    fn write_u8(&self, pos: usize, value: u8) {
        let buf = self.as_ptr();
        buf[pos] = value;
    }

    fn write_u16(&self, pos: usize, value: u16) {
        let buf = self.as_ptr();
        buf[pos..pos + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn write_u32(&self, pos: usize, value: u32) {
        let buf = self.as_ptr();
        buf[pos..pos + 4].copy_from_slice(&value.to_le_bytes());
    }
}

impl Page {
    pub fn try_page_type(&self) -> Option<PageType> {
        self.read_u8(Self::PAGE_TYPE_OFFSET).try_into().ok()
    }

    pub fn page_type(&self) -> PageType {
        self.try_page_type().unwrap()
    }

    pub fn first_freeblock(&self) -> u16 {
        self.read_u16(Self::FIRST_FREEBLOCK_OFFSET)
    }

    pub fn set_first_freeblock(&self, value: u16) {
        self.write_u16(Self::FIRST_FREEBLOCK_OFFSET, value);
    }

    pub fn last_used_offset(&self) -> u16 {
        self.read_u16(Self::LAST_USED_OFFSET)
    }

    pub fn set_last_used_offset(&self, value: u16) {
        self.write_u16(Self::LAST_USED_OFFSET, value);
    }

    pub fn free_fragments(&self) -> u8 {
        self.read_u8(Self::FREE_FRAGMENTS_OFFSET)
    }

    pub fn set_free_fragments(&self, value: u8) {
        self.write_u8(Self::FREE_FRAGMENTS_OFFSET, value)
    }

    pub fn add_free_fragment(&self, value: u8) {
        self.set_free_fragments(self.free_fragments() + value)
    }

    // pub fn try_high_key_offset(&self) -> Option<u16> {
    //     let offset = self.read_u16(Self::HIGH_KEY_OFFSET);
    //     if offset != 0 { Some(offset) } else { None }
    // }

    // pub fn set_high_key_offset(&self, value: u16) {
    //     self.write_u16(Self::HIGH_KEY_OFFSET, value);
    // }

    pub fn try_rigth_sibling(&self) -> Option<PageNumber> {
        let next = self.read_u32(Self::RIGTH_SIBLING_OFFSET);

        if next != 0 { Some(next) } else { None }
    }

    /// Returns next `PageNumber` if `PageType` is internal or None if `Page` is Leaf.
    pub fn try_rigth_child(&self) -> Option<PageNumber> {
        match self.page_type() {
            PageType::IndexInternal | PageType::TableInternal => {
                Some(self.read_u32(Self::RIGTH_CHILD_OFFSET))
            }
            PageType::IndexLeaf | PageType::TableLeaf => None,
        }
    }

    /// Returns number of slots in `Page`. Includes high key cell.
    pub fn len(&self) -> u16 {
        self.read_u16(Self::LENGTH_OFFSET)
    }

    pub fn set_len(&self, value: u16) {
        self.write_u16(Self::LENGTH_OFFSET, value);
    }

    /// Returns number of slots in `Page`. Doesn't include high key cell.
    pub fn count(&self) -> u16 {
        self.len() - if self.has_high_key() { 1 } else { 0 }
    }
}

// Overflow Pages
impl Page {
    /// Use only when page type is overflow.
    pub fn next_overflow(&self) -> Option<PageNumber> {
        let next = self.read_u32(Self::NEXT_OVERFLOW_OFFSET);

        if next == 0 { None } else { Some(next) }
    }

    /// Use only when page type is overflow.
    pub fn overflow_payload_size(&self) -> u16 {
        self.read_u16(Self::PAYLOAD_SIZE_OFFSET)
    }

    /// Use only when page type is overflow.
    pub fn overflow_payload(&self) -> &[u8] {
        let start = Self::PAYLOAD_OFFSET;
        let end = start + self.overflow_payload_size() as usize;
        &self.as_ptr()[start..end]
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("page_type", &self.page_type())
            .field("first_freeblock", &self.first_freeblock())
            .field("num_slots", &self.len())
            .field("last_used_offset", &self.last_used_offset())
            .field("num_free_fragments", &self.free_fragments())
            .field("rigth_child", &self.try_rigth_child())
            .field("slot_array", &self.slot_array())
            .field(
                "cells",
                &(0..self.len())
                    .map(|i| {
                        let cell = self.get_cell(i).unwrap();
                        cell
                    })
                    .collect::<Vec<BTreeCell>>(),
            )
            .finish()
    }
}

/// View to slot array to make operations on it easier.
pub struct SlotArray<'a> {
    // Pointer to beginning of page content.
    mem: &'a mut [u8],
    /// Offset to slot array.
    slot_array_offset: usize,
}

impl<'a> SlotArray<'a> {
    pub fn new(mem: &'a mut [u8], slot_array_offset: usize) -> Self {
        Self {
            mem,
            slot_array_offset,
        }
    }

    fn read_u16(&self, pos: usize) -> u16 {
        u16::from_le_bytes([self.mem[pos], self.mem[pos + 1]])
    }

    fn write_u16(&mut self, pos: usize, value: u16) {
        self.mem[pos..pos + 2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn len(&self) -> u16 {
        self.read_u16(Page::LENGTH_OFFSET)
    }

    pub fn set_len(&mut self, value: u16) {
        self.write_u16(Page::LENGTH_OFFSET, value);
    }

    pub fn increment_len(&mut self) {
        let new_len = self.len() + 1;
        self.set_len(new_len);
    }

    pub fn decrement_len(&mut self) {
        let new_len = self.len() - 1;
        self.set_len(new_len);
    }

    #[inline]
    fn slot_array(&self) -> &[u16] {
        let beginning = self.slot_array_offset;
        let end = beginning + self.len() as usize * SLOT_SIZE;

        let slot_array_region = &self.mem[beginning..end];
        crate::utils::cast::cast_slice(slot_array_region)
    }

    #[inline]
    fn slot_array_mut(&mut self) -> &mut [u16] {
        let beginning = self.slot_array_offset;
        let end = beginning + self.len() as usize * SLOT_SIZE;

        let slot_array_region = &mut self.mem[beginning..end];
        crate::utils::cast::cast_slice_mut(slot_array_region)
    }

    /// Reads slot at given index.
    pub fn get(&self, index: SlotNumber) -> u16 {
        assert!(index < self.len(), "Index out of range");

        let raw = self.slot_array()[index as usize];
        u16::from_le(raw)
    }

    /// Overwrites given slot with new value.
    pub fn set(&mut self, index: SlotNumber, value: u16) {
        assert!(index < self.len(), "Index out of range");

        self.slot_array_mut()[index as usize] = value.to_le();
    }

    /// Inserts new slot into array and extends length of it.
    ///
    /// # Safety
    ///
    /// You need to ensure that array can grow without any problems, because
    /// it would otherwise overwrite some memory.
    pub fn insert(&mut self, index: SlotNumber, value: u16) {
        assert!(index <= self.len(), "Index out of range");
        self.increment_len();

        // if index isn't the last one, shift all slots to right
        if index < self.len() {
            let end = self.len() as usize - 1;
            self.slot_array_mut()
                .copy_within(index as usize..end, index as usize + 1);
        }

        self.slot_array_mut()[index as usize] = value.to_le();
    }

    pub fn push(&mut self, value: u16) {
        self.insert(self.len(), value);
    }

    pub fn remove(&mut self, index: SlotNumber) -> u16 {
        assert!(index < self.len(), "Index out of range");

        let removed_slot = self.get(index);

        let start = index as usize + 1;
        let end = self.len() as usize;

        self.slot_array_mut()
            .copy_within(start..end, index as usize);

        self.decrement_len();

        removed_slot
    }
}

impl Debug for SlotArray<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotArray")
            .field("len", &self.len())
            .field("slots", &self.slot_array())
            .finish()
    }
}

pub struct FreeblockList<'a> {
    /// Mutable reference to whole page content.
    mem: &'a mut [u8],
}

impl<'a> FreeblockList<'a> {
    pub fn new(mem: &'a mut [u8]) -> Self {
        Self { mem }
    }

    fn read_u8(&self, pos: usize) -> u8 {
        self.mem[pos]
    }

    fn read_u16(&self, pos: usize) -> u16 {
        u16::from_le_bytes([self.mem[pos], self.mem[pos + 1]])
    }

    fn write_u8(&mut self, pos: usize, value: u8) {
        self.mem[pos] = value;
    }

    fn write_u16(&mut self, pos: usize, value: u16) {
        self.mem[pos..pos + 2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn first_freeblock(&self) -> u16 {
        self.read_u16(Page::FIRST_FREEBLOCK_OFFSET)
    }

    pub fn set_first_freeblock(&mut self, value: u16) {
        self.write_u16(Page::FIRST_FREEBLOCK_OFFSET, value);
    }

    pub fn free_fragments(&self) -> u8 {
        self.read_u8(Page::FREE_FRAGMENTS_OFFSET)
    }

    pub fn set_free_fragments(&mut self, value: u8) {
        self.write_u8(Page::FREE_FRAGMENTS_OFFSET, value)
    }

    pub fn add_free_fragment(&mut self, value: u8) {
        self.set_free_fragments(self.free_fragments() + value)
    }

    /// Returns freeblock at given offset (offset to next freeblock, size of current freeblock).
    fn get_freeblock(&self, offset: u16) -> (u16, u16) {
        let next_freeblock = self.read_u16(offset as usize);
        let freeblock_size = self.read_u16(offset as usize + 2);
        (next_freeblock, freeblock_size)
    }

    fn set_freeblock(&mut self, offset: u16, next: u16, size: u16) {
        self.write_u16(offset as usize, next);
        self.write_u16(offset as usize + 2, size);
    }

    // Looks for freeblock that can fit given `cell_size`. If there is one, it
    // will return offset to it, otherwise it will return None. Also it manages
    // slpitting freeblocks, so there is no need to calculate fragmentation.
    fn take_freeblock(&mut self, cell_size: u16) -> Option<u16> {
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

    /// Adds new freeblock to beginning of freeblocks linked-list.
    fn push_freeblock(&mut self, offset: u16, size: u16) {
        if size < 4 {
            self.add_free_fragment(size as u8);
        }

        let first_freeblock = self.first_freeblock();

        if first_freeblock == 0 {
            self.set_freeblock(offset, 0, size);
        } else {
            let (next, _) = self.get_freeblock(first_freeblock);
            self.set_freeblock(offset, next, size);
        }
        self.set_first_freeblock(offset);
    }

    /// Returns iterator over freeblock sizes. Usefull for calculating total free space.
    fn iter(&self) -> FreeblockIterator<'_> {
        FreeblockIterator {
            freeblocks: self,
            current: self.first_freeblock(),
        }
    }
}

struct FreeblockIterator<'a> {
    freeblocks: &'a FreeblockList<'a>,
    current: u16,
}

impl<'a> Iterator for FreeblockIterator<'a> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == 0 {
            return None;
        }

        let (next, current_size) = self.freeblocks.get_freeblock(self.current);
        self.current = next;

        Some(current_size)
    }
}

/// Wraps possible types of cell stored in `Page`. On disk each variable is stored in little endian except varints.
#[derive(Debug)]
pub enum BTreeCell {
    IndexInternalCell(IndexInternalCell),
    IndexLeafCell(IndexLeafCell),
    TableInternalCell(TableInternalCell),
    TableLeafCell(TableLeafCell),
}

impl BTreeCell {
    // pub fn dummy_cell(key: BTreeKey<'_>, page_type: PageType) -> storage::Result<Self> {
    //     match (key, page_type) {
    //         (BTreeKey::TableRowId((row_id, _)), PageType::TableInternal) => {
    //             // Create minimal table internal cell (row_id + dummy left_child)
    //             Ok(BTreeCell::TableInternalCell(TableInternalCell {
    //                 left_child: 0,
    //                 row_id
    //             }))
    //         }
    //         (BTreeKey::IndexKey(record), PageType::IndexInternal) => {
    //             // Create minimal index internal cell (record + dummy left_child)
    //             Ok(BTreeCell::IndexInternalCell(IndexInternalCell::new(
    //                 record,
    //                 PageNumber::from(0),
    //             )))
    //         }
    //         _ => Err(storage::Error::InvalidPageType),
    //     }
    // }

    /// Returns local size of cell. **No overflow pages included**.
    pub fn local_cell_size(&self) -> usize {
        match self {
            Self::IndexInternalCell(cell) => cell.local_storage_size(),
            Self::IndexLeafCell(cell) => cell.local_storage_size(),
            Self::TableInternalCell(cell) => cell.local_storage_size(),
            Self::TableLeafCell(cell) => cell.local_storage_size(),
        }
    }

    pub fn as_ref(&self) -> &[u8] {
        match self {
            Self::IndexInternalCell(cell) => cell.as_ref(),
            Self::IndexLeafCell(cell) => cell.as_ref(),
            Self::TableInternalCell(cell) => cell.as_ref(),
            Self::TableLeafCell(cell) => cell.as_ref(),
        }
    }

    pub fn is_overflowing(&self) -> bool {
        match self {
            Self::IndexInternalCell(cell) => cell.is_overflowing(),
            Self::IndexLeafCell(cell) => cell.is_overflowing(),
            Self::TableInternalCell(cell) => cell.is_overflowing(),
            Self::TableLeafCell(cell) => cell.is_overflowing(),
        }
    }

    pub fn local_payload(&self) -> &[u8] {
        match self {
            Self::IndexInternalCell(cell) => cell.payload(),
            Self::IndexLeafCell(cell) => cell.payload(),
            Self::TableInternalCell(cell) => cell.payload(),
            Self::TableLeafCell(cell) => cell.payload(),
        }
    }

    /// Returns whole cell payload including overflow pages. In best case it
    /// just returns borrowed local payload, but if needed it will reassemble
    /// cell content.
    pub fn whole_payload(&self, pager: &mut Pager) -> storage::Result<Cow<'_, [u8]>> {
        reassemble_payload(pager, self)
    }

    /// Returns B-tree key from this cell. Pager is needed, because payload
    /// migth be reassembled.
    fn key(&self, pager: &mut Pager) -> storage::Result<BTreeKey<'_>> {
        let reassembled_cell = reassemble_payload(pager, self)?;

        match self {
            Self::IndexInternalCell(_) => {
                let record = Record::new(reassembled_cell);
                Ok(BTreeKey::new_index_key(record))
            }
            Self::IndexLeafCell(_) => {
                let record = Record::new(reassembled_cell);
                Ok(BTreeKey::new_index_key(record))
            }
            Self::TableInternalCell(cell) => Ok(BTreeKey::new_table_row_id(cell.row_id, None)),
            Self::TableLeafCell(cell) => {
                let record = Record::new(reassembled_cell);
                Ok(BTreeKey::new_table_row_id(cell.row_id, Some(record)))
            }
        }
    }
}

impl ToOwned for BTreeCell {
    type Owned = BTreeCell;

    fn to_owned(&self) -> Self::Owned {
        let owned: Cow<'static, [u8]> = Cow::Owned(self.as_ref().to_vec());

        let cell = match self {
            BTreeCell::IndexInternalCell(c) => {
                let index_internal_cell = IndexInternalCell {
                    raw: owned,
                    left_child: c.left_child,
                    payload_size: c.payload_size,
                    payload_ref: c.payload_ref,
                    first_overflow: c.first_overflow,
                };

                BTreeCell::IndexInternalCell(index_internal_cell)
            }
            BTreeCell::IndexLeafCell(c) => {
                let index_leaf_cell = IndexLeafCell {
                    raw: owned,
                    payload_size: c.payload_size,
                    payload_ref: c.payload_ref,
                    first_overflow: c.first_overflow,
                };

                BTreeCell::IndexLeafCell(index_leaf_cell)
            }
            BTreeCell::TableInternalCell(c) => {
                let table_internal_cell = TableInternalCell {
                    raw: owned,
                    row_id: c.row_id,
                    left_child: c.left_child,
                };

                BTreeCell::TableInternalCell(table_internal_cell)
            }
            BTreeCell::TableLeafCell(c) => {
                let table_leaf_cell = TableLeafCell {
                    raw: owned,
                    row_id: c.row_id,
                    payload_size: c.payload_size,
                    payload_ref: c.payload_ref,
                    first_overflow: c.first_overflow,
                };

                BTreeCell::TableLeafCell(table_leaf_cell)
            }
        };

        cell
    }
}

pub trait CellOps {
    /// Size of cell including header, but only stored locally (not total)
    fn local_storage_size(&self) -> usize;
    /// Returns reference to whole cell content.
    fn as_ref(&self) -> &[u8];
    /// Takes buffer and writtes cell content to it.
    /// # Safety
    /// **Cell content is written at the beginning!**
    fn write_to_buffer(&self, buffer: &mut [u8]);
    /// Returns true if cell is overflowing.
    fn is_overflowing(&self) -> bool;
    /// Returns slice to whole cell payload including overflow pages.
    ///
    /// # Safety
    ///
    /// If cell overflows, then it needs to be reassembled first.
    fn payload(&self) -> &[u8];
}

#[derive(Debug, Clone, Copy)]
pub struct PayloadRef {
    offset: usize,
    len: usize,
}

impl PayloadRef {
    pub fn as_slice<'a>(&self, raw: &'a [u8]) -> &'a [u8] {
        &raw[self.offset..self.offset + self.len]
    }
}

#[derive(Debug)]
pub struct IndexInternalCell {
    /// Pointer to whole cell content. (used for copying cell).
    raw: Cow<'static, [u8]>,
    /// Left child of BTree Page.
    pub left_child: PageNumber,
    /// Total size of Cell including overflow Pages.
    payload_size: u64,
    /// Pointer to content of cell living in Page. It doesn't include overflowing payload.
    payload_ref: PayloadRef,
    /// If cell is overflowing then it points to first overflowing page.
    /// Placed at the end of cell (not in the `payload` field).
    first_overflow: Option<PageNumber>,
}

impl IndexInternalCell {
    pub fn payload(&self) -> &[u8] {
        self.payload_ref.as_slice(&self.raw)
    }
}

impl CellOps for IndexInternalCell {
    fn local_storage_size(&self) -> usize {
        self.raw.len()
    }

    fn as_ref(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    fn is_overflowing(&self) -> bool {
        self.first_overflow.is_some()
    }

    fn payload(&self) -> &[u8] {
        let start = self.payload_ref.offset;
        let end = start + self.payload_size as usize;
        &self.as_ref()[start..end]
    }
}

#[derive(Debug)]
pub struct IndexLeafCell {
    /// Pointer to whole cell content. (used for copying cell).
    raw: Cow<'static, [u8]>,
    /// Total size of Cell including overflow Pages.
    payload_size: u64,
    /// Pointer to content of cell living in Page. It doesn't include overflowing payload.
    payload_ref: PayloadRef,
    /// If cell is overflowing then it points to first overflowing page.
    /// Placed at the end of cell (not in the `payload` field).
    first_overflow: Option<PageNumber>,
}

impl IndexLeafCell {
    pub fn payload(&self) -> &[u8] {
        self.payload_ref.as_slice(&self.raw)
    }
}

impl CellOps for IndexLeafCell {
    fn local_storage_size(&self) -> usize {
        self.raw.len()
    }

    fn as_ref(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    fn is_overflowing(&self) -> bool {
        self.first_overflow.is_some()
    }

    fn payload(&self) -> &[u8] {
        let start = self.payload_ref.offset;
        let end = start + self.payload_size as usize;
        &self.as_ref()[start..end]
    }
}

#[derive(Debug)]
pub struct TableInternalCell {
    /// Pointer to whole cell content. (used for copying cell).
    raw: Cow<'static, [u8]>,
    /// Unique ID of row.
    row_id: i64,
    /// Left child of BTree Page.
    pub left_child: PageNumber,
}

impl CellOps for TableInternalCell {
    fn local_storage_size(&self) -> usize {
        self.raw.len()
    }

    fn as_ref(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    #[inline(always)]
    fn is_overflowing(&self) -> bool {
        false
    }

    fn payload(&self) -> &[u8] {
        &self.raw[..self.raw.len() - size_of::<PageNumber>()]
    }
}

#[derive(Debug)]
pub struct TableLeafCell {
    /// Pointer to whole cell content. (used for copying cell).
    raw: Cow<'static, [u8]>,
    /// Unique ID of row.
    row_id: i64,
    /// Total size of Cell including overflow Pages.
    payload_size: u64,
    /// Pointer to content of cell living in Page. It doesn't include overflowing payload.
    payload_ref: PayloadRef,
    /// If cell is overflowing then it points to first overflowing page.
    /// Placed at the end of cell (not in the `payload` field).
    first_overflow: Option<PageNumber>,
}

impl TableLeafCell {
    pub fn payload(&self) -> &[u8] {
        self.payload_ref.as_slice(&self.raw)
    }
}

impl CellOps for TableLeafCell {
    fn local_storage_size(&self) -> usize {
        self.raw.len()
    }

    fn as_ref(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    fn is_overflowing(&self) -> bool {
        self.first_overflow.is_some()
    }

    fn payload(&self) -> &[u8] {
        let start = self.payload_ref.offset;
        let end = start + self.payload_size as usize;
        &self.as_ref()[start..end]
    }
}

pub fn local_btree_cell_size(
    page_buffer: &'_ [u8],
    page_type: PageType,
    offset: u16,
    min_cell_size: usize,
    max_cell_size: usize,
    usable_space: usize,
) -> u16 {
    let mut cursor = BytesCursor::new(page_buffer);
    cursor.set_position(offset as usize);

    let mut size = 0;

    match page_type {
        PageType::IndexInternal => {
            let _ = cursor.read_u32_le();
            size += size_of::<PageNumber>();

            // variable size, not payload size
            let (payload_size, n) = cursor.read_varint();
            size += n;

            let (_, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            size += local_payload_size;
        }
        PageType::IndexLeaf => {
            // variable size, not payload size
            let (payload_size, n) = cursor.read_varint();
            size += n;

            let (_, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            size += local_payload_size;
        }
        PageType::TableInternal => {
            // variable size, not row_id size
            let (_, n) = cursor.read_varint();
            size += n;

            size += size_of::<PageNumber>();
        }
        PageType::TableLeaf => {
            // variable size, not row_id size
            let (_, n) = cursor.read_varint();
            size += n;

            // variable size, not payload size
            let (payload_size, n) = cursor.read_varint();
            size += n;

            let (_, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            size += local_payload_size;
        }
    }

    size as u16
}

/// Creates `BTreeCell` based on provided `PageType`. `offset` param is offset
/// to beginning of given cell.
pub fn read_btree_cell(
    page_buffer: &'static [u8],
    page_type: PageType,
    offset: u16,
    min_cell_size: usize,
    max_cell_size: usize,
    usable_space: usize,
) -> storage::Result<BTreeCell> {
    let mut cursor = BytesCursor::new(page_buffer);
    cursor.set_position(offset as usize);

    match page_type {
        PageType::IndexInternal => {
            let left_child = cursor.try_read_u32_le()?;
            let (payload_size, _) = cursor.try_read_varint()?;

            let (is_overflowing, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            let start = cursor.position();
            let end = start + local_payload_size;

            let raw_buffer = cursor.into_inner();

            let raw_cell = &raw_buffer[offset as usize..end];
            let payload_buffer = &raw_buffer[start..end];

            let (payload_ref, first_overflow) =
                read_local_payload(start, payload_buffer, local_payload_size, is_overflowing);

            let cell = IndexInternalCell {
                raw: Cow::Borrowed(raw_cell),
                left_child,
                payload_size,
                payload_ref,
                first_overflow,
            };

            Ok(BTreeCell::IndexInternalCell(cell))
        }
        PageType::IndexLeaf => {
            let (payload_size, _) = cursor.try_read_varint()?;

            let (is_overflowing, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            let start = cursor.position();
            let end = start + local_payload_size;

            let raw_buffer = cursor.into_inner();

            let raw_cell = &raw_buffer[offset as usize..end];
            let payload_buffer = &raw_buffer[start..end];

            let (payload_ref, first_overflow) =
                read_local_payload(start, payload_buffer, local_payload_size, is_overflowing);

            let cell = IndexLeafCell {
                raw: Cow::Borrowed(raw_cell),
                payload_size,
                payload_ref,
                first_overflow,
            };

            Ok(BTreeCell::IndexLeafCell(cell))
        }
        PageType::TableInternal => {
            let row_id = bytes::zigzag_decode(cursor.try_read_varint()?.0);
            let left_child = cursor.try_read_u32_le()?;

            let end = cursor.position();
            let raw_cell = &cursor.into_inner()[offset as usize..end];

            let cell = TableInternalCell {
                raw: Cow::Borrowed(raw_cell),
                row_id,
                left_child,
            };
            Ok(BTreeCell::TableInternalCell(cell))
        }
        PageType::TableLeaf => {
            let row_id = bytes::zigzag_decode(cursor.try_read_varint()?.0);
            let (payload_size, _) = cursor.try_read_varint()?;

            let (is_overflowing, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            let start = cursor.position();
            let end = start + local_payload_size;

            let raw_buffer = cursor.into_inner();

            let raw_cell = &raw_buffer[offset as usize..end];
            let payload_buffer = &raw_buffer[start..end];

            let (payload_ref, first_overflow) =
                read_local_payload(start, payload_buffer, local_payload_size, is_overflowing);

            let cell = TableLeafCell {
                raw: Cow::Borrowed(raw_cell),
                row_id,
                payload_size,
                payload_ref,
                first_overflow,
            };
            Ok(BTreeCell::TableLeafCell(cell))
        }
    }
}

pub fn write_btree_cell(
    page_buffer: &'_ mut [u8],
    offset: u16,
    cell: &BTreeCell,
) -> storage::Result<()> {
    let position = offset as usize;

    match cell {
        BTreeCell::IndexInternalCell(c) => c.write_to_buffer(&mut page_buffer[position..]),
        BTreeCell::IndexLeafCell(c) => c.write_to_buffer(&mut page_buffer[position..]),
        BTreeCell::TableInternalCell(c) => c.write_to_buffer(&mut page_buffer[position..]),
        BTreeCell::TableLeafCell(c) => c.write_to_buffer(&mut page_buffer[position..]),
    }

    Ok(())
}

/// Takes region where cell payload is stored, `payload_size` and `is_overflowing`
/// and returns slice of payload (without next value! ==>) and **if exists, `first_overflow` page pointer**.
pub fn read_local_payload(
    offset: usize,
    payload_region: &'_ [u8],
    payload_size: usize,
    is_overflowing: bool,
) -> (PayloadRef, Option<PageNumber>) {
    let mut payload_ref = PayloadRef { offset, len: 0 };
    if is_overflowing {
        let overflow_page = u32::from_le_bytes([
            payload_region[payload_size - 1],
            payload_region[payload_size - 2],
            payload_region[payload_size - 3],
            payload_region[payload_size - 4],
        ]);
        payload_ref.len = payload_size - 4;
        (payload_ref, Some(overflow_page))
    } else {
        payload_ref.len = payload_size;
        (payload_ref, None)
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

/// Returns whole cell payload including overflow pages. Takes mutable reference
/// to pager and cell that you want to reassemble. Returns [Cow], which is
/// `Borrowed` when cell isn't overflowing and `Owned` when cell needed to be
/// reconstructed. Returned slice can be directly used as record.
pub fn reassemble_payload<'a>(
    pager: &mut Pager,
    cell: &'a BTreeCell,
) -> storage::Result<Cow<'a, [u8]>> {
    if !cell.is_overflowing() {
        return Ok(Cow::Borrowed(cell.local_payload()));
    }

    let (first_overflow, total_size, local_payload) = match cell {
        BTreeCell::IndexInternalCell(c) => (
            c.first_overflow.unwrap(),
            c.payload_size as usize,
            c.payload(),
        ),
        BTreeCell::IndexLeafCell(c) => (
            c.first_overflow.unwrap(),
            c.payload_size as usize,
            c.payload(),
        ),
        BTreeCell::TableLeafCell(c) => (
            c.first_overflow.unwrap(),
            c.payload_size as usize,
            c.payload(),
        ),
        BTreeCell::TableInternalCell(_) => {
            // Internal cells don't overflow
            return Ok(Cow::Borrowed(cell.local_payload()));
        }
    };

    let mut result = Vec::with_capacity(total_size);

    result.extend_from_slice(local_payload);

    let mut current_overflow = first_overflow;

    while result.len() < total_size {
        let overflow_page = pager.read_page(current_overflow)?;
        let guard = overflow_page.lock_shared();

        result.extend_from_slice(guard.overflow_payload());

        if let Some(next) = guard.next_overflow() {
            current_overflow = next;
        } else {
            break;
        }
    }

    if result.len() != total_size {
        return Err(storage::Error::Corruped);
    }

    Ok(Cow::Owned(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_max() -> anyhow::Result<()> {
        let page = Page::alloc(4096, None);
        page.as_ptr()[0] = 5;

        // println!("Min cell size: {}", page.min_cell_size());
        // println!("Max cell size: {}", page.max_cell_size());

        let mut slot_array = page.slot_array();
        let mut sample = Vec::new();

        slot_array.push(100);
        slot_array.push(200);
        slot_array.push(500);

        sample.push(100);
        sample.push(200);
        sample.push(500);

        slot_array.insert(1, 300);
        sample.insert(1, 300);

        println!("{:?}", slot_array);

        assert!(slot_array.len() as usize == sample.len());
        assert!(slot_array.slot_array() == &sample);

        page.defragment();

        Ok(())
    }

    #[test]
    fn test_page() -> anyhow::Result<()> {
        let page = Page::alloc(512, None);
        page.as_ptr()[0] = 5;
        page.set_last_used_offset(page.buffer.size() as u16);

        let mut raw_cell_content = vec![];

        let left_child: PageNumber = 10;
        let payload_size = 20;
        let payload = &[1; 20];
        // let first_overflow: Option<PageNumber> = Some(5);

        raw_cell_content.extend_from_slice(&left_child.to_le_bytes());

        raw_cell_content.extend_from_slice(&bytes::encode_to_varint(payload_size));

        let start = raw_cell_content.len();
        raw_cell_content.extend_from_slice(payload);

        let cell = BTreeCell::IndexInternalCell(IndexInternalCell {
            raw: Cow::Owned(raw_cell_content),
            left_child,
            payload_size,
            payload_ref: PayloadRef {
                offset: start,
                len: payload.len(),
            },
            first_overflow: None,
        });

        page.insert_cell(0, cell);

        println!("{:#?}", page);

        println!("{:?}", page.get_cell(0));

        let cell_2 = page.get_cell(0).unwrap().to_owned();

        page.insert_cell(1, cell_2);

        println!("{:#?}", page);
        println!("{}", page.total_free_space());

        println!("{:#?}", page.remove(0));

        Ok(())
    }
}
