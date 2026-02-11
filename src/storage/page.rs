use std::{
    cell::UnsafeCell,
    collections::{BinaryHeap, HashMap},
    fmt::Debug,
    io::IoSlice,
    ops::{Bound, RangeBounds},
};

use crate::{
    storage::{self, Error, PageNumber, SLOT_SIZE, SlotNumber},
    utils::{
        buffer::{Buffer, DropFn},
        bytes::{self, BytesCursor, VarInt},
    },
};

pub const DATABASE_HEADER_PAGE_NUMBER: PageNumber = 1;
pub const DATABASE_HEADER_SIZE: usize = 34;

pub const DEFAULT_PAGE_SIZE: u32 = 4096;

const DEFAULT_CACHE_SIZE: u32 = 3000;

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
#[derive(Debug, Clone, Copy)]
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
    pub first_freelist_page: PageNumber,
    /// Total number of freelist pages.
    pub freelist_pages: u32,
    /// Number of `pages` to cache.
    pub default_page_cache_size: u32,
    pub version_valid_for: u32,
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
        let first_freelist_page = u32::from_le_bytes(buffer[18..22].try_into().unwrap());
        let freelist_pages = u32::from_le_bytes(buffer[22..26].try_into().unwrap());
        let default_page_cache_size = u32::from_le_bytes(buffer[26..30].try_into().unwrap());
        let version_valid_for = u32::from_le_bytes(buffer[30..34].try_into().unwrap());

        Self {
            version,
            page_size,
            reserved_space,
            change_counter,
            database_size,
            first_freelist_page,
            freelist_pages,
            default_page_cache_size,
            version_valid_for,
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
        buffer[18..22].copy_from_slice(&self.first_freelist_page.to_le_bytes());
        buffer[22..26].copy_from_slice(&self.freelist_pages.to_le_bytes());
        buffer[26..30].copy_from_slice(&self.default_page_cache_size.to_le_bytes());
        buffer[30..34].copy_from_slice(&self.version_valid_for.to_le_bytes());
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
            first_freelist_page: 0,
            freelist_pages: 0,
            default_page_cache_size: DEFAULT_CACHE_SIZE,
            version_valid_for: 1,
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

    pub fn into_internal(self) -> PageType {
        match self {
            Self::IndexInternal => self,
            Self::IndexLeaf => Self::IndexInternal,
            Self::TableInternal => self,
            Self::TableLeaf => Self::TableInternal,
        }
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
/// | right_sibling     | 4     | 8         | points to right sibling of this page (at the same level in B-Tree).
/// | right_child       | 4     | 12        | present only in Internal Pages. Points to next B-Tree page > current.
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
    const RIGHT_SIBLING_OFFSET: usize = Self::FREE_FRAGMENTS_OFFSET + size_of::<u8>();
    const RIGHT_CHILD_OFFSET: usize = Self::RIGHT_SIBLING_OFFSET + size_of::<PageNumber>();

    pub const HIGH_KEY_SLOT: SlotNumber = 0;
    const FIRST_DATA_KEY: SlotNumber = Self::HIGH_KEY_SLOT + 1;

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

    /// Set defaults
    pub fn initialize(&self, page_type: PageType) {
        self.set_page_type(page_type);
        self.set_first_freeblock(0);
        self.set_len(0);
        self.set_last_used_offset(self.as_ptr().len() as u16);
        self.set_free_fragments(0);
        self.set_right_sibling(0);
        self.set_right_child(0);
    }

    /// Returns reference to whole page buffer.
    pub fn raw(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    /// Returns mutable pointer to page content. Skiping `offset` bytes (only
    /// applied for first db page, when we skip database header).
    pub fn as_ptr(&self) -> &mut [u8] {
        &mut self.buffer.as_mut_slice()[self.offset..]
    }

    /// Returns size of space we can use for cell allocation.
    pub fn usable_space(&self) -> usize {
        self.buffer.size() - self.header_size() - self.offset
    }

    /// Amount of payload that will be stored if cell overflows.
    pub fn min_cell_size(&self) -> usize {
        min_cell_size(self.usable_space())
    }

    /// Max allowed payload that will be stored if cell doesn't overflow.
    pub fn max_cell_size(&self) -> usize {
        max_cell_size(self.usable_space())
    }

    pub fn can_fit_in_overflow(&self) -> usize {
        self.buffer.size() - size_of::<PageNumber>() - size_of::<u16>()
    }

    pub fn total_free_space(&self) -> u16 {
        let total = self.free_space() + self.free_fragments() as u16;

        let freeblocks: u16 = self.freeblock_list().iter().sum();

        total + freeblocks
    }

    pub fn used_space(&self) -> u16 {
        self.usable_space() as u16 - self.total_free_space()
    }

    pub fn as_io_slice(&self) -> IoSlice {
        IoSlice::new(self.buffer.as_slice())
    }
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn is_leaf(&self) -> bool {
        match self.page_type() {
            PageType::IndexLeaf | PageType::TableLeaf => true,
            _ => false,
        }
    }

    pub(super) fn overflow_map(&self) -> &mut HashMap<SlotNumber, BTreeCell> {
        unsafe { self.overflow.get().as_mut().unwrap() }
    }

    pub fn is_overflow(&self) -> bool {
        !self.overflow_map().is_empty()
    }

    pub fn is_underflow(&self) -> bool {
        self.total_free_space() > self.usable_space() as u16 / 2
    }

    /// Depending on `PageType`, this function returns 12 or 16.
    pub fn header_size(&self) -> usize {
        match self.page_type() {
            PageType::IndexInternal | PageType::TableInternal => Self::INTERNAL_PAGE_HEADER_SIZE,
            PageType::IndexLeaf | PageType::TableLeaf => Self::LEAF_PAGE_HEADER_SIZE,
        }
    }

    /// Returns avalible space without header and offset.
    pub fn storage_space(&self) -> u16 {
        (self.buffer.size() - self.header_size() - self.offset) as u16
    }

    /// Free space between slot array and last cell.
    pub fn free_space(&self) -> u16 {
        self.last_used_offset()
            .saturating_sub((self.header_size()) as u16 + self.len() * SLOT_SIZE as SlotNumber)
    }

    pub fn first_data_offset(&self) -> SlotNumber {
        if self.has_high_key() {
            Self::FIRST_DATA_KEY
        } else {
            0
        }
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
        log::trace!("defragment page");
        let mut slots = self.slot_array();

        let mut min_heap = BinaryHeap::from_iter(
            slots
                .slot_array()
                .iter()
                .enumerate()
                .map(|(i, &val)| (val, i)),
        );

        let mut current_offset = self.as_ptr().len();

        while let Some((offset, i)) = min_heap.pop() {
            log::trace!("Offset to cell: {} at index: {}", offset, i);
            log::trace!("Cell: {:?}", self.get_cell(i as SlotNumber));

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
        self.get_cell_at_offset(offset).unwrap().local_size() as u16
    }

    /// Returns cell at given offset.
    ///
    /// # Fails
    ///
    ///  If there is no cell at this offset, this function will return error.
    fn get_cell_at_offset(&self, offset: u16) -> storage::Result<BTreeCellRef> {
        let min_cell_size = min_cell_size(self.usable_space());
        let max_cell_size = max_cell_size(self.usable_space());

        // buf lifetime is change to 'static to avoid later headache. But we need to be carefull with this reference.
        // let page_buffer = unsafe { cast::cast_static(&self.as_ptr()) };

        read_btree_cell_ref(
            self.as_ptr(),
            self.page_type(),
            offset,
            min_cell_size,
            max_cell_size,
            self.usable_space(),
        )
    }

    /// Returns parsed cell at given index.
    ///
    /// # Note
    ///
    /// Usually cell at index **0** is high key cell, so keep in mind this.
    pub fn get_cell<'a>(&'a self, index: SlotNumber) -> storage::Result<BTreeCellRef<'a>> {
        let offset_to_cell = self
            .slot_array()
            .try_get(index)
            .ok_or(storage::Error::CellIndexOutRange)?;

        self.get_cell_at_offset(offset_to_cell)
    }

    pub fn push(&self, cell: BTreeCell) {
        self.insert_cell(self.len(), cell);
    }

    pub fn insert_cell(&self, index: SlotNumber, cell: BTreeCell) {
        assert!(cell.local_size() <= self.max_cell_size(), "can't fit cell");

        if self.is_overflow() {
            self.overflow_map().insert(index, cell);
        } else if let Err(cell) = self.try_insert_cell(index, cell) {
            self.overflow_map().insert(index, cell);
        }
    }

    /// Attempts to insert cell at given index. If cell doesn't fit, it is returned
    /// as a error value. Otherwise slot number of this cell is returned.
    fn try_insert_cell(&self, index: SlotNumber, cell: BTreeCell) -> Result<SlotNumber, BTreeCell> {
        assert!(
            index <= self.count(),
            "Index out of range. Len: {}, index: {}",
            self.len(),
            index
        );

        let cell_size = cell.local_size();
        let storage_cell_size = cell.storage_size();

        let total_free_space = self.total_free_space() as usize;

        if storage_cell_size > total_free_space {
            return Err(cell);
        }

        let mut slot_array = self.slot_array();
        let mut freeblocks = self.freeblock_list();

        let (_, local_payload_size) = cell_overflows(
            cell_size,
            self.min_cell_size(),
            self.max_cell_size(),
            self.usable_space(),
        );

        // look for free space between pages before allocating new one.
        if let Some(offset) = freeblocks.take_freeblock(local_payload_size as u16)
            && self.free_space() >= SLOT_SIZE as u16
        {
            // let page_size = self.as_ptr().len() as u16;
            // if offset >= page_size {
            //     panic!(
            //         "freeblock returned invalid offset {} (page {})",
            //         offset, page_size
            //     );
            // }
            // if offset + cell_size as u16 > page_size {
            //     panic!(
            //         "freeblock offset + cell size out of page bounds: {} + {} > {}",
            //         offset, cell_size, page_size
            //     );
            // }

            write_btree_cell(self.as_ptr(), offset, &cell).expect("writting cell failed");

            slot_array.insert(index, offset);

            return Ok(index);
        }

        // defragment if needed
        if local_payload_size + SLOT_SIZE > self.free_space() as usize {
            self.defragment();
        }

        let offset = self.last_used_offset() - local_payload_size as u16;

        write_btree_cell(self.as_ptr(), offset, &cell).expect("writting cell failed");

        slot_array.insert(index, offset);

        self.set_last_used_offset(offset);

        Ok(index)
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
        let old_cell_size = old_cell.local_size();
        let new_cell_size = new_cell.local_size();

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
            .push_freeblock(offset, removed_cell.local_size() as u16);

        return removed_cell;
    }

    /// Functions like [`Vec::drain`], but removes elements only when consumed.
    ///
    /// # Note
    ///
    /// This function doesn't care about high key, so you have to make sure to
    /// either include it or not.
    pub fn drain(&self, range: impl RangeBounds<SlotNumber>) -> impl Iterator<Item = BTreeCell> {
        let start = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Excluded(i) => i + 1,
            Bound::Included(i) => *i,
        };

        let end = match range.end_bound() {
            Bound::Unbounded => self.len(),
            Bound::Excluded(i) => *i,
            Bound::Included(i) => i + 1,
        };

        let mut drain_index = start;
        let mut slot_index = start;

        std::iter::from_fn(move || {
            if drain_index <= end {
                let cell = self.overflow_map().remove(&drain_index).unwrap_or_else(|| {
                    let cell = self.get_cell(slot_index).unwrap().to_owned();
                    slot_index += 1;
                    cell
                });

                drain_index += 1;

                Some(cell)
            } else {
                let offsets = self.slot_array().drain(start..slot_index);

                for offset in offsets {
                    let size = self.get_cell_at_offset(offset).unwrap().local_size() as u16;
                    self.freeblock_list().push_freeblock(offset, size);
                }

                None
            }
        })
    }

    pub fn child(&self, index: SlotNumber) -> PageNumber {
        assert!(
            matches!(
                self.page_type(),
                PageType::IndexInternal | PageType::TableInternal
            ),
            "Invalid page type."
        );
        if index == self.len() {
            self.try_right_child().unwrap()
        } else {
            match self.get_cell(index).unwrap() {
                BTreeCellRef::IndexInternal(cell) => cell.left_child,
                BTreeCellRef::TableInternal(cell) => cell.left_child,
                _ => unreachable!(),
            }
        }
    }

    pub fn set_child(&self, index: SlotNumber, new_child: PageNumber) {
        assert!(
            matches!(
                self.page_type(),
                PageType::IndexInternal | PageType::TableInternal
            ),
            "Invalid page type."
        );
        if index == self.len() {
            // if index == self.len() {
            self.set_right_child(new_child);
        } else {
            match self.get_cell(index).unwrap() {
                BTreeCellRef::IndexInternal(mut cell) => cell.set_left_child(new_child),
                BTreeCellRef::TableInternal(mut cell) => cell.set_left_child(new_child),
                _ => unreachable!(),
            }
        }
    }

    #[inline]
    pub fn has_high_key(&self) -> bool {
        self.try_right_sibling().is_some()
    }
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

    pub fn set_page_type(&self, value: PageType) {
        self.write_u8(Self::PAGE_TYPE_OFFSET, value as u8);
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

    pub fn try_right_sibling(&self) -> Option<PageNumber> {
        let next = self.read_u32(Self::RIGHT_SIBLING_OFFSET);

        if next != 0 { Some(next) } else { None }
    }

    pub fn set_right_sibling(&self, value: PageNumber) {
        self.write_u32(Self::RIGHT_SIBLING_OFFSET, value);
    }

    /// Returns next `PageNumber` if `PageType` is internal or None if `Page` is Leaf.
    pub fn try_right_child(&self) -> Option<PageNumber> {
        match self.page_type() {
            PageType::IndexInternal | PageType::TableInternal => {
                Some(self.read_u32(Self::RIGHT_CHILD_OFFSET))
            }
            PageType::IndexLeaf | PageType::TableLeaf => None,
        }
    }

    pub fn set_right_child(&self, value: PageNumber) {
        self.write_u32(Self::RIGHT_CHILD_OFFSET, value);
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
        self.len() - self.first_data_offset()
    }
}

// Overflow Pages
impl Page {
    /// Use only when page type is overflow.
    pub fn next_overflow(&self) -> Option<PageNumber> {
        let next = self.read_u32(Self::NEXT_OVERFLOW_OFFSET);

        if next == 0 { None } else { Some(next) }
    }

    pub fn set_next_overflow(&self, value: PageNumber) {
        self.write_u32(Self::NEXT_OVERFLOW_OFFSET, value);
    }

    /// Use only when page type is overflow.
    pub fn overflow_payload_size(&self) -> u16 {
        self.read_u16(Self::PAYLOAD_SIZE_OFFSET)
    }

    pub fn set_overflow_payload_size(&self, value: u16) {
        self.write_u16(Self::PAYLOAD_SIZE_OFFSET, value);
    }

    /// Use only when page type is overflow.
    pub fn overflow_payload(&self) -> &[u8] {
        let start = Self::PAYLOAD_OFFSET;
        let end = start + self.overflow_payload_size() as usize;
        &self.as_ptr()[start..end]
    }

    pub fn set_overflow_payload(&self, payload: &[u8]) {
        assert!(payload.len() == self.overflow_payload_size() as usize);

        let start = 6;
        let end = start + payload.len();

        self.as_ptr()[start..end].copy_from_slice(payload);
    }
}

/// Page with DB header (page number: 1)
impl Page {
    /// Returns database header. Only use with page that have non-zero offset.
    pub fn db_header(&self) -> DatabaseHeader {
        DatabaseHeader::from_bytes(&self.buffer.as_slice()[..self.offset])
    }

    pub fn write_db_header(&self, header: DatabaseHeader) {
        header.write_to_buffer(&mut self.buffer.as_mut_slice()[..self.offset]);
    }
}

/// Only for pages on global freelist
impl Page {
    /// Returns page number of next free page, if exists.
    pub fn freelist_next(&self) -> PageNumber {
        self.read_u32(0)
    }

    pub fn freelist_set(&self, value: PageNumber) {
        self.write_u32(0, value);
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
            .field("right_sibling", &self.try_right_sibling())
            .field("right_child", &self.try_right_child())
            .field("slot_array", &self.slot_array())
            .field(
                "cells",
                &(0..self.count())
                    .map(|i| {
                        let cell = self.get_cell(i).unwrap();
                        cell
                    })
                    .collect::<Vec<BTreeCellRef>>(),
            )
            .field("overflow_cells", self.overflow_map())
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

    pub fn sub_len(&mut self, sub: SlotNumber) {
        let new_len = self.len() - sub;
        self.set_len(new_len);
    }

    pub fn decrement_len(&mut self) {
        self.sub_len(1);
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
        self.try_get(index).unwrap()
    }

    /// Reads slot at given index.
    pub fn try_get(&self, index: SlotNumber) -> Option<u16> {
        let raw = *self.slot_array().get(index as usize)?;
        Some(u16::from_le(raw))
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

    pub fn drain(&mut self, range: impl RangeBounds<SlotNumber>) -> Vec<u16> {
        let start = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Excluded(i) => i + 1,
            Bound::Included(i) => *i,
        } as usize;

        let end = match range.end_bound() {
            Bound::Unbounded => self.len(),
            Bound::Excluded(i) => *i,
            Bound::Included(i) => i + 1,
        } as usize;

        let removed_elements = self.slot_array()[start..end].to_vec();

        if end < self.len() as usize {
            self.slot_array_mut().copy_within(end.., start);
        }

        self.sub_len(removed_elements.len() as SlotNumber);

        removed_elements
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
                    let new_current = current + diff;

                    self.write_u16(current as usize + 2, diff);

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
            let next = self.first_freeblock();
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

pub struct CellIterator<'a> {
    page: &'a Page,
    current: u16,
}

impl<'a> Iterator for CellIterator<'a> {
    type Item = BTreeCellRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let cell = self.page.get_cell(self.current).ok();
        self.current += 1;
        cell
    }
}

pub trait CellOps {
    /// Size of cell including header, but only stored locally (not total).
    fn local_size(&self) -> usize;
    /// Required size to store this cell (cell size + slot number size).
    fn storage_size(&self) -> usize;
    /// Returns reference to whole cell content.
    fn raw(&self) -> &[u8];
    /// Takes buffer and writtes cell content to it.
    /// # Safety
    /// **Cell content is written at the beginning!**
    fn write_to_buffer(&self, buffer: &mut [u8]);
    /// Returns true if cell is overflowing.
    fn is_overflowing(&self) -> bool;
    /// Returns slice to local cell payload. Doesn't including overflow pages.
    fn payload(&self) -> &[u8];
    /// Returns number of overflow page, that is needed during reassembly.
    fn first_overflow(&self) -> Option<PageNumber>;
    /// Returns total size of cell including header and all overflow pages.
    fn payload_size(&self) -> VarInt;
    fn payload_ref(&self) -> PayloadRef;
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

/// Wraps possible types of cell stored in `Page`. On disk each variable is stored in little endian except varints.
#[derive(Debug, Clone)]
pub enum BTreeCell {
    IndexInternal(IndexInternalCell),
    IndexLeaf(IndexLeafCell),
    TableInternal(TableInternalCell),
    TableLeaf(TableLeafCell),
}

impl CellOps for BTreeCell {
    fn local_size(&self) -> usize {
        match self {
            Self::IndexInternal(cell) => cell.local_size(),
            Self::IndexLeaf(cell) => cell.local_size(),
            Self::TableInternal(cell) => cell.local_size(),
            Self::TableLeaf(cell) => cell.local_size(),
        }
    }

    fn storage_size(&self) -> usize {
        match self {
            Self::IndexInternal(cell) => cell.storage_size(),
            Self::IndexLeaf(cell) => cell.storage_size(),
            Self::TableInternal(cell) => cell.storage_size(),
            Self::TableLeaf(cell) => cell.storage_size(),
        }
    }

    fn raw(&self) -> &[u8] {
        match self {
            Self::IndexInternal(cell) => cell.raw(),
            Self::IndexLeaf(cell) => cell.raw(),
            Self::TableInternal(cell) => cell.raw(),
            Self::TableLeaf(cell) => cell.raw(),
        }
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        match self {
            Self::IndexInternal(cell) => cell.write_to_buffer(buffer),
            Self::IndexLeaf(cell) => cell.write_to_buffer(buffer),
            Self::TableInternal(cell) => cell.write_to_buffer(buffer),
            Self::TableLeaf(cell) => cell.write_to_buffer(buffer),
        }
    }

    fn is_overflowing(&self) -> bool {
        match self {
            Self::IndexInternal(cell) => cell.is_overflowing(),
            Self::IndexLeaf(cell) => cell.is_overflowing(),
            Self::TableInternal(cell) => cell.is_overflowing(),
            Self::TableLeaf(cell) => cell.is_overflowing(),
        }
    }

    fn payload(&self) -> &[u8] {
        match self {
            Self::IndexInternal(cell) => cell.payload(),
            Self::IndexLeaf(cell) => cell.payload(),
            Self::TableInternal(cell) => cell.payload(),
            Self::TableLeaf(cell) => cell.payload(),
        }
    }

    fn first_overflow(&self) -> Option<PageNumber> {
        match self {
            Self::IndexInternal(cell) => cell.first_overflow(),
            Self::IndexLeaf(cell) => cell.first_overflow(),
            Self::TableInternal(cell) => cell.first_overflow(),
            Self::TableLeaf(cell) => cell.first_overflow(),
        }
    }

    fn payload_size(&self) -> VarInt {
        match self {
            Self::IndexInternal(cell) => cell.payload_size(),
            Self::IndexLeaf(cell) => cell.payload_size(),
            Self::TableInternal(cell) => cell.payload_size(),
            Self::TableLeaf(cell) => cell.payload_size(),
        }
    }

    fn payload_ref(&self) -> PayloadRef {
        match self {
            Self::IndexInternal(cell) => cell.payload_ref(),
            Self::IndexLeaf(cell) => cell.payload_ref(),
            Self::TableInternal(cell) => cell.payload_ref(),
            Self::TableLeaf(cell) => cell.payload_ref(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexInternalCell {
    /// Pointer to whole cell content. (used for copying cell).
    raw: Vec<u8>,
    /// Left child of BTree Page.
    pub left_child: PageNumber,
    /// Total size of Cell including overflow Pages.
    payload_size: VarInt,
    /// Pointer to content of cell living in Page. It doesn't include overflowing payload.
    payload_ref: PayloadRef,
    /// If cell is overflowing then it points to first overflowing page.
    /// Placed at the end of cell (not in the `payload` field).
    first_overflow: Option<PageNumber>,
}

impl IndexInternalCell {
    /// Creates owned version of cell. Caller must ensure that payload overflow
    /// is handled first, because this function will put everything in single buffer.
    pub fn new(
        left_child: PageNumber,
        payload_size: VarInt,
        payload: &[u8],
        first_overflow: Option<PageNumber>,
    ) -> Self {
        let raw = Vec::new();
        let mut cursor = BytesCursor::new(raw);

        cursor.put_u32_le(left_child);
        cursor.put_varint(payload_size);

        let payload_ref = PayloadRef {
            len: payload.len(),
            offset: cursor.position(),
        };

        cursor.put_bytes(payload);

        if let Some(overflow_page) = first_overflow {
            cursor.put_u32_le(overflow_page);
        }

        Self {
            raw: cursor.into_inner(),
            left_child,
            payload_size,
            payload_ref,
            first_overflow,
        }
    }

    pub fn set_left_child(&mut self, value: PageNumber) {
        self.raw[..size_of::<PageNumber>()].copy_from_slice(&value.to_le_bytes());
        self.left_child = value;
    }

    pub fn payload(&self) -> &[u8] {
        self.payload_ref.as_slice(&self.raw)
    }
}

impl CellOps for IndexInternalCell {
    #[inline]
    fn local_size(&self) -> usize {
        self.raw.len()
    }

    #[inline]
    fn storage_size(&self) -> usize {
        SLOT_SIZE + self.local_size()
    }

    #[inline]
    fn raw(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    #[inline]
    fn is_overflowing(&self) -> bool {
        self.first_overflow.is_some()
    }

    fn payload(&self) -> &[u8] {
        let start = self.payload_ref.offset;
        let end = start + self.payload_size as usize;
        &self.raw()[start..end]
    }

    #[inline]
    fn first_overflow(&self) -> Option<PageNumber> {
        self.first_overflow
    }

    #[inline]
    fn payload_size(&self) -> VarInt {
        self.payload_size
    }

    #[inline]
    fn payload_ref(&self) -> PayloadRef {
        self.payload_ref
    }
}

#[derive(Debug, Clone)]
pub struct IndexLeafCell {
    /// Pointer to whole cell content. (used for copying cell).
    raw: Vec<u8>,
    /// Total size of Cell including overflow Pages.
    payload_size: VarInt,
    /// Pointer to content of cell living in Page. It doesn't include overflowing payload.
    payload_ref: PayloadRef,
    /// If cell is overflowing then it points to first overflowing page.
    /// Placed at the end of cell (not in the `payload` field).
    first_overflow: Option<PageNumber>,
}

impl IndexLeafCell {
    /// Creates owned version of cell. Caller must ensure that payload overflow
    /// is handled first, because this function will put everything in single buffer.
    pub fn new(payload_size: VarInt, payload: &[u8], first_overflow: Option<PageNumber>) -> Self {
        let raw = Vec::new();
        let mut cursor = BytesCursor::new(raw);

        cursor.put_varint(payload_size);

        let payload_ref = PayloadRef {
            len: payload.len(),
            offset: cursor.position(),
        };

        cursor.put_bytes(payload);

        if let Some(overflow_page) = first_overflow {
            cursor.put_u32_le(overflow_page);
        }

        Self {
            raw: cursor.into_inner(),
            payload_size,
            payload_ref,
            first_overflow,
        }
    }

    pub fn into_internal(self, left_child: PageNumber) -> IndexInternalCell {
        IndexInternalCell::new(
            left_child,
            self.payload_size,
            self.payload(),
            self.first_overflow,
        )
    }
}

impl CellOps for IndexLeafCell {
    #[inline]
    fn local_size(&self) -> usize {
        self.raw.len()
    }

    #[inline]
    fn storage_size(&self) -> usize {
        SLOT_SIZE + self.local_size()
    }

    #[inline]
    fn raw(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    #[inline]
    fn is_overflowing(&self) -> bool {
        self.first_overflow.is_some()
    }

    fn payload(&self) -> &[u8] {
        let start = self.payload_ref.offset;
        let end = start + self.payload_size as usize;
        &self.raw()[start..end]
    }

    #[inline]
    fn first_overflow(&self) -> Option<PageNumber> {
        self.first_overflow
    }

    #[inline]
    fn payload_size(&self) -> VarInt {
        self.payload_size
    }

    #[inline]
    fn payload_ref(&self) -> PayloadRef {
        self.payload_ref
    }
}

#[derive(Debug, Clone)]
pub struct TableInternalCell {
    /// Pointer to whole cell content. (used for copying cell).
    raw: Vec<u8>,
    /// Left child of BTree Page.
    pub left_child: PageNumber,
    /// Unique ID of row.
    pub row_id: i64,
}

impl TableInternalCell {
    /// Creates owned version of cell. Caller must ensure that payload overflow
    /// is handled first, because this function will put everything in single buffer.
    pub fn new(row_id: i64, left_child: PageNumber) -> Self {
        let raw = Vec::new();
        let mut cursor = BytesCursor::new(raw);

        cursor.put_u32_le(left_child);
        cursor.put_varint(bytes::zigzag_encode(row_id));

        Self {
            raw: cursor.into_inner(),
            left_child,
            row_id,
        }
    }

    pub fn set_left_child(&mut self, value: PageNumber) {
        self.raw[..size_of::<PageNumber>()].copy_from_slice(&value.to_le_bytes());
        self.left_child = value;
    }
}

impl CellOps for TableInternalCell {
    #[inline]
    fn local_size(&self) -> usize {
        self.raw.len()
    }

    #[inline]
    fn storage_size(&self) -> usize {
        SLOT_SIZE + self.local_size()
    }

    #[inline]
    fn raw(&self) -> &[u8] {
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

    #[inline]
    fn payload(&self) -> &[u8] {
        &self.raw[size_of::<PageNumber>()..]
    }

    #[inline(always)]
    fn first_overflow(&self) -> Option<PageNumber> {
        None
    }

    #[inline]
    fn payload_size(&self) -> VarInt {
        (self.raw.len() - size_of::<PageNumber>()) as VarInt
    }

    #[inline]
    fn payload_ref(&self) -> PayloadRef {
        PayloadRef {
            offset: size_of::<PageNumber>(),
            len: self.payload_size() as usize,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    /// Pointer to whole cell content. (used for copying cell).
    raw: Vec<u8>,
    /// Unique ID of row.
    pub row_id: i64,
    /// Total size of Cell including overflow Pages.
    payload_size: VarInt,
    /// Pointer to content of cell living in Page. It doesn't include overflowing payload.
    payload_ref: PayloadRef,
    /// If cell is overflowing then it points to first overflowing page.
    /// Placed at the end of cell (not in the `payload` field).
    first_overflow: Option<PageNumber>,
}

impl TableLeafCell {
    /// Creates owned version of cell. Caller must ensure that payload overflow
    /// is handled first, because this function will put everything in single buffer.
    pub fn new(
        row_id: i64,
        payload_size: VarInt,
        payload: &[u8],
        first_overflow: Option<PageNumber>,
    ) -> Self {
        let raw = Vec::new();
        let mut cursor = BytesCursor::new(raw);

        cursor.put_varint(bytes::zigzag_encode(row_id));
        cursor.put_varint(payload_size);

        let payload_ref = PayloadRef {
            len: payload.len(),
            offset: cursor.position(),
        };

        cursor.put_bytes(payload);

        if let Some(overflow_page) = first_overflow {
            cursor.put_u32_le(overflow_page);
        }

        Self {
            raw: cursor.into_inner(),
            row_id,
            payload_size,
            payload_ref,
            first_overflow,
        }
    }

    pub fn into_internal(self, left_child: PageNumber) -> TableInternalCell {
        TableInternalCell::new(self.row_id, left_child)
    }
}

impl CellOps for TableLeafCell {
    #[inline]
    fn local_size(&self) -> usize {
        self.raw.len()
    }

    #[inline]
    fn storage_size(&self) -> usize {
        SLOT_SIZE + self.local_size()
    }

    #[inline]
    fn raw(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    #[inline]
    fn is_overflowing(&self) -> bool {
        self.first_overflow.is_some()
    }

    fn payload(&self) -> &[u8] {
        let start = self.payload_ref.offset;
        let end = start + self.payload_size as usize;
        &self.raw()[start..end]
    }

    #[inline]
    fn first_overflow(&self) -> Option<PageNumber> {
        self.first_overflow
    }

    #[inline]
    fn payload_size(&self) -> VarInt {
        self.payload_size
    }

    #[inline]
    fn payload_ref(&self) -> PayloadRef {
        self.payload_ref
    }
}

#[derive(Debug)]
pub enum BTreeCellRef<'a> {
    IndexInternal(IndexInternalCellRef<'a>),
    IndexLeaf(IndexLeafCellRef<'a>),
    TableInternal(TableInternalCellRef<'a>),
    TableLeaf(TableLeafCellRef<'a>),
}

impl<'a> CellOps for BTreeCellRef<'a> {
    fn local_size(&self) -> usize {
        match self {
            Self::IndexInternal(cell) => cell.local_size(),
            Self::IndexLeaf(cell) => cell.local_size(),
            Self::TableInternal(cell) => cell.local_size(),
            Self::TableLeaf(cell) => cell.local_size(),
        }
    }

    fn storage_size(&self) -> usize {
        match self {
            Self::IndexInternal(cell) => cell.storage_size(),
            Self::IndexLeaf(cell) => cell.storage_size(),
            Self::TableInternal(cell) => cell.storage_size(),
            Self::TableLeaf(cell) => cell.storage_size(),
        }
    }

    fn raw(&self) -> &[u8] {
        match self {
            Self::IndexInternal(cell) => cell.raw(),
            Self::IndexLeaf(cell) => cell.raw(),
            Self::TableInternal(cell) => cell.raw(),
            Self::TableLeaf(cell) => cell.raw(),
        }
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        match self {
            Self::IndexInternal(cell) => cell.write_to_buffer(buffer),
            Self::IndexLeaf(cell) => cell.write_to_buffer(buffer),
            Self::TableInternal(cell) => cell.write_to_buffer(buffer),
            Self::TableLeaf(cell) => cell.write_to_buffer(buffer),
        }
    }

    fn is_overflowing(&self) -> bool {
        match self {
            Self::IndexInternal(cell) => cell.is_overflowing(),
            Self::IndexLeaf(cell) => cell.is_overflowing(),
            Self::TableInternal(cell) => cell.is_overflowing(),
            Self::TableLeaf(cell) => cell.is_overflowing(),
        }
    }

    fn payload(&self) -> &[u8] {
        match self {
            Self::IndexInternal(cell) => cell.payload(),
            Self::IndexLeaf(cell) => cell.payload(),
            Self::TableInternal(cell) => cell.payload(),
            Self::TableLeaf(cell) => cell.payload(),
        }
    }

    fn first_overflow(&self) -> Option<PageNumber> {
        match self {
            Self::IndexInternal(cell) => cell.first_overflow(),
            Self::IndexLeaf(cell) => cell.first_overflow(),
            Self::TableInternal(cell) => cell.first_overflow(),
            Self::TableLeaf(cell) => cell.first_overflow(),
        }
    }

    fn payload_size(&self) -> VarInt {
        match self {
            Self::IndexInternal(cell) => cell.payload_size(),
            Self::IndexLeaf(cell) => cell.payload_size(),
            Self::TableInternal(cell) => cell.payload_size(),
            Self::TableLeaf(cell) => cell.payload_size(),
        }
    }

    fn payload_ref(&self) -> PayloadRef {
        match self {
            Self::IndexInternal(cell) => cell.payload_ref(),
            Self::IndexLeaf(cell) => cell.payload_ref(),
            Self::TableInternal(cell) => cell.payload_ref(),
            Self::TableLeaf(cell) => cell.payload_ref(),
        }
    }
}

impl<'a> BTreeCellRef<'a> {
    /// Returns offset of cell in page.
    ///
    /// # Safety
    ///
    /// Use it only when cell is referencing page content.
    /// Otherwise it will return corrupted data.
    pub fn offset_in_page(&self, page_buffer: &[u8]) -> usize {
        self.raw().as_ptr() as usize - page_buffer.as_ptr() as usize
    }

    pub fn to_owned(&self) -> BTreeCell {
        let raw = self.raw().to_vec();

        match self {
            Self::IndexInternal(cell) => BTreeCell::IndexInternal(IndexInternalCell {
                raw,
                left_child: cell.left_child,
                payload_size: cell.payload_size,
                payload_ref: cell.payload_ref,
                first_overflow: cell.first_overflow,
            }),
            Self::IndexLeaf(cell) => BTreeCell::IndexLeaf(IndexLeafCell {
                raw,
                payload_size: cell.payload_size,
                payload_ref: cell.payload_ref,
                first_overflow: cell.first_overflow,
            }),
            Self::TableInternal(cell) => BTreeCell::TableInternal(TableInternalCell {
                raw,
                row_id: cell.row_id,
                left_child: cell.left_child,
            }),
            Self::TableLeaf(cell) => BTreeCell::TableLeaf(TableLeafCell {
                raw,
                row_id: cell.row_id,
                payload_size: cell.payload_size,
                payload_ref: cell.payload_ref,
                first_overflow: cell.first_overflow,
            }),
        }
    }
}

#[derive(Debug)]
pub struct IndexInternalCellRef<'a> {
    raw: &'a [u8],
    pub left_child: PageNumber,
    payload_size: VarInt,
    payload_ref: PayloadRef,
    first_overflow: Option<PageNumber>,
}

impl<'a> IndexInternalCellRef<'a> {
    fn set_left_child(&mut self, value: PageNumber) {
        // SAFETY: we reuse self.raw slice which must be valid and just cast it into mut.
        let raw_mut =
            unsafe { std::slice::from_raw_parts_mut(self.raw.as_ptr().cast_mut(), self.raw.len()) };
        raw_mut[..size_of::<PageNumber>()].copy_from_slice(&value.to_le_bytes());
        self.left_child = value;
    }
}

impl<'a> CellOps for IndexInternalCellRef<'a> {
    #[inline]
    fn local_size(&self) -> usize {
        self.raw.len()
    }

    #[inline]
    fn storage_size(&self) -> usize {
        SLOT_SIZE + self.local_size()
    }

    #[inline]
    fn raw(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    #[inline]
    fn is_overflowing(&self) -> bool {
        self.first_overflow.is_some()
    }

    fn payload(&self) -> &[u8] {
        let start = self.payload_ref.offset;
        let end = start + self.payload_size as usize;
        &self.raw()[start..end]
    }

    #[inline]
    fn first_overflow(&self) -> Option<PageNumber> {
        self.first_overflow
    }

    #[inline]
    fn payload_size(&self) -> VarInt {
        self.payload_size
    }

    #[inline]
    fn payload_ref(&self) -> PayloadRef {
        self.payload_ref
    }
}

#[derive(Debug)]
pub struct IndexLeafCellRef<'a> {
    raw: &'a [u8],
    payload_size: VarInt,
    payload_ref: PayloadRef,
    first_overflow: Option<PageNumber>,
}

impl<'a> CellOps for IndexLeafCellRef<'a> {
    #[inline]
    fn local_size(&self) -> usize {
        self.raw.len()
    }

    #[inline]
    fn storage_size(&self) -> usize {
        SLOT_SIZE + self.local_size()
    }

    #[inline]
    fn raw(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    #[inline]
    fn is_overflowing(&self) -> bool {
        self.first_overflow.is_some()
    }

    fn payload(&self) -> &[u8] {
        let start = self.payload_ref.offset;
        let end = start + self.payload_size as usize;
        &self.raw()[start..end]
    }

    #[inline]
    fn first_overflow(&self) -> Option<PageNumber> {
        self.first_overflow
    }

    #[inline]
    fn payload_size(&self) -> VarInt {
        self.payload_size
    }

    #[inline]
    fn payload_ref(&self) -> PayloadRef {
        self.payload_ref
    }
}

#[derive(Debug)]
pub struct TableInternalCellRef<'a> {
    raw: &'a [u8],
    pub left_child: PageNumber,
    pub row_id: i64,
}

impl<'a> TableInternalCellRef<'a> {
    fn set_left_child(&mut self, value: PageNumber) {
        // SAFETY: we reuse self.raw slice which must be valid and just cast it into mut.
        let raw_mut =
            unsafe { std::slice::from_raw_parts_mut(self.raw.as_ptr().cast_mut(), self.raw.len()) };
        raw_mut[..size_of::<PageNumber>()].copy_from_slice(&value.to_le_bytes());
        self.left_child = value;
    }
}

impl<'a> CellOps for TableInternalCellRef<'a> {
    #[inline]
    fn local_size(&self) -> usize {
        self.raw.len()
    }

    #[inline]
    fn storage_size(&self) -> usize {
        SLOT_SIZE + self.local_size()
    }

    #[inline]
    fn raw(&self) -> &[u8] {
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

    #[inline]
    fn payload(&self) -> &[u8] {
        &self.raw[size_of::<PageNumber>()..]
    }

    #[inline]
    fn first_overflow(&self) -> Option<PageNumber> {
        None
    }

    #[inline]
    fn payload_size(&self) -> VarInt {
        (self.raw.len() - size_of::<PageNumber>()) as VarInt
    }

    #[inline]
    fn payload_ref(&self) -> PayloadRef {
        PayloadRef {
            offset: size_of::<PageNumber>(),
            len: self.payload_size() as usize,
        }
    }
}

#[derive(Debug)]
pub struct TableLeafCellRef<'a> {
    raw: &'a [u8],
    pub row_id: i64,
    payload_size: VarInt,
    payload_ref: PayloadRef,
    first_overflow: Option<PageNumber>,
}

impl<'a> CellOps for TableLeafCellRef<'a> {
    #[inline]
    fn local_size(&self) -> usize {
        self.raw.len()
    }

    #[inline]
    fn storage_size(&self) -> usize {
        SLOT_SIZE + self.local_size()
    }

    #[inline]
    fn raw(&self) -> &[u8] {
        &self.raw
    }

    fn write_to_buffer(&self, buffer: &mut [u8]) {
        let end = self.raw.len();
        buffer[..end].copy_from_slice(&self.raw);
    }

    #[inline]
    fn is_overflowing(&self) -> bool {
        self.first_overflow.is_some()
    }

    fn payload(&self) -> &[u8] {
        let start = self.payload_ref.offset;
        let end = start + self.payload_size as usize;
        &self.raw()[start..end]
    }

    #[inline]
    fn first_overflow(&self) -> Option<PageNumber> {
        self.first_overflow
    }

    #[inline]
    fn payload_size(&self) -> VarInt {
        self.payload_size
    }

    #[inline]
    fn payload_ref(&self) -> PayloadRef {
        self.payload_ref
    }
}

fn read_btree_cell_ref<'a>(
    page_buffer: &'a [u8],
    page_type: PageType,
    cell_offset: u16,
    min_cell_size: usize,
    max_cell_size: usize,
    usable_space: usize,
) -> storage::Result<BTreeCellRef<'a>> {
    let cell_offset = cell_offset as usize;
    let mut cursor = BytesCursor::new(page_buffer);
    cursor.set_position(cell_offset);

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

            let payload_offset_in_page = cursor.position();
            let cell_end = payload_offset_in_page + local_payload_size;

            let payload_offset_in_cell = payload_offset_in_page - cell_offset;
            let page_buffer = cursor.into_inner();

            let raw = &page_buffer[cell_offset..cell_end];
            let payload_region = &raw[payload_offset_in_cell..];

            let (payload_ref, first_overflow) = read_local_payload(
                payload_region,
                payload_offset_in_cell,
                local_payload_size,
                is_overflowing,
            );

            let cell = IndexInternalCellRef {
                raw,
                left_child,
                payload_size,
                payload_ref,
                first_overflow,
            };

            Ok(BTreeCellRef::IndexInternal(cell))
        }
        PageType::IndexLeaf => {
            let (payload_size, _) = cursor.try_read_varint()?;

            let (is_overflowing, local_payload_size) = cell_overflows(
                payload_size as usize,
                min_cell_size,
                max_cell_size,
                usable_space,
            );

            let payload_offset_in_page = cursor.position();
            let cell_end = payload_offset_in_page + local_payload_size;

            let payload_offset_in_cell = payload_offset_in_page - cell_offset;
            let page_buffer = cursor.into_inner();

            let raw = &page_buffer[cell_offset..cell_end];
            let payload_region = &raw[payload_offset_in_cell..];

            let (payload_ref, first_overflow) = read_local_payload(
                payload_region,
                payload_offset_in_cell,
                local_payload_size,
                is_overflowing,
            );

            let cell = IndexLeafCellRef {
                raw,
                payload_size,
                payload_ref,
                first_overflow,
            };

            Ok(BTreeCellRef::IndexLeaf(cell))
        }
        PageType::TableInternal => {
            let left_child = cursor.try_read_u32_le()?;
            let row_id = bytes::zigzag_decode(cursor.try_read_varint()?.0);

            let cell_end = cursor.position();

            let raw = &cursor.into_inner()[cell_offset..cell_end];

            let cell = TableInternalCellRef {
                raw,
                left_child,
                row_id,
            };

            Ok(BTreeCellRef::TableInternal(cell))
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

            let payload_offset_in_page = cursor.position();
            let cell_end = payload_offset_in_page + local_payload_size;

            let payload_offset_in_cell = payload_offset_in_page - cell_offset;
            let page_buffer = cursor.into_inner();

            let raw = &page_buffer[cell_offset..cell_end];
            let payload_region = &raw[payload_offset_in_cell..];

            let (payload_ref, first_overflow) = read_local_payload(
                payload_region,
                payload_offset_in_cell,
                local_payload_size,
                is_overflowing,
            );

            let cell = TableLeafCellRef {
                raw,
                row_id,
                payload_size,
                payload_ref,
                first_overflow,
            };

            Ok(BTreeCellRef::TableLeaf(cell))
        }
    }
}

pub fn write_btree_cell<T: CellOps>(
    page_buffer: &'_ mut [u8],
    offset: u16,
    cell: &T,
) -> storage::Result<()> {
    let position = offset as usize;

    cell.write_to_buffer(&mut page_buffer[position..]);

    Ok(())
}

/// Takes region where cell payload is stored, `payload_size` and `is_overflowing`
/// and returns slice of payload (without next value! ==>) and **if exists, `first_overflow` page pointer**.
pub fn read_local_payload(
    payload_region: &[u8],
    payload_offset: usize,
    local_payload_size: usize,
    is_overflowing: bool,
) -> (PayloadRef, Option<PageNumber>) {
    let mut payload_ref = PayloadRef {
        offset: payload_offset,
        len: 0,
    };
    if is_overflowing {
        let overflow_page = u32::from_le_bytes(
            payload_region[local_payload_size - 4..local_payload_size]
                .try_into()
                .unwrap(),
        );
        payload_ref.len = local_payload_size - 4;
        (payload_ref, Some(overflow_page))
    } else {
        payload_ref.len = local_payload_size;
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

        let owned_cell =
            BTreeCell::IndexInternal(IndexInternalCell::new(123, 6, "Maciek".as_bytes(), None));

        page.insert_cell(0, owned_cell);

        let owned_cell =
            BTreeCell::IndexInternal(IndexInternalCell::new(987, 8, "Kowalski".as_bytes(), None));

        page.insert_cell(1, owned_cell);

        println!(
            "cell offset: {}",
            page.get_cell(1)?.offset_in_page(page.as_ptr())
        );
        println!("payload: {:?}", page.get_cell(1)?.payload());

        let _ = page.remove(0);
        page.defragment();

        println!("cell: {:?}", page.get_cell(0)?);
        println!(
            "cell offset: {}",
            page.get_cell(0)?.offset_in_page(page.as_ptr())
        );

        println!("cell payload: {:?}", page.get_cell(0)?.payload());

        println!("{:?}", page.slot_array());

        for slot in page.slot_array().drain(..) {
            println!("slot: {slot}");
        }

        println!("{:?}", page.slot_array());

        Ok(())
    }
}
