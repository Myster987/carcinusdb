use std::{borrow::Cow, cell::RefCell, cmp::Ordering, collections::VecDeque, rc::Rc, sync::Arc};

use crate::{
    sql::record::{Record, compare_records, records_equal},
    storage::{
        self, PageNumber, SlotNumber,
        page::{
            BTreeCell, BTreeCellRef, CellOps, IndexInternalCell, IndexLeafCell, Page, PageType,
            TableInternalCell, TableLeafCell, cell_overflows,
        },
        pager::Pager,
        wal::transaction::{ReadTx, WriteTx},
    },
    utils::bytes::VarInt,
};

#[derive(Debug)]
pub enum BTreeKey<'a> {
    TableKey((i64, Option<Record<'a>>)),
    IndexKey(Record<'a>),
}

impl<'a> BTreeKey<'a> {
    pub fn new_table_key(row_id: i64, record: Option<Record<'a>>) -> Self {
        Self::TableKey((row_id, record))
    }

    pub fn new_index_key(record: Record<'a>) -> Self {
        Self::IndexKey(record)
    }

    #[inline]
    pub fn is_index(&self) -> bool {
        match self {
            Self::IndexKey(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_table(&self) -> bool {
        !self.is_index()
    }

    pub fn get_record(&self) -> Option<Record<'a>> {
        match self {
            Self::TableKey((_, record)) => record.clone(),
            Self::IndexKey(record) => Some(record.clone()),
        }
    }

    /// Returns row id of this B-tree key.
    ///
    /// # Safety
    ///
    /// Caller must ensure that this is valid table key.
    pub fn row_id(&self) -> i64 {
        match self {
            Self::TableKey((row_id, _)) => *row_id,
            _ => panic!("Shouldn't be called on index keys."),
        }
    }

    pub fn to_owned(&self) -> BTreeKey<'static> {
        match self {
            Self::TableKey((row_id, record)) => {
                BTreeKey::TableKey((*row_id, record.as_ref().map(|r| r.to_owned())))
            }
            Self::IndexKey(record) => BTreeKey::IndexKey(record.to_owned()),
        }
    }
}

impl PartialEq for BTreeKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::TableKey((id1, _)), Self::TableKey((id2, _))) => id1 == id2,
            (Self::IndexKey(r1), Self::IndexKey(r2)) => records_equal(r1, r2),
            _ => false, // Different key types are never equal
        }
    }
}

impl Eq for BTreeKey<'_> {}

impl PartialOrd for BTreeKey<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BTreeKey<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Table keys: compare only by row_id
            (Self::TableKey((id1, _)), Self::TableKey((id2, _))) => id1.cmp(id2),

            // Index keys: compare records lexicographically
            (Self::IndexKey(r1), Self::IndexKey(r2)) => compare_records(r1, r2),

            // Different types: table keys < index keys (arbitrary but consistent)
            (Self::TableKey(_), Self::IndexKey(_)) => Ordering::Less,
            (Self::IndexKey(_), Self::TableKey(_)) => Ordering::Greater,
        }
    }
}

#[derive(Debug)]
pub enum SearchResult {
    Found { page: PageNumber, slot: SlotNumber },
    NotFound { page: PageNumber, slot: SlotNumber },
}

impl SearchResult {
    pub fn is_found(&self) -> bool {
        match self {
            SearchResult::Found { page: _, slot: _ } => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub enum BTreeType {
    Table,
    Index,
}

pub trait DatabaseCursor {
    /// Traverses B-tree in order to find value that mathes given key.
    /// Returns position of found entry or where it should be inserted.
    fn seek(&mut self, key: &BTreeKey) -> storage::Result<SearchResult>;

    /// Postions cursor to leftmost page starting from current position.
    fn seek_first(&mut self) -> storage::Result<bool>;
    /// Returns true if cursor advanced in postion or false, if we reached
    /// end of btree.
    fn next(&mut self) -> storage::Result<bool>;

    // /// Attempts to extract key at current cursor position. By design cursor
    // /// should hold guard to page that it's currently on. Without this protection
    // /// key could be moved out durring balancing.
    // fn key(&mut self) -> storage::Result<BTreeKey<'_>>;

    // /// Extracts record from current position of cursor. This function also
    // /// depends on cursor holding page guard in advance.
    // fn value(&mut self) -> storage::Result<Option<Record<'_>>>;

    // /// The same as `DatabaseCursor::key` but returns owned data.
    // fn key_owned(&mut self) -> storage::Result<BTreeKey<'static>>;
    // /// The same as `DatabaseCursor::value` but returns owned data.
    // fn value_owned(&mut self) -> storage::Result<Option<Record<'static>>>;

    /// Returns record at current cursor position.
    fn try_record(&self) -> storage::Result<Record<'static>>;

    /// Returns current page and slot that cursor is on.
    fn position(&self) -> (PageNumber, SlotNumber);
}

pub struct BTreeCursor<'tx, Tx> {
    tx: Rc<RefCell<Tx>>,
    pager: &'tx Arc<Pager>,
    root: PageNumber,
    current_page: PageNumber,
    current_slot: SlotNumber,
    path_stack: Vec<(PageNumber, SlotNumber)>,
    init: bool,
    done: bool,
}

impl<'tx, Tx: ReadTx> BTreeCursor<'tx, Tx> {
    pub fn new(tx: Rc<RefCell<Tx>>, pager: &'tx Arc<Pager>, root: PageNumber) -> Self {
        Self {
            tx,
            pager,
            root,
            current_page: root,
            current_slot: 0,
            path_stack: Vec::new(),
            init: false,
            done: false,
        }
    }

    /// Performs binary search on this page looking for given key. When exact
    /// match is found, it returns slot number of cell that contained this key.
    /// Otherwise returns slot where key is supposed to be.
    fn binary_search(
        &mut self,
        page: &Page,
        search_key: &BTreeKey,
    ) -> storage::Result<Result<SlotNumber, SlotNumber>> {
        let first = page.first_data_offset();
        let count = page.count();

        let mut left = 0;
        let mut rigth = count;

        while left < rigth {
            let mid = left + (rigth - left) / 2;
            let slot = first + mid;

            let key = self.extracty_key(page, slot)?;

            match search_key.cmp(&key) {
                Ordering::Less => rigth = mid,
                Ordering::Equal => return Ok(Ok(slot)),
                Ordering::Greater => left = mid + 1,
            }
        }

        Ok(Err(first + left))
    }

    /// Check if search operation needs to visit rigth sibling. This can happen
    /// when page was splited and key we are looking for is there.
    pub fn key_in_range(&self, page: &Page, key: &BTreeKey) -> storage::Result<bool> {
        if let Some(high_key) = self.extract_high_key(page)? {
            Ok(*key < high_key)
        } else {
            Ok(true)
        }
    }

    /// Returns page high key, if it exists. Otherwise it returns `Ok(None)`.
    fn extract_high_key<'a>(&self, page: &'a Page) -> storage::Result<Option<BTreeKey<'a>>> {
        if !page.has_high_key() {
            return Ok(None);
        }

        return self.extracty_key(page, Page::HIGH_KEY_SLOT).map(Some);
    }

    /// Takes page reference and returns `BTreeKey` based on page type. Cell
    /// migth need reassembly.
    fn extracty_key<'a>(&self, page: &'a Page, index: SlotNumber) -> storage::Result<BTreeKey<'a>> {
        if index >= page.len() {
            return Err(storage::Error::CellIndexOutRange);
        }

        let cell = page.get_cell(index)?;
        let reassembled_payload = self.reassemble_payload(page, index)?;

        match cell {
            BTreeCellRef::IndexInternal(_) => {
                let record = Record::new(reassembled_payload);
                Ok(BTreeKey::new_index_key(record))
            }
            BTreeCellRef::IndexLeaf(_) => {
                let record = Record::new(reassembled_payload);
                Ok(BTreeKey::new_index_key(record))
            }
            BTreeCellRef::TableInternal(cell) => Ok(BTreeKey::new_table_key(cell.row_id, None)),
            BTreeCellRef::TableLeaf(cell) => {
                let record = Record::new(reassembled_payload);
                Ok(BTreeKey::new_table_key(cell.row_id, Some(record)))
            }
        }
    }

    /// Returns whole cell payload including overflow pages. Takes mutable reference
    /// to pager and cell that you want to reassemble. Returns [Cow], which is
    /// `Borrowed` when cell isn't overflowing and `Owned` when cell needed to be
    /// reconstructed. Returned slice can be directly used as record.
    pub fn reassemble_payload<'a>(
        &self,
        page: &'a Page,
        index: SlotNumber,
    ) -> storage::Result<Cow<'a, [u8]>> {
        let cell = page.get_cell(index)?;
        let offset_to_cell = cell.offset_in_page(page.as_ptr());

        let local_payload = cell
            .payload_ref()
            .as_slice(&page.as_ptr()[offset_to_cell..]);

        if !cell.is_overflowing() {
            return Ok(Cow::Borrowed(local_payload));
        }

        let first_overflow = cell.first_overflow().unwrap();
        let total_size = cell.payload_size() as usize;

        let mut result = Vec::with_capacity(total_size);

        result.extend_from_slice(local_payload);

        let mut current_overflow = first_overflow;

        while result.len() < total_size {
            let overflow_page = self.pager.read_page(&*self.tx.borrow(), current_overflow)?;
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
}

impl<'tx, Tx: ReadTx> DatabaseCursor for BTreeCursor<'tx, Tx> {
    fn seek(&mut self, key: &BTreeKey) -> storage::Result<SearchResult> {
        // start at root
        self.current_page = self.root;

        loop {
            let page = self
                .pager
                .read_page(&*self.tx.borrow(), self.current_page)?;
            let guard = page.lock_shared();

            // we need to move to rigth node
            if !self.key_in_range(&guard, key)? {
                self.current_page = guard
                    .try_right_sibling()
                    .expect("page was splited and should contain sibling");
                continue;
            }

            let search_result = self.binary_search(&guard, key)?;

            if let Ok(slot) = search_result {
                // mark that we can search for data from here
                self.done = false;

                return Ok(SearchResult::Found {
                    page: self.current_page,
                    slot,
                });
            }
            if guard.is_leaf() {
                // even if page is doesn't match it could be used for range scans
                // at least I think it can :D
                self.done = false;

                return Ok(SearchResult::NotFound {
                    page: self.current_page,
                    slot: search_result.unwrap_err(),
                });
            }

            let child_slot = search_result.unwrap();

            self.path_stack.push((self.current_page, child_slot));
            // go to child which may contain searched key
            self.current_page = guard.child(child_slot);
        }
    }

    fn seek_first(&mut self) -> storage::Result<bool> {
        loop {
            let page = self
                .pager
                .read_page(&*self.tx.borrow(), self.current_page)?;
            let guard = page.lock_shared();

            if guard.is_leaf() {
                self.current_slot = guard.first_data_offset();

                // page is empty, so iteration doesn't make any sense.
                if guard.is_empty() {
                    self.done = true;
                }

                break;
            }

            self.current_page = guard.child(guard.first_data_offset());
        }

        Ok(true)
    }

    fn next(&mut self) -> storage::Result<bool> {
        if self.done {
            return Ok(false);
        }

        // cursor needs to position itself.
        if !self.init {
            self.seek_first()?;
            self.init = true;
            // return if there are cells present.
            return Ok(!self.done);
        }

        if let Ok(current_page) = self.pager.read_page(&*self.tx.borrow(), self.current_page) {
            let page = current_page.lock_shared();

            if page.is_empty() {
                self.done = true;
                return Ok(false);
            }

            // check if there are still cells that haven't been visited. we must
            // use page.len(), because current_slot often starts at 1.
            if self.current_slot < page.len() {
                self.current_slot += 1;
                return Ok(true);
            }

            if let Some(rigth_sibling) = page.try_right_sibling() {
                let next_page = self.pager.read_page(&*self.tx.borrow(), rigth_sibling)?;
                let guard = next_page.lock_shared();

                self.current_slot = guard.first_data_offset();
                self.current_page = rigth_sibling;

                return Ok(true);
            }
        }

        // end of iteration
        Ok(false)
    }

    fn try_record(&self) -> storage::Result<Record<'static>> {
        let page = self
            .pager
            .read_page(&*self.tx.borrow(), self.current_page)?;
        let guard = page.lock_shared();

        let key = self.extracty_key(&guard, self.current_slot)?;

        key.get_record()
            .ok_or(storage::Error::InvalidPageType)
            .map(|r| r.to_owned())
    }

    fn position(&self) -> (PageNumber, SlotNumber) {
        (self.current_page, self.current_slot)
    }
}

impl<'tx, Tx: WriteTx> BTreeCursor<'tx, Tx> {
    const BALANCE_SIBLINGS_PER_SIDE: usize = 3;

    pub fn insert(&mut self, entry: BTreeKey<'_>) -> storage::Result<()> {
        let search_result = self.seek(&entry)?;

        match search_result {
            SearchResult::Found { page: _, slot: _ } => Err(storage::Error::DuplicateKey),
            SearchResult::NotFound { page: _, slot: _ } => self.try_insert_into_leaf(entry),
        }
    }

    fn try_insert_into_leaf(&mut self, entry: BTreeKey<'_>) -> storage::Result<()> {
        let page = self
            .pager
            .read_page(&*self.tx.borrow(), self.current_page)?;
        let guard = page.lock_exclusive();

        // // page is empty (only in case of root page)
        // if !guard.has_high_key() {
        //     let high_key = self.build_high_key(&guard, &entry)?;
        //     guard.insert_cell(0, high_key);
        //     self.current_slot += 1
        // }

        let cell = self.build_cell(&guard, entry)?;

        guard.insert_cell(self.current_slot, cell);

        self.pager.add_dirty(&page);

        Ok(())
    }

    fn build_high_key(&mut self, page: &Page, entry: &BTreeKey<'_>) -> storage::Result<BTreeCell> {
        let page_type = page.page_type();

        if matches!(page_type, PageType::TableInternal | PageType::TableLeaf) {
            let cell = match page_type {
                PageType::TableInternal => {
                    BTreeCell::TableInternal(TableInternalCell::new(entry.row_id(), 0))
                }
                PageType::TableLeaf => {
                    BTreeCell::TableLeaf(TableLeafCell::new(entry.row_id(), 0, &[], None))
                }
                _ => unreachable!(),
            };

            return Ok(cell);
        }

        let record = entry
            .get_record()
            .expect("Entry should contain record in order to be inserted");

        let payload = record.raw();
        let payload_size = payload.len();

        let (is_overflowing, local_payload_size) = cell_overflows(
            payload_size,
            page.min_cell_size(),
            page.max_cell_size(),
            page.usable_space(),
        );

        let first_overflow = if is_overflowing {
            self.build_overflow_chain(page, payload, local_payload_size)?
        } else {
            None
        };

        let local_payload = &payload[..local_payload_size];

        let cell = match page_type {
            PageType::IndexInternal => BTreeCell::IndexInternal(IndexInternalCell::new(
                0,
                payload_size as VarInt,
                local_payload,
                first_overflow,
            )),
            PageType::IndexLeaf => BTreeCell::IndexLeaf(IndexLeafCell::new(
                payload_size as VarInt,
                local_payload,
                first_overflow,
            )),
            _ => unreachable!(),
        };

        Ok(cell)
    }

    fn build_cell(&mut self, page: &Page, entry: BTreeKey<'_>) -> storage::Result<BTreeCell> {
        let record = entry
            .get_record()
            .expect("Entry should contain record in order to be inserted");

        let payload = record.raw();
        let payload_size = payload.len();

        let (is_overflowing, local_payload_size) = cell_overflows(
            payload_size,
            page.min_cell_size(),
            page.max_cell_size(),
            page.usable_space(),
        );

        let first_overflow = if is_overflowing {
            self.build_overflow_chain(page, payload, local_payload_size)?
        } else {
            None
        };

        let local_payload = &payload[..local_payload_size];

        let cell = match entry {
            BTreeKey::IndexKey(_) => BTreeCell::IndexLeaf(IndexLeafCell::new(
                payload_size as VarInt,
                local_payload,
                first_overflow,
            )),
            BTreeKey::TableKey(_) => BTreeCell::TableLeaf(TableLeafCell::new(
                entry.row_id(),
                payload_size as VarInt,
                payload,
                first_overflow,
            )),
        };

        Ok(cell)
    }

    fn build_overflow_chain(
        &mut self,
        page: &Page,
        payload: &[u8],
        local_payload_size: usize,
    ) -> storage::Result<Option<PageNumber>> {
        let payload_size = payload.len();

        // build linked list of overflow pages by going backwards, from end to start.
        let overflow_payload = &payload[local_payload_size..];

        let mut overflows = payload_size - local_payload_size;

        let max_overflow_size_per_page = page.can_fit_in_overflow();

        let mut prev_overflow_page = 0;

        while overflows > 0 {
            let overflow_page_number = self.pager.alloc_empty_page(&mut *self.tx.borrow_mut())?;
            let overflow_page = self
                .pager
                .read_page(&*self.tx.borrow(), overflow_page_number)?;
            let overflow_page_guard = overflow_page.lock_exclusive();

            overflow_page_guard.set_next_overflow(prev_overflow_page);

            let mut current_content = overflows % max_overflow_size_per_page;

            if current_content == 0 {
                current_content = max_overflow_size_per_page;
            }

            overflow_page_guard.set_overflow_payload_size(current_content as u16);

            let payload = &overflow_payload[overflows - current_content..overflows];

            overflow_page_guard.set_overflow_payload(payload);
            overflows -= current_content;

            prev_overflow_page = overflow_page_number;
        }

        Ok(Some(prev_overflow_page))
    }

    fn balance(&mut self) -> storage::Result<()> {
        let current_page = self
            .pager
            .read_page(&*self.tx.borrow(), self.current_page)?;
        let current_page_guard = current_page.lock_exclusive();

        let is_root = self.current_page == self.root;

        let is_underflow =
            current_page_guard.is_empty() || !is_root && current_page_guard.is_underflow();

        // tree is balanced.
        if !current_page_guard.is_overflow() && !is_underflow {
            return Ok(());
        }

        // // root is empty.
        // if is_root && is_underflow {
        //     // root is the only page, so we can't do anything.
        //     if current_page_guard.is_leaf() {
        //         return Ok(());
        //     }

        //     let child_page = current_page_guard.try_rigth_child().unwrap();
        //     let
        // }

        if current_page_guard.is_overflow() {
            let new_page = self
                .pager
                .alloc_page(&mut *self.tx.borrow_mut(), current_page_guard.page_type())?;

            let mut cells: VecDeque<_> = current_page_guard.drain(..).collect();

            let cell_sizes: Vec<_> = cells.iter().map(|c| c.local_size()).collect();

            let (left_high_key, right_high_key, split_index) = calculate_split_ratio(
                &cell_sizes,
                current_page_guard.raw().len(),
                current_page_guard.has_high_key(),
            );

            let right_high_key = right_high_key.map(|right| cells[right].clone());
            let left_high_key = left_high_key.map(|left| cells[left].clone());

            let rigth = cells.split_off(split_index);

            // remove old high key
            let _ = cells.pop_front();
            {
                let new_page = self.pager.read_page(&*self.tx.borrow(), new_page)?;
                let new_page_guard = new_page.lock_exclusive();

                new_page_guard.insert_cell(0, left_high_key.unwrap());

                let mut current_index = 1;

                for cell in rigth {
                    new_page_guard.insert_cell(current_index, cell);
                    current_index += 1;
                }

                new_page_guard.set_right_sibling(self.current_page);

                self.pager.add_dirty(&new_page);
            }

            todo!()
        }

        Ok(())
    }

    // fn load_siblings(&mut self, page_number: PageNumber, parent_page: PageNumber) -> Vec<(PageNumber, SlotNumber)> {

    // }
}

type CellSplit = (Option<usize>, Vec<usize>);

fn calculate_split_ratio(
    cell_sizes: &[usize],
    split_threshold: usize,
    has_high_key: bool,
) -> (Option<usize>, Option<usize>, usize) {
    let cell_sizes = cell_sizes.iter().copied();

    let mut running = 0;

    let mut right_high_key = None;
    let mut split_index = 0;

    for (i, size) in cell_sizes.enumerate() {
        if has_high_key && right_high_key.is_none() {
            right_high_key = Some(i);
            continue;
        }

        if running >= split_threshold {
            split_index = i;
            break;
        }

        running += size;
    }

    let left_high_key = Some(split_index);

    (left_high_key, right_high_key, split_index)
}

#[cfg(test)]
mod tests {
    use crate::storage::page;

    use super::*;

    #[test]
    fn test_split_ratio() {
        let usable_space = page::DEFAULT_PAGE_SIZE as usize;
        let split_threshold = usable_space / 2;
        let has_high_key = true;

        let min_cell_size = page::min_cell_size(usable_space);
        let max_cell_size = page::max_cell_size(usable_space);

        let mut cell_sizes = Vec::new();
        let mut total = 0;

        if has_high_key {
            cell_sizes.push(rand::random_range(min_cell_size..max_cell_size));
        }

        while total < usable_space {
            let size = rand::random_range(min_cell_size..max_cell_size);
            total += size;
            cell_sizes.push(size);
        }

        let ratio = calculate_split_ratio(&cell_sizes, split_threshold, has_high_key);

        println!("min: {min_cell_size}, max: {max_cell_size}");
        println!("cells: {:?}", cell_sizes);
        println!("split threshold: {split_threshold}");
        println!("{:?}", ratio);
    }
}
