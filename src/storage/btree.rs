use std::{borrow::Cow, cell::RefCell, cmp::Ordering, rc::Rc, sync::Arc};

use crate::{
    sql::record::{Record, compare_records, records_equal},
    storage::{
        self, PageNumber, SlotNumber,
        page::{
            BTreeCell, BTreeCellRef, CellOps, IndexLeafCell, Page, TableLeafCell, cell_overflows,
        },
        pager::{Pager, SharedPageGuard},
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

    /// Attempts to extract key at current cursor position. By design cursor
    /// should hold guard to page that it's currently on. Without this protection
    /// key could be moved out durring balancing.
    fn key(&mut self) -> storage::Result<BTreeKey<'_>>;

    /// Extracts record from current position of cursor. This function also
    /// depends on cursor holding page guard in advance.
    fn value(&mut self) -> storage::Result<Option<Record<'_>>>;

    /// The same as `DatabaseCursor::key` but returns owned data.
    fn key_owned(&mut self) -> storage::Result<BTreeKey<'static>>;
    /// The same as `DatabaseCursor::value` but returns owned data.
    fn value_owned(&mut self) -> storage::Result<Option<Record<'static>>>;

    /// Returns current page and slot that cursor is on.
    fn position(&self) -> (PageNumber, SlotNumber);
}

pub struct BTreeCursor<'tx, Tx> {
    tx: Rc<RefCell<Tx>>,
    pager: &'tx Arc<Pager>,
    root: PageNumber,
    page_guard: Option<SharedPageGuard>,
    current_page: PageNumber,
    current_slot: SlotNumber,
    done: bool,
}

impl<'tx, Tx: ReadTx> BTreeCursor<'tx, Tx> {
    pub fn new(tx: Rc<RefCell<Tx>>, pager: &'tx Arc<Pager>, root: PageNumber) -> Self {
        Self {
            tx,
            pager,
            root,
            page_guard: None,
            current_page: root,
            current_slot: 0,
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
            Ok(*key <= high_key)
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
                    .try_rigth_sibling()
                    .expect("page was splited and should contain sibling");
                continue;
            }

            let search_result = self.binary_search(&guard, key)?;

            if let Ok(slot) = search_result {
                self.page_guard = Some(guard);
                // mark that we can search for data from here
                self.done = false;

                return Ok(SearchResult::Found {
                    page: self.current_page,
                    slot,
                });
            }
            if guard.is_leaf() {
                self.page_guard = Some(guard);
                // even if page is doesn't match it could be used for range scans
                // at least I think it can :D
                self.done = false;

                return Ok(SearchResult::NotFound {
                    page: self.current_page,
                    slot: search_result.unwrap_err(),
                });
            }

            // go to child which may contain searched key
            self.current_page = guard.child(search_result.unwrap_err());
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

                self.page_guard = Some(guard);
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
        if self.page_guard.is_none() {
            self.seek_first()?;
            // return if there are cells present.
            return Ok(!self.done);
        }

        if let Some(page) = self.page_guard.as_ref() {
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

            if let Some(rigth_sibling) = page.try_rigth_sibling() {
                let next_page = self.pager.read_page(&*self.tx.borrow(), rigth_sibling)?;
                let guard = next_page.lock_shared();

                self.current_slot = guard.first_data_offset();
                self.current_page = rigth_sibling;

                self.page_guard = Some(guard);

                return Ok(true);
            }
        }

        // end of iteration
        Ok(false)
    }

    fn key(&mut self) -> storage::Result<BTreeKey<'_>> {
        let page = self
            .page_guard
            .as_ref()
            .ok_or(storage::Error::InvalidPageType)?;
        self.extracty_key(page, self.current_slot)
    }

    fn value(&mut self) -> storage::Result<Option<Record<'_>>> {
        self.key().map(|key| key.get_record())
    }

    fn key_owned(&mut self) -> storage::Result<BTreeKey<'static>> {
        self.key().map(|key| key.to_owned())
    }

    fn value_owned(&mut self) -> storage::Result<Option<Record<'static>>> {
        self.value().map(|val| val.map(|record| record.to_owned()))
    }

    fn position(&self) -> (PageNumber, SlotNumber) {
        (self.current_page, self.current_slot)
    }
}

impl<'tx, Tx: WriteTx> BTreeCursor<'tx, Tx> {
    pub fn insert_leaf(&mut self, entry: BTreeKey<'_>) -> storage::Result<()> {
        let record = entry
            .get_record()
            .expect("Entry should contain record in order to be inserted");

        let payload = record.raw();
        let payload_size = payload.len();

        let page = self
            .pager
            .read_page(&*self.tx.borrow(), self.current_page)?;
        let guard = page.lock_exclusive();

        let (is_overflowing, local_payload_size) = cell_overflows(
            payload_size,
            guard.min_cell_size(),
            guard.max_cell_size(),
            guard.usable_space(),
        );

        let first_overflow = if is_overflowing {
            // build linked list of overflow pages by going backwards, from end to start.
            let overflow_payload = &payload[local_payload_size..];

            let mut overflows = payload_size - local_payload_size;

            let max_overflow_size_per_page = guard.can_fit_in_overflow();

            let mut prev_overflow_page = 0;

            while overflows > 0 {
                let overflow_page_number =
                    self.pager.alloc_empty_page(&mut *self.tx.borrow_mut())?;
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

            Some(prev_overflow_page)
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

        guard.insert_cell(self.current_slot, cell);

        Ok(())
    }
}
