use std::{borrow::Cow, cmp::Ordering};

use crate::{
    sql::record::{Record, compare_records, records_equal},
    storage::{
        self, PageNumber, SlotNumber,
        page::{BTreeCellRef, CellOps, Page},
        pager::{Pager, SharedPageGuard},
    },
};

#[derive(Debug)]
pub enum BTreeKey<'a> {
    TableRowId((i64, Option<Record<'a>>)),
    IndexKey(Record<'a>),
}

impl<'a> BTreeKey<'a> {
    pub fn new_table_row_id(row_id: i64, record: Option<Record<'a>>) -> Self {
        BTreeKey::TableRowId((row_id, record))
    }

    pub fn new_index_key(record: Record<'a>) -> Self {
        BTreeKey::IndexKey(record)
    }

    pub fn get_record(&self) -> Option<Record<'a>> {
        match self {
            BTreeKey::TableRowId((_, record)) => record.clone(),
            BTreeKey::IndexKey(record) => Some(record.clone()),
        }
    }

    pub fn to_owned(&self) -> BTreeKey<'static> {
        match self {
            BTreeKey::TableRowId((row_id, record)) => {
                BTreeKey::TableRowId((*row_id, record.as_ref().map(|r| r.to_owned())))
            }
            BTreeKey::IndexKey(record) => BTreeKey::IndexKey(record.to_owned()),
        }
    }
}

impl PartialEq for BTreeKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::TableRowId((id1, _)), Self::TableRowId((id2, _))) => id1 == id2,
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
            (Self::TableRowId((id1, _)), Self::TableRowId((id2, _))) => id1.cmp(id2),

            // Index keys: compare records lexicographically
            (Self::IndexKey(r1), Self::IndexKey(r2)) => compare_records(r1, r2),

            // Different types: table keys < index keys (arbitrary but consistent)
            (Self::TableRowId(_), Self::IndexKey(_)) => Ordering::Less,
            (Self::IndexKey(_), Self::TableRowId(_)) => Ordering::Greater,
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

pub struct BTree {
    root: PageNumber,

    pager: Pager,
}

impl BTree {
    pub fn new(pager: Pager, root: PageNumber) -> Self {
        Self { root, pager }
    }

    /// Check if search operation needs to visit rigth sibling. This can happen
    /// when page was splited and key we are looking for is there.
    pub fn key_in_range(&mut self, page: &Page, key: &BTreeKey) -> storage::Result<bool> {
        if let Some(high_key) = self.extract_high_key(page)? {
            Ok(*key <= high_key)
        } else {
            Ok(true)
        }
    }

    /// Returns page high key, if it exists. Otherwise it returns `Ok(None)`.
    fn extract_high_key<'a>(&mut self, page: &'a Page) -> storage::Result<Option<BTreeKey<'a>>> {
        if !page.has_high_key() {
            return Ok(None);
        }

        return self.extracty_key(page, Page::HIGH_KEY_SLOT).map(Some);
    }

    /// Takes page reference and returns `BTreeKey` based on page type. Cell
    /// migth need reassembly.
    fn extracty_key<'a>(
        &mut self,
        page: &'a Page,
        index: SlotNumber,
    ) -> storage::Result<BTreeKey<'a>> {
        if index >= page.len() {
            return Err(storage::Error::CellIndexOutRange);
        }

        let cell = page.get_cell(index)?;
        let reassembled_payload = reassemble_payload(&mut self.pager, page, index)?;

        match cell {
            BTreeCellRef::IndexInternal(_) => {
                let record = Record::new(reassembled_payload);
                Ok(BTreeKey::new_index_key(record))
            }
            BTreeCellRef::IndexLeaf(_) => {
                let record = Record::new(reassembled_payload);
                Ok(BTreeKey::new_index_key(record))
            }
            BTreeCellRef::TableInternal(cell) => Ok(BTreeKey::new_table_row_id(cell.row_id, None)),
            BTreeCellRef::TableLeaf(cell) => {
                let record = Record::new(reassembled_payload);
                Ok(BTreeKey::new_table_row_id(cell.row_id, Some(record)))
            }
        }
    }
}

pub trait DatabaseCursor {
    /// Traverses B-tree in order to find value that mathes given key.
    /// Returns position of found entry or where it should be inserted.
    fn seek(&mut self, key: &BTreeKey) -> storage::Result<SearchResult>;

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

pub struct BTreeCursor<'a> {
    btree: &'a mut BTree,
    page_guard: Option<SharedPageGuard>,
    current_page: PageNumber,
    current_slot: SlotNumber,
}

impl<'a> BTreeCursor<'a> {
    pub fn new(btree: &'a mut BTree) -> Self {
        let root = btree.root;
        Self {
            btree,
            page_guard: None,
            current_page: root,
            current_slot: 0,
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
        let count = page.count();

        let mut left = 0;
        let mut rigth = count;

        while left < rigth {
            let mid = left + (rigth - left) / 2;
            let slot = Page::FIRST_DATA_KEY + mid;

            let key = self.btree.extracty_key(page, slot)?;

            match search_key.cmp(&key) {
                Ordering::Less => rigth = mid,
                Ordering::Equal => return Ok(Ok(slot)),
                Ordering::Greater => left = mid + 1,
            }
        }

        Ok(Err(Page::FIRST_DATA_KEY + left))
    }
}

impl<'a> DatabaseCursor for BTreeCursor<'a> {
    fn seek(&mut self, key: &BTreeKey) -> storage::Result<SearchResult> {
        // start at root
        self.current_page = self.btree.root;

        loop {
            let page = self.btree.pager.read_page(self.current_page)?;
            let guard = page.lock_shared();

            // we need to move to rigth node
            if !self.btree.key_in_range(&guard, key)? {
                self.current_page = guard
                    .try_rigth_sibling()
                    .expect("page was splited and should contain sibling");
                continue;
            }

            let search_result = self.binary_search(&guard, key)?;

            if let Ok(slot) = search_result {
                self.page_guard.replace(guard);

                return Ok(SearchResult::Found {
                    page: self.current_page,
                    slot,
                });
            }
            if guard.is_leaf() {
                self.page_guard.replace(guard);

                return Ok(SearchResult::NotFound {
                    page: self.current_page,
                    slot: search_result.unwrap_err(),
                });
            }

            // go to child which may contain searched key
            self.current_page = guard.child(search_result.unwrap_err());
        }
    }

    fn key(&mut self) -> storage::Result<BTreeKey<'_>> {
        let page = self
            .page_guard
            .as_ref()
            .ok_or(storage::Error::InvalidPageType)?;
        self.btree.extracty_key(page, self.current_slot)
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

/// Returns whole cell payload including overflow pages. Takes mutable reference
/// to pager and cell that you want to reassemble. Returns [Cow], which is
/// `Borrowed` when cell isn't overflowing and `Owned` when cell needed to be
/// reconstructed. Returned slice can be directly used as record.
pub fn reassemble_payload<'a>(
    pager: &mut Pager,
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
