use std::{borrow::Cow, cmp::Ordering};

use crate::{
    sql::record::{Record, compare_records, records_equal},
    storage::{
        self, PageNumber, SlotNumber,
        page::{self, CellOps, Page},
        pager::Pager,
    },
};

// pub struct ProtectedPayload {
//     pin: SharedPageGuard,
//     payload: Payload<'static>,
// }

// impl ProtectedPayload {
//     pub fn new(pin: SharedPageGuard, payload: Payload<'static>) -> Self {
//         Self { pin, payload }
//     }
// }

// #[derive(Debug)]
// pub enum Payload<'a> {
//     /// Payload was small enough, so we can store direct reference to it.
//     Ref(&'a [u8]),
//     /// Payload was too large and needed reassembly.
//     Reassembled(Box<[u8]>),
// }

// impl<'a> AsRef<[u8]> for Payload<'a> {
//     fn as_ref(&self) -> &[u8] {
//         match self {
//             Self::Ref(r) => r,
//             Self::Reassembled(r) => r,
//         }
//     }
// }

// #[derive(Debug)]
// pub struct FixedSizeMemCmp {
//     to_cmp: usize,
// }

// impl FixedSizeMemCmp {
//     pub fn for_type<T>() -> Self {
//         Self {
//             to_cmp: size_of::<T>(),
//         }
//     }
// }

// impl BytesCmp for FixedSizeMemCmp {
//     fn bytes_cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
//         a[..self.to_cmp].cmp(&b[..self.to_cmp])
//     }
// }

// impl TryFrom<DataType> for FixedSizeMemCmp {
//     type Error = ();

//     fn try_from(value: DataType) -> Result<Self, Self::Error> {
//         match value {
//             DataType::VarChar(_) | DataType::Boolean => Err(()),
//             // fixed_type => Ok(Self {
//             // })
//             _ => todo!(),
//         }
//     }
// }

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

pub struct BTree {
    root: PageNumber,

    pager: Pager,
}

impl BTree {
    pub fn new(pager: Pager, root: PageNumber) -> Self {
        Self { root, pager }
    }

    fn extract_high_key<'a>(&mut self, page: &'a Page) -> storage::Result<Option<BTreeKey<'a>>> {
        if !page.has_high_key() {
            return Ok(None);
        }

        let payload = reassemble_payload(&mut self.pager, page, Page::HIGH_KEY_SLOT)?;

        let key = BTreeKey::IndexKey(Record::new(payload));

        Ok(Some(key))
    }
}

// pub struct BTreeCursor {
//     page_pin: PagePin<()>,
//     page_number: PageNumber,
//     slot_number: SlotNumber,
// }

// impl BTreeCursor {
//     pub fn new(pager: &mut Pager, page_number: PageNumber) -> storage::Result<Self> {
//         let page = pager.read_page(page_number)?;
//         let guard = page.lock_shared();

//         Ok(Self {
//             page_pin: guard.into_pin(()),
//             page_number,
//             slot_number: 0,
//         })
//     }

//     pub fn current_page(&self) -> &Page {
//         self.page_pin.page.inner().content.as_ref().unwrap()
//     }

//     pub fn current_cell(&self) -> storage::Result<BTreeCell> {
//         self.current_page().get_cell(self.slot_number)
//     }

//     pub fn move_rigth(&mut self, pager: &mut Pager) -> storage::Result<bool> {
//         let page = self.current_page();

//         if let Some(rigth_sibling) = page.try_rigth_sibling() {
//             let rigth_page = pager.read_page(rigth_sibling)?;
//             let guard = rigth_page.lock_shared();

//             self.page_pin = guard.into_pin(());
//             self.page_number = rigth_sibling;
//             self.slot_number = 0;

//             Ok(true)
//         } else {
//             Ok(false)
//         }
//     }

// pub fn get_child_pointer(&self) -> storage::Result<PageNumber> {
//     let page = self.current_page();

//     if page.is_leaf() {
//        return Err(storage::Error::PageNotFound(self.page_number));
//     }

//     if self.slot_number =>
// }

// pub fn descende(&mut self, pager: &mut Pager, child: )
// }

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
