use std::cmp::Ordering;

use crate::{
    sql::record::{Record, compare_records, records_equal},
    storage::{PageNumber, pager::Pager},
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
