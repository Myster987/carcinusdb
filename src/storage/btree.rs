use std::{cmp::Ordering, io};

use crate::storage::{
    self, PageNumber, SlotNumber,
    page::BTreeCell,
    pager::{Pager, SharedPageGuard},
};

// pub trait BytesCmp {
//     /// Takes two byte slices and returns Ordering.
//     fn bytes_cmp(&self, a: &[u8], b: &[u8]) -> Ordering;
// }

pub struct ProtectedPayload {
    pin: SharedPageGuard,
    payload: Payload<'static>,
}

impl ProtectedPayload {
    pub fn new(pin: SharedPageGuard, payload: Payload<'static>) -> Self {
        Self { pin, payload }
    }
}

#[derive(Debug)]
pub enum Payload<'a> {
    /// Payload was small enough, so we can store direct reference to it.
    Ref(&'a [u8]),
    /// Payload was too large and needed reassembly.
    Reassembled(Box<[u8]>),
}

impl<'a> AsRef<[u8]> for Payload<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Ref(r) => r,
            Self::Reassembled(r) => r,
        }
    }
}

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

// pub enum BTreeKey<'a> {
//     RowId(i64),
//     // IndexKey()
// }

pub struct BTree {
    root: PageNumber,

    pager: Pager,
}

impl BTree {
    pub fn new(pager: Pager, root: PageNumber) -> Self {
        Self { root, pager }
    }
}

// pub fn reassemble_payload(
//     pager: &mut Pager,
//     page_number: PageNumber,
//     slot_number: SlotNumber,
// ) -> storage::Result<ProtectedPayload> {
//     let page = pager.read_page(page_number)?;
//     let pin = page.lock_shared();

//     if !pin.cell_overflows(slot_number) {
//         let cell = pin.get_cell(slot_number)?;
//         let payload = Payload::Ref(unsafe { crate::utils::cast::cast_static(cell.as_ref()) });

//         Ok(ProtectedPayload::new(pin, payload))
//     } else {
//         Err(storage::Error::PageNotFound(page_number))
//     }
// }
