use std::cmp::Ordering;

use crate::{
    sql::statement::DataType,
    storage::{PageNumber, pager::Pager},
};

pub trait BytesCmp {
    /// Takes two byte slices and returns Ordering.
    fn bytes_cmp(&self, a: &[u8], b: &[u8]) -> Ordering;
}

#[derive(Debug)]
pub enum Payload<'a> {
    /// Payload was small enough, so we
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

#[derive(Debug)]
pub struct FixedSizeMemCmp {
    to_cmp: usize,
}

impl FixedSizeMemCmp {
    pub fn for_type<T>() -> Self {
        Self {
            to_cmp: size_of::<T>(),
        }
    }
}

impl BytesCmp for FixedSizeMemCmp {
    fn bytes_cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        a[..self.to_cmp].cmp(&b[..self.to_cmp])
    }
}

impl TryFrom<DataType> for FixedSizeMemCmp {
    type Error = ();

    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::VarChar(_) | DataType::Boolean => Err(()),
            // fixed_type => Ok(Self {
            // })
            _ => todo!(),
        }
    }
}

pub struct BTree {
    root: PageNumber,

    pager: Pager,
}
