use std::{fs::File, sync::Arc};

use crate::{storage::PageNumber, utils::io::BlockIO};

pub const WAL_HEADER_SIZE: usize = size_of::<WalHeader>();
pub const WAL_FRAME_HEADER_SIZE: usize = size_of::<WalFrameHeader>();


#[repr(C)]
#[derive(Debug)]
pub struct WalHeader {
    /// Page size
    pub page_size: u32,

    pub checkpoint_seq_num: u32,
    /// Random number incremented with each checkpoint
    pub salt_1: u32,
    /// Random number incremented with each checkpoint
    pub salt_2: u32,
    pub checksum_1: u32,
    pub checksum_2: u32,
}

#[repr(C)]
#[derive(Debug)]
pub struct WalFrameHeader {
    /// Page number of frame
    page_number: PageNumber,
    /// Size of database in pages. Only for commit records.
    /// For all other records is 0.
    db_size: u32,
    /// Copies from `WalHeader`
    salt_1: u32,
    /// Copies from `WalHeader`
    salt_2: u32,
    /// First half of cumulative checksum up to this page (including)
    checksum_1: u32,
    /// Second half of cumulative checksum up to this page (including)
    checksum_2: u32,
}


pub struct Wal {
    io: Arc<BlockIO<File>>,

}