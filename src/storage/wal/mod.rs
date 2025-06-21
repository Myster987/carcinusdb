use crate::storage::{PageNumber, TransactionId};

pub struct WalHeader {
    version: u16,
    transaction_count: u32,
    is_init: bool,
    
}

pub struct FrameHeader {
    page_number: PageNumber,
    transaction_id: TransactionId
}

pub struct Frame {

}