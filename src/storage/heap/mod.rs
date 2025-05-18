pub mod block_manager;
pub mod fsm;

pub type BlockId = u32;
pub type BlockNumber = u32;

/// Size of 1 block (1 GB)
pub const BLOCK_SIZE: usize = 2_usize.pow(30) * 1;
