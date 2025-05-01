//! Each table gets it's own directory that contains:
//! - meta (metadata file)
//! - fsm
//! - block.0, block.1, block.2, etc...

/// Size of each block that table will be divided into.
const BLOCK_SIZE: usize = 2_usize.pow(30) * 1;
