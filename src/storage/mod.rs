use std::mem;

pub mod page;

pub type PageNumber = u32;
pub type SlotNumber = u16;

pub const SLOT_SIZE: usize = mem::size_of::<SlotNumber>();
