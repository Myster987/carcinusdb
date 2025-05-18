use crate::utils::buffer::Buffer;

/// FSM is accurate to 1/256-th of a page 
pub const FSM_PRECISION: usize = 256; 


pub struct FSMPageHeader {

}


pub struct FSMPage {
    buffer: Buffer<FSMPageHeader>
}