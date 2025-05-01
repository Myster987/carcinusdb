use crate::utils::traits::Unsigned;

#[derive(Debug)]
pub enum Column<T: Unsigned> {
    SmallInt,
    Int,
    BigInt,
    Boolean,
    VarChar(T),
}
