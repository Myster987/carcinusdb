use crate::utils::traits::Unsigned;

use super::column::Column;

pub struct Table<T: Unsigned> {
    id: usize,
    name: String,
    columns: Vec<Column<T>>,
}
