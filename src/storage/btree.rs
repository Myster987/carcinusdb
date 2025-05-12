use super::{PageNumber, pager::Pager};

pub struct BTree {
    root: PageNumber,
    pager: Pager,
}
