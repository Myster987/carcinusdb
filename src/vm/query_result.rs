use crate::vm::{
    self,
    operator::{Operator, Row},
};

pub enum QueryResult<'tx> {
    Rows(RowIterator<'tx>),
    RowsAffected(usize),
}

pub struct RowIterator<'tx> {
    operator: Box<dyn Operator + 'tx>,
}

impl<'tx> Iterator for RowIterator<'tx> {
    type Item = vm::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.operator.next().transpose()
    }
}
