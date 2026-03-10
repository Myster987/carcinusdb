use std::fmt::Debug;

use crate::vm::{
    self,
    operator::{Operator, Row},
};

pub enum QueryResult<'tx> {
    Rows(RowIterator<'tx>),
    RowsAffected(usize),
}

impl Debug for QueryResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rows(_) => f.write_str("QueryResult::Rows(iterator)"),
            Self::RowsAffected(affected) => write!(f, "QueryResult::RowsAffected({})", affected),
        }
    }
}

pub struct RowIterator<'tx> {
    operator: Box<dyn Operator + 'tx>,
}

impl<'tx> RowIterator<'tx> {
    pub fn new(operator: Box<dyn Operator + 'tx>) -> Self {
        Self { operator }
    }
}

impl<'tx> Iterator for RowIterator<'tx> {
    type Item = vm::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.operator.next().transpose()
    }
}
