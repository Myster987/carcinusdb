use std::fmt::Debug;

use crate::{
    sql::schema::Schema,
    utils::debug_table::DebugTable,
    vm::{
        self,
        operator::{Operator, Row},
    },
};

pub enum QueryResult<'tx> {
    Rows(RowIterator<'tx>),
    RowsAffected(usize),
}

unsafe impl Send for QueryResult<'_> {}

impl<'tx> QueryResult<'tx> {
    pub fn to_string(self) -> vm::Result<String> {
        match self {
            Self::RowsAffected(affected) => Ok(format!("{} rows affected", affected)),
            Self::Rows(rows) => {
                let columns = rows.operator.schema().column_names();
                let collected = rows.collect::<vm::Result<Vec<_>>>()?;

                let mut table = DebugTable::new();
                columns.iter().for_each(|col| table.add_column(col));

                for row in &collected {
                    table.insert_row(row.values().iter().map(|v| v.to_string()).collect());
                }

                Ok(format!("{}", table))
            }
        }
    }
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

    pub fn schema(&self) -> &Schema {
        self.operator.schema()
    }
}

impl<'tx> Iterator for RowIterator<'tx> {
    type Item = vm::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.operator.next().transpose()
    }
}
