use std::fmt::Debug;

use crate::{
    sql::{record::Record, schema::Schema},
    utils::debug_table::DebugTable,
    vm::{self, operator::Operator},
};

pub enum QueryResult<'tx> {
    Records(RecordIterator<'tx>),
    RecordsAffected(usize),
}

unsafe impl Send for QueryResult<'_> {}

impl<'tx> QueryResult<'tx> {
    pub fn to_string(self) -> vm::Result<String> {
        match self {
            Self::RecordsAffected(affected) => Ok(format!("{} rows affected", affected)),
            Self::Records(records) => {
                let columns = records.operator.schema().column_names();
                let collected = records.collect::<vm::Result<Vec<_>>>()?;

                let mut table = DebugTable::new();
                columns.iter().for_each(|col| table.add_column(col));

                for record in &collected {
                    table.insert_row(record.values().iter().map(|v| v.to_string()).collect());
                }

                Ok(format!("{}", table))
            }
        }
    }
}

impl Debug for QueryResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Records(_) => f.write_str("QueryResult::Records(iterator)"),
            Self::RecordsAffected(affected) => {
                write!(f, "QueryResult::RecordsAffected({})", affected)
            }
        }
    }
}

pub struct RecordIterator<'tx> {
    operator: Box<dyn Operator + 'tx>,
}

impl<'tx> RecordIterator<'tx> {
    pub fn new(operator: Box<dyn Operator + 'tx>) -> Self {
        Self { operator }
    }

    pub fn schema(&self) -> &Schema {
        self.operator.schema()
    }
}

impl<'tx> Iterator for RecordIterator<'tx> {
    type Item = vm::Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.operator.next().transpose()
    }
}
