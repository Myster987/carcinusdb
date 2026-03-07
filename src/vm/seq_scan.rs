use crate::{
    sql::schema::Schema,
    storage::{
        btree::{BTreeCursor, DatabaseCursor},
        wal::transaction::ReadTx,
    },
    vm::{
        self,
        operator::{Operator, Row},
    },
};

pub struct SeqScan<'tx, Tx: ReadTx> {
    cursor: BTreeCursor<'tx, Tx>,
    schema: Schema,
    started: bool,
}

impl<'tx, Tx: ReadTx> Operator for SeqScan<'tx, Tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        if !self.started {
            self.started = true;
            if !self.cursor.seek_first()? {
                return Ok(None);
            }
        } else {
            if !self.cursor.next()? {
                return Ok(None);
            }
        }

        let record = self.cursor.try_record()?;
        Ok(Some(record.to_owned()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
