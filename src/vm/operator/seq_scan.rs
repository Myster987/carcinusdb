use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

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
    cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
    schema: Schema,
    started: bool,
    pub skip_advance: Rc<Cell<bool>>,
}

impl<'tx, Tx: ReadTx> SeqScan<'tx, Tx> {
    pub fn new(cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>, schema: Schema) -> Self {
        Self {
            cursor,
            schema,
            started: false,
            skip_advance: Rc::new(Cell::new(false)),
        }
    }
}

impl<'tx, Tx: ReadTx> Operator for SeqScan<'tx, Tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        let mut cursor = self.cursor.borrow_mut();
        if !self.started {
            self.started = true;
            if !cursor.seek_first()? {
                return Ok(None);
            }
        } else if self.skip_advance.get() {
            // cursor already advanced by delete/update — just read current
            self.skip_advance.set(false);
        } else {
            if !cursor.next()? {
                return Ok(None);
            }
        }

        let record = cursor.try_record()?;
        Ok(Some(record.to_owned()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
