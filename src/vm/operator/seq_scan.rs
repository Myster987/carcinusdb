use std::{cell::RefCell, rc::Rc};

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
}

impl<'tx, Tx: ReadTx> SeqScan<'tx, Tx> {
    pub fn new(cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>, schema: Schema) -> Self {
        Self {
            cursor,
            schema,
            started: false,
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
        } else {
            if !cursor.next()? {
                return Ok(None);
            }
        }

        match cursor.try_record() {
            Ok(record) => Ok(Some(record)),
            _ => Ok(None),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
