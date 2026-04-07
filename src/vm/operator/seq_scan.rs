use std::{cell::RefCell, rc::Rc};

use crate::{
    sql::schema::Schema,
    storage::btree::{BTreeCursor, DatabaseCursor},
    vm::{
        self,
        operator::{Operator, Row},
    },
};

pub struct SeqScan<'tx> {
    cursor: Rc<RefCell<BTreeCursor<'tx>>>,
    schema: Schema,
    started: bool,
}

impl<'tx> SeqScan<'tx> {
    pub fn new(cursor: Rc<RefCell<BTreeCursor<'tx>>>, schema: Schema) -> Self {
        Self {
            cursor,
            schema,
            started: false,
        }
    }
}

impl<'tx> Operator for SeqScan<'tx> {
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
