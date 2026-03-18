use std::{cell::RefCell, cmp::Ordering, rc::Rc};

use crate::{
    sql::{
        record::{RecordBuilder, compare_records},
        schema::Schema,
        types::Value,
    },
    storage::{
        btree::{BTreeCursor, BTreeKey, DatabaseCursor},
        wal::transaction::ReadTx,
    },
    vm::{
        self,
        operator::{Operator, Row},
    },
};

pub enum ScanBound {
    Unbounded,
    Inclusive(Value),
    Exclusive(Value),
}

pub struct IndexScan<'tx, Tx: ReadTx + 'tx> {
    /// walks the index b-tree.
    index_cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
    /// fetches actual row by row_id.
    table_cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
    /// low bound of scan.
    low: ScanBound,
    /// high bound of scan (for exact scans it's the same as `low`).
    high: ScanBound,
    schema: Schema,
    started: bool,
    done: bool,
}

impl<'tx, Tx: ReadTx + 'tx> IndexScan<'tx, Tx> {
    // point lookup — WHERE col = value
    pub fn eq(
        index_cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
        table_cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
        key: Value,
        schema: Schema,
    ) -> Self {
        Self {
            index_cursor,
            table_cursor,
            low: ScanBound::Inclusive(key.to_owned()),
            high: ScanBound::Inclusive(key),
            schema,
            started: false,
            done: false,
        }
    }

    // range scan — WHERE col BETWEEN low AND high
    pub fn range(
        index_cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
        table_cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
        low: ScanBound,
        high: ScanBound,
        schema: Schema,
    ) -> Self {
        Self {
            index_cursor,
            table_cursor,
            low,
            high,
            schema,
            started: false,
            done: false,
        }
    }
}

impl<'tx, Tx: ReadTx + 'tx> Operator for IndexScan<'tx, Tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        if self.done {
            return Ok(None);
        }

        let mut index = self.index_cursor.borrow_mut();

        if !self.started {
            self.started = true;

            let found = match &self.low {
                ScanBound::Unbounded => index.seek_first()?,
                ScanBound::Inclusive(val) | ScanBound::Exclusive(val) => {
                    let mut record = RecordBuilder::new();
                    record.add(val.clone());
                    let record = record.serialize_to_record();

                    let key = BTreeKey::new_index_key(record);
                    let found = index.seek(&key)?;

                    if matches!(&self.low, ScanBound::Exclusive(_)) {
                        index.next()?
                    } else {
                        found.is_found()
                    }
                }
            };
            if !found {
                self.done = true;
                return Ok(None);
            }
        } else {
            if !index.next()? {
                self.done = true;
                return Ok(None);
            }
        }

        let index_record = index.try_record()?;

        let past_bound = match &self.high {
            ScanBound::Unbounded => false,
            ScanBound::Inclusive(val) => {
                let mut record = RecordBuilder::new();
                record.add(val.to_owned());
                let bound_record = record.serialize_to_record();
                matches!(
                    compare_records(&index_record, &bound_record),
                    Ordering::Greater
                )
            }
            ScanBound::Exclusive(val) => {
                let mut record = RecordBuilder::new();
                record.add(val.to_owned());
                let bound_record = record.serialize_to_record();
                matches!(
                    compare_records(&index_record, &bound_record),
                    Ordering::Greater | Ordering::Equal
                )
            }
        };

        if past_bound {
            self.done = true;
            return Ok(None);
        }

        let row_id = index_record.get_value(1).to_int();
        let mut table = self.table_cursor.borrow_mut();
        table.seek(&BTreeKey::new_table_key(row_id, None))?;

        let row = table.try_record()?.to_owned();

        Ok(Some(row))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
