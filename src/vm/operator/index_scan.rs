use std::{cell::RefCell, cmp::Ordering, rc::Rc};

use crate::{
    sql::{
        parser::statement::{BinaryOperator, Expression},
        record::{RecordBuilder, compare_records},
        schema::{IndexMetadata, Schema, TableMetadata},
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

pub struct IndexScan<'tx, IndexTx: ReadTx + 'tx, TableTx: ReadTx + 'tx> {
    /// walks the index b-tree.
    index_cursor: Rc<RefCell<BTreeCursor<'tx, IndexTx>>>,
    /// fetches actual row by row_id.
    table_cursor: Rc<RefCell<BTreeCursor<'tx, TableTx>>>,
    /// low bound of scan.
    low: ScanBound,
    /// high bound of scan (for exact scans it's the same as `low`).
    high: ScanBound,
    schema: Schema,
    started: bool,
    done: bool,
}

impl<'tx, IndexTx: ReadTx + 'tx, TableTx: ReadTx + 'tx> IndexScan<'tx, IndexTx, TableTx> {
    pub fn new(
        index_cursor: Rc<RefCell<BTreeCursor<'tx, IndexTx>>>,
        table_cursor: Rc<RefCell<BTreeCursor<'tx, TableTx>>>,
        kind: ScanKind,
        schema: Schema,
    ) -> Self {
        let (low, high) = match kind {
            ScanKind::Eq(val) => (ScanBound::Inclusive(val.clone()), ScanBound::Inclusive(val)),
            ScanKind::Range(lo, hi) => (lo, hi),
        };
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

    // point lookup — WHERE col = value
    pub fn eq(
        index_cursor: Rc<RefCell<BTreeCursor<'tx, IndexTx>>>,
        table_cursor: Rc<RefCell<BTreeCursor<'tx, TableTx>>>,
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
        index_cursor: Rc<RefCell<BTreeCursor<'tx, IndexTx>>>,
        table_cursor: Rc<RefCell<BTreeCursor<'tx, TableTx>>>,
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

impl<'tx, IndexTx: ReadTx + 'tx, TableTx: ReadTx + 'tx> Operator
    for IndexScan<'tx, IndexTx, TableTx>
{
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
                    let mut record = RecordBuilder::new().add(val.clone()).build();

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
                let bound_record = RecordBuilder::new().add(val.to_owned()).build();
                matches!(
                    compare_records(&index_record, &bound_record),
                    Ordering::Greater
                )
            }
            ScanBound::Exclusive(val) => {
                let bound_record = RecordBuilder::new().add(val.to_owned()).build();
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

pub enum ScanKind {
    Eq(Value),
    Range(ScanBound, ScanBound),
}

pub fn find_index<'a>(
    r#where: &Option<Expression>,
    table: &'a TableMetadata,
) -> Option<(&'a IndexMetadata, ScanKind, Option<Expression>)> {
    let r#where = r#where.as_ref()?;

    match r#where {
        // eq
        Expression::BinaryOperation {
            left,
            operator: BinaryOperator::Eq,
            right,
        } => {
            let (index, val) = extract_col_val(left, right, table)?;
            Some((index, ScanKind::Eq(val), None))
        }

        // range
        Expression::BinaryOperation {
            left,
            operator,
            right,
        } if matches!(
            operator,
            BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq
        ) =>
        {
            let (index, val) = extract_col_val(left, right, table)?;
            let bound = operator_to_range_bounds(operator, val);
            Some((index, ScanKind::Range(bound.0, bound.1), None))
        }

        Expression::BinaryOperation {
            left,
            operator: BinaryOperator::And,
            right,
        } => {
            if let Some((index, kind, _)) = find_index(&Some(*left.clone()), table) {
                return Some((index, kind, Some(*right.clone())));
            }
            if let Some((index, kind, _)) = find_index(&Some(*right.clone()), table) {
                return Some((index, kind, Some(*left.clone())));
            }
            None
        }

        // no usefull index found
        _ => None,
    }
}

fn extract_col_val<'a>(
    left: &Expression,
    right: &Expression,
    table: &'a TableMetadata,
) -> Option<(&'a IndexMetadata, Value)> {
    // col = val
    if let (Expression::Identifier(col), Expression::Value(val)) = (left, right) {
        let index = table.indexes.iter().find(|i| i.column.name == *col)?;
        return Some((index, val.clone()));
    }

    // val = col (reversed — some people write 5 = id)
    if let (Expression::Value(val), Expression::Identifier(col)) = (left, right) {
        let index = table.indexes.iter().find(|i| i.column.name == *col)?;
        return Some((index, val.clone()));
    }

    None // can't use index
}

fn operator_to_range_bounds(operator: &BinaryOperator, value: Value) -> (ScanBound, ScanBound) {
    match operator {
        // WHERE col > 5   → (5, +∞)
        BinaryOperator::Gt => (ScanBound::Exclusive(value), ScanBound::Unbounded),

        // WHERE col >= 5  → [5, +∞)
        BinaryOperator::GtEq => (ScanBound::Inclusive(value), ScanBound::Unbounded),

        // WHERE col < 5   → (-∞, 5)
        BinaryOperator::Lt => (ScanBound::Unbounded, ScanBound::Exclusive(value)),

        // WHERE col <= 5  → (-∞, 5]
        BinaryOperator::LtEq => (ScanBound::Unbounded, ScanBound::Inclusive(value)),

        // shouldn't be called with other operators
        _ => unreachable!("called with non-range operator: {:?}", operator),
    }
}
