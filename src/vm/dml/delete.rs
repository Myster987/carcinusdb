use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use crate::{
    sql::{parser::statement::Expression, schema::Schema},
    storage::{
        btree::{BTreeCursor, DeleteOptions},
        wal::transaction::WriteTx,
    },
    vm::{
        self,
        operator::{Operator, Row, filter::Filter, seq_scan::SeqScan},
    },
};

pub struct Delete<'tx, Tx: WriteTx + 'tx> {
    cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
    operator: Box<dyn Operator + 'tx>,
    skip_advance: Rc<Cell<bool>>,
}

impl<'tx, Tx: WriteTx + 'tx> Delete<'tx, Tx> {
    pub fn new(
        cursor: BTreeCursor<'tx, Tx>,
        schema: Schema,
        r#where: Option<Expression>,
    ) -> vm::Result<Self> {
        let cursor = Rc::new(RefCell::new(cursor));
        let scan = SeqScan::new(cursor.clone(), schema);
        let skip_advance = scan.skip_advance.clone();

        let mut plan: Box<dyn Operator + 'tx> = Box::new(scan);

        if let Some(expr) = r#where {
            plan = Box::new(Filter::new(plan, expr));
        }

        Ok(Self {
            cursor,
            operator: plan,
            skip_advance,
        })
    }
}

impl<'tx, Tx: WriteTx> Operator for Delete<'tx, Tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        let Some(row) = self.operator.next()? else {
            return Ok(None);
        };

        self.cursor
            .borrow_mut()
            .delete_current(DeleteOptions::default())?;
        // tell SeqScan: cursor already advanced, skip next advance
        self.skip_advance.set(true);

        Ok(Some(row))
    }

    fn schema(&self) -> &Schema {
        self.operator.schema()
    }
}
