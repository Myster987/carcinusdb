use std::{cell::RefCell, rc::Rc};

use crate::{
    sql::{parser::statement::Expression, schema::Schema},
    storage::{btree::BTreeCursor, wal::transaction::ReadTx},
    vm::{
        self,
        operator::{Operator, Row, filter::Filter, projection::Projection, seq_scan::SeqScan},
    },
};

pub struct Select<'tx, Tx: ReadTx + 'tx> {
    _cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
    operator: Box<dyn Operator + 'tx>,
}

impl<'tx, Tx: ReadTx + 'tx> Select<'tx, Tx> {
    pub fn new(
        cursor: BTreeCursor<'tx, Tx>,
        schema: Schema,
        columns: Vec<Expression>,
        r#where: Option<Expression>,
        order_by: Vec<Expression>,
    ) -> vm::Result<Self> {
        let cursor = Rc::new(RefCell::new(cursor));
        let mut plan: Box<dyn Operator + 'tx> = Box::new(SeqScan::new(cursor.clone(), schema));

        if let Some(expr) = r#where {
            plan = Box::new(Filter::new(plan, expr));
        }

        if !columns.is_empty() {
            plan = Box::new(Projection::new(plan, columns)?);
        }

        Ok(Self {
            _cursor: cursor,
            operator: plan,
        })
    }
}

impl<'tx, Tx: ReadTx + 'tx> Operator for Select<'tx, Tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        self.operator.next()
    }

    fn schema(&self) -> &Schema {
        self.operator.schema()
    }
}
