use std::{cell::RefCell, rc::Rc};

use crate::{
    database::DatabaseTransaction,
    sql::{parser::statement::Expression, schema::Schema},
    vm::{
        self,
        operator::{
            Operator, Row,
            filter::Filter,
            index_scan::{IndexScan, ScanKind, find_index},
            projection::Projection,
            seq_scan::SeqScan,
        },
    },
};

pub fn plan_select<'tx>(
    tx: &DatabaseTransaction<'tx>,
    columns: Vec<Expression>,
    from: String,
    r#where: Option<Expression>,
    order_by: Vec<Expression>,
) -> vm::Result<Box<dyn Operator + 'tx>> {
    let table = tx.catalog().get_table(&from)?;

    let (scan, residual): (Box<dyn Operator + 'tx>, Option<Expression>) =
        match find_index(&r#where, &table) {
            Some((index, ScanKind::Eq(val), residual)) => {
                let op = Box::new(IndexScan::eq(
                    Rc::new(RefCell::new(tx.cursor(index.root))),
                    Rc::new(RefCell::new(tx.cursor(table.root))),
                    val,
                    table.schema.clone(),
                ));
                (op, residual)
            }
            Some((index, ScanKind::Range(lo, hi), residual)) => {
                let op = Box::new(IndexScan::range(
                    Rc::new(RefCell::new(tx.cursor(index.root))),
                    Rc::new(RefCell::new(tx.cursor(table.root))),
                    lo,
                    hi,
                    table.schema.clone(),
                ));
                (op, residual)
            }
            None => {
                let op = Box::new(SeqScan::new(
                    Rc::new(RefCell::new(tx.cursor(table.root))),
                    table.schema.clone(),
                ));
                (op, r#where) // full where goes to filter
            }
        };

    let mut plan: Box<dyn Operator + 'tx> = scan;

    if let Some(expr) = residual {
        plan = Box::new(Filter::new(plan, expr));
    }

    if !columns.is_empty() {
        plan = Box::new(Projection::new(plan, columns)?);
    }

    Ok(Box::new(Select { operator: plan }))
}

pub struct Select<'tx> {
    operator: Box<dyn Operator + 'tx>,
}

impl<'tx> Operator for Select<'tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        self.operator.next()
    }

    fn schema(&self) -> &Schema {
        self.operator.schema()
    }
}
