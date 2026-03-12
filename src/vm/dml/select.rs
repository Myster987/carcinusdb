use crate::{
    database::ReadDbTx,
    sql::{parser::statement::Expression, schema::Schema},
    vm::{
        self,
        operator::{Operator, Row, filter::Filter, projection::Projection, seq_scan::SeqScan},
    },
};

pub struct Select<'tx> {
    operator: Box<dyn Operator + 'tx>,
}

impl<'tx> Select<'tx> {
    pub fn new<Tx: ReadDbTx>(
        tx: &'tx Tx,
        columns: Vec<Expression>,
        from: String,
        r#where: Option<Expression>,
        order_by: Vec<Expression>,
    ) -> vm::Result<Self> {
        let table = tx.catalog().get_table(&from)?;
        let cursor = tx.read_cursor(table.root);

        let mut plan: Box<dyn Operator + 'tx> =
            Box::new(SeqScan::new(cursor, table.schema.clone()));

        if let Some(expr) = r#where {
            plan = Box::new(Filter::new(plan, expr));
        }

        if !columns.is_empty() {
            plan = Box::new(Projection::new(plan, columns)?);
        }

        Ok(Self { operator: plan })
    }
}

impl<'tx> Operator for Select<'tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        self.operator.next()
    }

    fn schema(&self) -> &Schema {
        self.operator.schema()
    }
}
