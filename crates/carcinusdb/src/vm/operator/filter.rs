use crate::{
    sql::{parser::statement::Expression, record::Record, types::Value},
    vm::{self, expression::resolve_expression_to_value, operator::Operator},
};

pub struct Filter<'tx> {
    child: Box<dyn Operator + 'tx>,
    predicate: Expression,
}

impl<'tx> Filter<'tx> {
    pub fn new(child: Box<dyn Operator + 'tx>, predicate: Expression) -> Self {
        Self { child, predicate }
    }
}

impl<'tx> Operator for Filter<'tx> {
    fn next(&mut self) -> vm::Result<Option<Record>> {
        loop {
            match self.child.next()? {
                Some(record) => {
                    match resolve_expression_to_value(
                        &record,
                        self.child.schema(),
                        &self.predicate,
                    )? {
                        Value::Bool(true) => return Ok(Some(record)),
                        _ => continue,
                    }
                }
                None => return Ok(None),
            }
        }
    }

    fn schema(&self) -> &crate::sql::schema::Schema {
        self.child.schema()
    }
}
