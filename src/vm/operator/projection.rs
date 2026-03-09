use crate::{
    sql::{self, parser::statement::Expression},
    vm::{
        self,
        operator::{Operator, Row},
    },
};

pub struct Projection<'tx> {
    child: Box<dyn Operator + 'tx>,
    indicies: Vec<usize>,
}

impl<'tx> Projection<'tx> {
    pub fn new(child: Box<dyn Operator + 'tx>, columns: Vec<Expression>) -> vm::Result<Self> {
        let schema = child.schema();

        let indicies = columns
            .iter()
            .map(|expr| match expr {
                Expression::Identifier(ident) => schema
                    .index_of(ident)
                    .ok_or(sql::analyzer::Error::ColumnNotFound(ident.to_owned()).into()),
                _ => Err(vm::Error::Unsupported(expr.clone())),
            })
            .collect::<vm::Result<Vec<usize>>>()?;

        Ok(Self { child, indicies })
    }
}

impl<'tx> Operator for Projection<'tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        match self.child.next()? {
            Some(row) => Ok(Some(row.project(&self.indicies)?)),
            None => Ok(None),
        }
    }

    fn schema(&self) -> &crate::sql::schema::Schema {
        self.child.schema()
    }
}
