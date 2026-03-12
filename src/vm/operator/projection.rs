use crate::{
    sql::{self, parser::statement::Expression, schema::Schema},
    vm::{
        self,
        operator::{Operator, Row},
    },
};

pub struct Projection<'tx> {
    child: Box<dyn Operator + 'tx>,
    indicies: Vec<usize>,
    schema: Schema,
}

impl<'tx> Projection<'tx> {
    pub fn new(child: Box<dyn Operator + 'tx>, columns: Vec<Expression>) -> vm::Result<Self> {
        let child_schema = child.schema();

        let indicies = columns
            .iter()
            .map(|expr| match expr {
                Expression::Identifier(ident) => child_schema
                    .index_of(ident)
                    .ok_or(sql::analyzer::Error::ColumnNotFound(ident.to_owned()).into()),
                _ => Err(vm::Error::Unsupported(expr.clone())),
            })
            .collect::<vm::Result<Vec<usize>>>()?;

        let output_schema = indicies
            .iter()
            .map(|&i| child_schema.columns[i].clone())
            .collect();

        let schema = Schema::new(output_schema);

        Ok(Self {
            child,
            indicies,
            schema,
        })
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
        &self.schema
    }
}
