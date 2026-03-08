use crate::{
    sql::{self, parser::statement::Expression, schema::Schema, types::Value},
    vm::{
        self,
        operator::{Operator, Row},
    },
};

pub struct Filter {
    child: Box<dyn Operator>,
    predicate: Expression,
}

impl Operator for Filter {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        loop {
            match self.child.next()? {
                Some(row) => match eval(&self.predicate, &row, self.child.schema())? {
                    Value::Bool(true) => return Ok(Some(row)),
                    _ => continue,
                },
                None => return Ok(None),
            }
        }
    }

    fn schema(&self) -> &crate::sql::schema::Schema {
        self.child.schema()
    }
}

fn eval(expr: &Expression, row: &Row, schema: &Schema) -> vm::Result<Value> {
    match expr {
        Expression::Value(value) => Ok(value.clone()),

        Expression::Identifier(ident) => {
            let index = schema
                .index_of(ident)
                .ok_or(sql::analyzer::Error::ColumnNotFound(ident.to_owned()))?;
            Ok(row.get_value(index).to_owned())
        }

        Expression::BinaryOperation {
            left,
            operator,
            right,
        } => {
            let left = eval(left, row, schema)?;
            let right = eval(right, row, schema)?;

            todo!()
        }

        Expression::UnaryOperation { operator, expr } => {
            let value = eval(expr, row, schema)?;

            todo!()
        }

        _ => todo!(),
    }
}
