use crate::{
    sql::{
        self,
        parser::statement::Expression,
        record::RecordMut,
        schema::{Column, Schema},
        types::Value,
    },
    vm::{
        self,
        expression::resolve_expression_to_value,
        operator::{Operator, Row},
    },
};

pub struct Projection<'tx> {
    child: Box<dyn Operator + 'tx>,
    columns: Vec<Expression>,
    projected_schema: Schema,
}

impl<'tx> Projection<'tx> {
    pub fn new(child: Box<dyn Operator + 'tx>, columns: Vec<Expression>) -> vm::Result<Self> {
        let child_schema = child.schema();
        let mut projected_schema = Schema::empty();

        for col in &columns {
            let new_column = match col {
                Expression::Identifier(ident) => child_schema.get(ident).to_owned(),
                Expression::Alias { expr: _, r#as } => Column::from_name(r#as),
                rest => Column::from_name(&format!("{rest}")),
            };
            projected_schema.push(new_column);
        }

        Ok(Self {
            child,
            columns,
            projected_schema,
        })
    }
}

impl<'tx> Operator for Projection<'tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        // SELECT id, age + 10 FROM test;

        let Some(row) = self.child.next()? else {
            return Ok(None);
        };

        let output = self
            .columns
            .iter()
            .map(|expr| resolve_expression_to_value(&row, self.child.schema(), expr))
            .collect::<sql::Result<Vec<Value>>>()?;

        let record = RecordMut::from_values(output).serialize_to_record();

        Ok(Some(record))
    }

    fn schema(&self) -> &Schema {
        &self.projected_schema
    }
}
