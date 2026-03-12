use crate::{
    sql::{parser::statement::Expression, schema::Schema, types::Value},
    storage::{
        btree::{BTreeCursor, DatabaseCursor, DeleteOptions},
        wal::transaction::WriteTx,
    },
    vm::{
        self,
        expression::resolve_expression_to_value,
        operator::{Operator, Row},
    },
};
pub struct Delete<'tx, Tx: WriteTx> {
    cursor: BTreeCursor<'tx, Tx>,
    predicate: Option<Expression>,
    schema: Schema,
    started: bool,
}

impl<'tx, Tx: WriteTx> Delete<'tx, Tx> {
    pub fn new(
        cursor: BTreeCursor<'tx, Tx>,
        schema: Schema,
        predicate: Option<Expression>,
    ) -> vm::Result<Self> {
        Ok(Self {
            cursor,
            predicate,
            schema,
            started: false,
        })
    }
}

impl<'tx, Tx: WriteTx> Operator for Delete<'tx, Tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        if !self.started {
            self.started = true;
            if !self.cursor.seek_first()? {
                return Ok(None);
            }
        }

        loop {
            let row = match self.cursor.try_record() {
                Ok(row) => row,
                Err(_) => return Ok(None),
            };

            let matches = match &self.predicate {
                None => true,
                Some(expr) => matches!(
                    resolve_expression_to_value(&row, &self.schema, expr)?,
                    Value::Bool(true)
                ),
            };

            if matches {
                self.cursor.delete_current(DeleteOptions::default())?;
                return Ok(Some(row));
            } else {
                if !self.cursor.next()? {
                    return Ok(None);
                }
            }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
