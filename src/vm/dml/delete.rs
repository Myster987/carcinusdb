use std::{cell::RefCell, rc::Rc};

use crate::{
    database::WriteDbTx,
    sql::{
        parser::statement::Expression,
        record::RecordBuilder,
        schema::{IndexMetadata, Schema},
        types::Value,
    },
    storage::{
        btree::{BTreeCursor, BTreeKey, DeleteOptions},
        wal::transaction::WriteTx,
    },
    vm::{
        self,
        operator::{
            Operator, Row,
            filter::Filter,
            index_scan::{IndexScan, find_index},
            projection::Projection,
            seq_scan::SeqScan,
        },
    },
};

pub fn plan_delete<'tx, DbTx: WriteDbTx + 'tx>(
    tx: &'tx DbTx,
    from: String,
    r#where: Option<Expression>,
    returning: Option<Vec<Expression>>,
) -> vm::Result<Box<dyn Operator + 'tx>> {
    let table = tx.catalog().get_table(&from)?;

    let mut plan: Box<dyn Operator + 'tx> = match find_index(&r#where, &table) {
        Some((index, kind, filter)) => {
            let table_cursor = Rc::new(RefCell::new(tx.write_cursor(table.root)));
            let index_scan = IndexScan::new(
                Rc::new(RefCell::new(tx.read_cursor(index.root))),
                table_cursor.clone(),
                kind,
                table.schema.clone(),
            );
            let source: Box<dyn Operator + 'tx> = match filter {
                Some(expr) => Box::new(Filter::new(Box::new(index_scan), expr)),
                None => Box::new(index_scan),
            };

            let index_cursors = table
                .indexes
                .iter()
                .map(|idx| (idx.clone(), tx.write_cursor(idx.root)))
                .collect();

            Box::new(Delete::with_index_source(
                table_cursor,
                source,
                index_cursors,
            ))
        }
        // no matching index found. fallback to sequential scan. still corect,
        // just slower.
        None => {
            let index_cursors = table
                .indexes
                .iter()
                .map(|idx| (idx.clone(), tx.write_cursor(idx.root)))
                .collect();

            Box::new(Delete::sequential_scan(
                tx.write_cursor(table.root),
                table.schema.clone(),
                r#where,
                index_cursors,
            )?)
        }
    };

    if let Some(returning) = returning {
        plan = Box::new(Projection::new(plan, returning)?);
    }

    Ok(plan)
}

pub struct Delete<'tx, Tx: WriteTx + 'tx> {
    cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
    source: Box<dyn Operator + 'tx>,
    index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx, Tx>)>,
}

impl<'tx, Tx: WriteTx + 'tx> Delete<'tx, Tx> {
    pub fn sequential_scan(
        cursor: BTreeCursor<'tx, Tx>,
        schema: Schema,
        r#where: Option<Expression>,
        index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx, Tx>)>,
    ) -> vm::Result<Self> {
        let cursor = Rc::new(RefCell::new(cursor));
        let scan = SeqScan::new(cursor.clone(), schema);

        let mut source: Box<dyn Operator + 'tx> = Box::new(scan);

        if let Some(expr) = r#where {
            source = Box::new(Filter::new(source, expr));
        }

        Ok(Self {
            cursor,
            source,
            index_cursors,
        })
    }

    pub fn with_index_source(
        cursor: Rc<RefCell<BTreeCursor<'tx, Tx>>>,
        source: Box<dyn Operator + 'tx>,
        index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx, Tx>)>,
    ) -> Self {
        Self {
            cursor,
            source,
            index_cursors,
        }
    }
}

impl<'tx, Tx: WriteTx> Operator for Delete<'tx, Tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        let Some(row) = self.source.next()? else {
            return Ok(None);
        };

        self.cursor.borrow_mut().delete(
            &BTreeKey::new_table_key(row.get_value(0).to_int(), Some(row.to_borrowed())),
            DeleteOptions::default(),
        )?;

        for (index_metadata, index_cursor) in self.index_cursors.iter_mut() {
            let col_idx = index_metadata.column_index;
            let col_value = row.get_value(col_idx).to_owned();
            let row_id = row.get_value(0).to_int();

            let record = RecordBuilder::new()
                .add(col_value)
                .add(Value::Int(row_id))
                .build();

            let key = BTreeKey::new_index_key(record);
            index_cursor.delete(&key, DeleteOptions::default())?;
        }

        Ok(Some(row))
    }

    fn schema(&self) -> &Schema {
        self.source.schema()
    }
}
