use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    database::DatabaseTransaction,
    sql::{
        parser::statement::{Assignment, Expression},
        record::{RecordBuilder, RecordMut},
        schema::{IndexMetadata, Schema},
        types::Value,
    },
    storage::btree::{BTreeCursor, BTreeKey, InsertOptions, UpdateOptions},
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

pub fn plan_update<'tx>(
    tx: &'tx DatabaseTransaction,
    table: String,
    columns: Vec<Assignment>,
    r#where: Option<Expression>,
    returning: Option<Vec<Expression>>,
) -> vm::Result<Box<dyn Operator + 'tx>> {
    let table = tx.catalog().get_table(&table)?;

    let mut plan: Box<dyn Operator + 'tx> = match find_index(&r#where, &table) {
        Some((index, kind, filter)) => {
            let table_cursor = Rc::new(RefCell::new(tx.cursor(table.root)));
            let index_scan = IndexScan::new(
                Rc::new(RefCell::new(tx.cursor(index.root))),
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
                .map(|idx| (idx.clone(), tx.cursor(idx.root)))
                .collect();

            Box::new(Update::with_index_source(
                table_cursor,
                source,
                table.schema,
                columns,
                index_cursors,
                returning.is_some(),
            ))
        }
        // no matching index found. fallback to sequential scan. still corect,
        // just slower.
        None => {
            let index_cursors = table
                .indexes
                .iter()
                .map(|idx| (idx.clone(), tx.cursor(idx.root)))
                .collect();

            Box::new(Update::sequential_scan(
                tx.cursor(table.root),
                table.schema.clone(),
                columns,
                r#where,
                index_cursors,
                returning.is_some(),
            )?)
        }
    };

    if let Some(returning) = returning {
        plan = Box::new(Projection::new(plan, returning)?);
    };

    Ok(plan)
}

pub struct Update<'tx> {
    cursor: Rc<RefCell<BTreeCursor<'tx>>>,
    source: Box<dyn Operator + 'tx>,
    assignments: HashMap<usize, Value>,
    index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx>)>,
    update_options: UpdateOptions,
}

impl<'tx> Update<'tx> {
    pub fn sequential_scan(
        cursor: BTreeCursor<'tx>,
        schema: Schema,
        assignments: Vec<Assignment>,
        r#where: Option<Expression>,
        index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx>)>,
        is_returning: bool,
    ) -> vm::Result<Self> {
        let mut assignment_map = HashMap::with_capacity(assignments.len());

        for Assignment { identifier, value } in assignments {
            let index = schema.index_of(&identifier).unwrap();
            let Expression::Value(value) = value else {
                panic!()
            };
            assignment_map.insert(index, value);
        }

        let cursor = Rc::new(RefCell::new(cursor));
        let scan = SeqScan::new(cursor.clone(), schema);

        let mut source: Box<dyn Operator + 'tx> = Box::new(scan);

        if let Some(expr) = r#where {
            source = Box::new(Filter::new(source, expr));
        }

        let mut update_options = UpdateOptions::default();

        if is_returning {
            update_options.set_returning();
        }

        Ok(Self {
            cursor,
            source,
            assignments: assignment_map,
            index_cursors,
            update_options,
        })
    }

    pub fn with_index_source(
        cursor: Rc<RefCell<BTreeCursor<'tx>>>,
        source: Box<dyn Operator + 'tx>,
        schema: Schema,
        assignments: Vec<Assignment>,
        index_cursors: Vec<(IndexMetadata, BTreeCursor<'tx>)>,
        is_returning: bool,
    ) -> Self {
        let mut assignment_map = HashMap::with_capacity(assignments.len());

        for Assignment { identifier, value } in assignments {
            let index = schema.index_of(&identifier).unwrap();
            let Expression::Value(value) = value else {
                panic!()
            };
            assignment_map.insert(index, value);
        }

        let mut update_options = UpdateOptions::default();

        if is_returning {
            update_options.set_returning();
        }

        Self {
            cursor,
            source,
            assignments: assignment_map,
            index_cursors,
            update_options,
        }
    }
}

impl<'tx> Operator for Update<'tx> {
    fn next(&mut self) -> vm::Result<Option<Row>> {
        let Some(row) = self.source.next()? else {
            return Ok(None);
        };

        let mut record_builder = RecordMut::from_record(&row);

        for (i, assaign) in self.assignments.iter() {
            let _ = record_builder.set(*i, assaign.clone());
        }

        let row_id = row.get_value(0).to_int();
        let old_key = BTreeKey::new_table_key(row_id, Some(row.to_borrowed()));

        let new_record = record_builder.serialize_to_record();
        let new_key = BTreeKey::new_table_key(row_id, Some(new_record.to_borrowed()));

        let inserted_record =
            self.cursor
                .borrow_mut()
                .update(&old_key, new_key, self.update_options)?;

        for (index_metadata, index_cursor) in self.index_cursors.iter_mut() {
            let col_idx = index_metadata.column_index;
            let old_col_value = old_key.get_record().unwrap().get_value(col_idx).to_owned();
            let new_col_value = new_record.get_value(col_idx).to_owned();
            let row_id = new_record.get_value(0).to_int();

            let old_idx_record = RecordBuilder::new()
                .add(old_col_value)
                .add(Value::Int(row_id))
                .build();

            let new_idx_record = RecordBuilder::new()
                .add(new_col_value)
                .add(Value::Int(row_id))
                .build();

            let old_key = BTreeKey::new_index_key(old_idx_record);
            let new_key = BTreeKey::new_index_key(new_idx_record);

            index_cursor.update(&old_key, new_key, InsertOptions::default())?;
        }

        return Ok(inserted_record);
    }

    fn schema(&self) -> &Schema {
        self.source.schema()
    }
}
