use crate::{
    database::ReadDbTx,
    sql::{
        self,
        parser::statement::{Expression, Statement},
        schema::ROW_ID_COLUMN,
        types::Value,
    },
};

pub fn prepare<Tx: ReadDbTx>(tx: &Tx, statement: &mut Statement) -> sql::Result<()> {
    let catalog = tx.catalog();
    match statement {
        Statement::Select { columns, from, .. }
            if columns.iter().any(|expr| *expr == Expression::Wildcard) =>
        {
            let metadata = catalog.get_table(from)?;

            let identifiers = metadata
                .schema
                .columns
                .iter()
                .filter(|&col| col.name != ROW_ID_COLUMN)
                .cloned()
                .map(|col| Expression::Identifier(col.name))
                .collect::<Vec<Expression>>();

            let mut resolved_wildcards = Vec::new();

            for expr in columns.drain(..) {
                if expr == Expression::Wildcard {
                    resolved_wildcards.extend(identifiers.iter().cloned());
                } else {
                    resolved_wildcards.push(expr);
                }
            }

            *columns = resolved_wildcards;
        }

        Statement::Insert {
            into,
            columns,
            values,
        } => {
            let metadata = catalog.get_table(into)?;

            if columns.is_empty() {
                *columns = metadata.schema.column_names();
            }

            if metadata.schema.columns[0].name == ROW_ID_COLUMN {
                if columns[0] != ROW_ID_COLUMN {
                    columns.insert(0, ROW_ID_COLUMN.into());
                }

                let mut current_row_id = tx
                    .read_cursor(metadata.root)
                    .next_row_id()
                    .map_err(|_| sql::Error::InvalidSerialType)?;

                for row in values.iter_mut() {
                    row.insert(0, Expression::Value(Value::Int(current_row_id)));
                    current_row_id += 1;
                }
            }

            for current_index in 0..metadata.schema.len() {
                let sorted_index = metadata.schema.index_of(&columns[current_index]).unwrap();
                columns.swap(current_index, sorted_index);
                for row in values.iter_mut() {
                    row.swap(current_index, sorted_index);
                }
            }

            for col in metadata.schema.columns.iter() {
                if col.name == ROW_ID_COLUMN {
                    continue;
                }

                let col_idx = metadata.schema.index_of(&col.name).unwrap();
                let provided = columns.contains(&col.name);

                for row in values.iter_mut() {
                    if provided {
                        if col.properties.is_not_null() {
                            if let Expression::Value(Value::Null) = &row[col_idx] {
                                return Err(sql::Error::NotNullViolation(col.name.clone()));
                            }
                        }
                    } else {
                        let value = match &col.default {
                            Some(default) => Expression::Value(default.clone()),
                            None if col.properties.is_null() => Expression::Value(Value::Null),
                            None => return Err(sql::Error::MissingValue(col.name.clone())),
                        };
                        if col_idx < row.len() {
                            row[col_idx] = value;
                        } else {
                            row.push(value);
                        }
                    }
                }
            }
        }

        Statement::Explain(inner) => {
            prepare(tx, &mut *inner)?;
        }

        _ => {} // Nothing to do here.
    };

    Ok(())
}
