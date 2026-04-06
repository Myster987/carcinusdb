use crate::{
    database::{CARCINUSDB_MASTER_TABLE_ROOT, DatabaseTransaction},
    sql::{
        parser::{
            self,
            statement::{Constrains, Create},
        },
        record::{RecordBuilder, RecordMut},
        schema::{IndexMetadata, Schema, TableMetadata},
        types::{Value, text::Text},
    },
    storage::btree::{BTreeKey, BTreeType, DatabaseCursor, InsertOptions},
    vm::{self, query_result::QueryResult},
};

pub fn create<'tx>(
    tx: &DatabaseTransaction<'tx>,
    statement: Create,
) -> vm::Result<QueryResult<'tx>> {
    match statement {
        Create::Table { name, columns } => create_table(tx, name, columns),
        Create::Index {
            name,
            table,
            column,
            unique,
        } => create_index(tx, name, table, column, unique),
        _ => todo!(),
    }
}

fn create_table<'tx>(
    tx: &DatabaseTransaction<'tx>,
    name: String,
    columns: Vec<parser::statement::Column>,
) -> vm::Result<QueryResult<'tx>> {
    let root_page = tx.create_btree(BTreeType::Table)?;

    let sql = reconstruct_create_table_sql(&name, &columns);

    let mut record = RecordMut::new();

    record.add(Value::Text(Text::new("table".into()))); // type
    record.add(Value::Text(Text::new(name.clone()))); // name
    record.add(Value::Text(Text::new(name.clone()))); // table_name
    record.add(Value::Int(root_page as i64)); // root_page
    record.add(Value::Text(Text::new(sql))); // sql

    let record = record.serialize_to_record();

    let mut cursor = tx.cursor(CARCINUSDB_MASTER_TABLE_ROOT);

    let row_id = cursor.next_row_id()?;

    cursor.insert(
        BTreeKey::new_table_key(row_id, Some(record)),
        InsertOptions::default(),
    )?;

    let schema = Schema::new(columns.into_iter().map(|c| c.into()).collect());

    tx.catalog().insert_table(
        name.clone(),
        TableMetadata::new(root_page, name, schema, vec![]),
    );

    Ok(QueryResult::RowsAffected(1))
}

fn create_index<'tx>(
    tx: &DatabaseTransaction<'tx>,
    name: String,
    table: String,
    column: String,
    unique: bool,
) -> vm::Result<QueryResult<'tx>> {
    let root_page = tx.create_btree(BTreeType::Index)?;

    let sql = reconstruct_create_index_sql(&name, &table, &column, unique);

    let mut record = RecordMut::new();

    record.add(Value::Text(Text::new("index".into()))); // type
    record.add(Value::Text(Text::new(name.clone()))); // name
    record.add(Value::Text(Text::new(table.clone()))); // table_name
    record.add(Value::Int(root_page as i64)); // root_page
    record.add(Value::Text(Text::new(sql))); // sql

    let record = record.serialize_to_record();

    let mut cursor = tx.cursor(CARCINUSDB_MASTER_TABLE_ROOT);

    let row_id = cursor.next_row_id()?;

    cursor.insert(
        BTreeKey::new_table_key(row_id, Some(record)),
        InsertOptions::default(),
    )?;

    let column_index = tx.catalog().get_table(&table)?.index_of(&column).unwrap();
    let column = tx.catalog().get_table(&table)?.schema.columns[column_index].clone();
    let index_metadata = IndexMetadata::new(root_page, name, column, column_index, unique);

    tx.catalog().insert_index(&table, index_metadata)?;

    {
        let source_table_root = tx.catalog().get_table(&table)?.root;
        let mut source_cursor = tx.cursor(source_table_root);
        let mut index_cursor = tx.cursor(root_page);

        while source_cursor.next()? {
            let record = source_cursor.try_record()?;
            let row_id = record.get_value(0).to_owned();
            let value = record.get_value(column_index).to_owned();

            let insert_record = RecordBuilder::new().add(row_id).add(value).build();
            let entry = BTreeKey::new_index_key(insert_record);

            index_cursor.insert(entry, InsertOptions::default())?;
        }
    }

    Ok(QueryResult::RowsAffected(1))
}

// Reconstructs "CREATE TABLE name (col1 TYPE, col2 TYPE NOT NULL, ...)" SQL.
fn reconstruct_create_table_sql(name: &str, columns: &[parser::statement::Column]) -> String {
    let cols = columns
        .iter()
        .map(|col| {
            let mut s = format!("{} {}", col.name, col.data_type);
            for constraint in &col.constrains {
                match constraint {
                    Constrains::PrimaryKey => s.push_str(" PRIMARY KEY"),
                    Constrains::NotNull => s.push_str(" NOT NULL"),
                    Constrains::Unique => s.push_str(" UNIQUE"),
                }
            }
            s
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!("CREATE TABLE {name} ({cols});")
}

// Reconstructs "CREATE INDEX name ON table (column)" SQL.
fn reconstruct_create_index_sql(name: &str, table: &str, column: &str, unique: bool) -> String {
    let unique = if unique { " UNIQUE" } else { "" };
    format!("CREATE{unique} INDEX {name} ON {table} ({column});")
}
