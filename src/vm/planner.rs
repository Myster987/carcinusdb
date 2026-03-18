use crate::{
    database::{ReadDbTx, WriteDbTx},
    sql::{
        parser::statement::{BinaryOperator, Create, Drop, Expression, Statement},
        schema::{IndexMetadata, TableMetadata},
        types::Value,
    },
    storage::btree::BTreeKey,
    vm::{
        self, dml,
        operator::{Operator, index_scan::ScanBound},
    },
};

pub enum ExecutionPlan<'tx> {
    // iterator based
    Query(Box<dyn Operator + 'tx>),

    // table or index
    Create(Create),
    Drop(Drop),

    // transaction control
    BeginTransaction,
    Rollback,
    Commit,

    // meta
    Explain(Box<ExecutionPlan<'tx>>),
}

pub fn plan_read<'tx, Tx: ReadDbTx>(
    statement: Statement,
    tx: &'tx Tx,
) -> vm::Result<ExecutionPlan<'tx>> {
    match statement {
        Statement::Select {
            columns,
            from,
            r#where,
            order_by,
        } => {
            let table = tx.catalog().get_table(&from)?;
            let select = dml::select::Select::new(
                tx.read_cursor(table.root),
                table.schema,
                columns,
                r#where,
                order_by,
            )?;
            Ok(ExecutionPlan::Query(Box::new(select)))
        }

        Statement::Explain(inner) => Ok(ExecutionPlan::Explain(Box::new(plan_read(*inner, tx)?))),

        _ => Err(vm::Error::ReadOnly),
    }
}

pub fn plan_write<'tx>(
    statement: Statement,
    tx: &'tx impl WriteDbTx,
) -> vm::Result<ExecutionPlan<'tx>> {
    match statement {
        Statement::Select {
            columns,
            from,
            r#where,
            order_by,
        } => {
            let table = tx.catalog().get_table(&from)?;
            let select = dml::select::Select::new(
                tx.read_cursor(table.root),
                table.schema,
                columns,
                r#where,
                order_by,
            )?;
            Ok(ExecutionPlan::Query(Box::new(select)))
        }
        Statement::Insert {
            into,
            columns,
            values,
        } => {
            let table = tx.catalog().get_table(&into)?;
            let insert = dml::insert::Insert::new(
                tx.write_cursor(table.root),
                table.schema,
                columns,
                values,
            );
            Ok(ExecutionPlan::Query(Box::new(insert)))
        }
        Statement::Update {
            table,
            columns,
            r#where,
        } => {
            let table = tx.catalog().get_table(&table)?;
            let update = dml::update::Update::new(
                tx.write_cursor(table.root),
                table.schema,
                columns,
                r#where,
            )?;
            Ok(ExecutionPlan::Query(Box::new(update)))
        }
        Statement::Delete { from, r#where } => {
            let table = tx.catalog().get_table(&from)?;
            let delete =
                dml::delete::Delete::new(tx.write_cursor(table.root), table.schema, r#where)?;
            Ok(ExecutionPlan::Query(Box::new(delete)))
        }

        Statement::Create(s) => Ok(ExecutionPlan::Create(s)),
        Statement::Drop(s) => Ok(ExecutionPlan::Drop(s)),

        Statement::BeginTransaction => Ok(ExecutionPlan::BeginTransaction),
        Statement::Rollback => Ok(ExecutionPlan::Rollback),
        Statement::Commit => Ok(ExecutionPlan::Commit),

        Statement::Explain(inner) => Ok(ExecutionPlan::Explain(Box::new(plan_write(*inner, tx)?))),
    }
}

pub enum ScanKind {
    Eq(Value),
    Range(ScanBound, ScanBound),
}

fn find_index<'a>(
    r#where: &Option<Expression>,
    table: &'a TableMetadata,
) -> Option<(&'a IndexMetadata, ScanKind, Option<Expression>)> {
    let r#where = r#where.as_ref()?;

    match r#where {
        // eq
        Expression::BinaryOperation {
            left,
            operator: BinaryOperator::Eq,
            right,
        } => {
            let (index, val) = extract_col_val(left, right, table)?;
            Some((index, ScanKind::Eq(val), None))
        }

        // range
        Expression::BinaryOperation {
            left,
            operator,
            right,
        } if matches!(
            operator,
            BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq
        ) =>
        {
            let (index, val) = extract_col_val(left, right, table)?;
            let bound = operator_to_range_bounds(operator, val);
            Some((index, ScanKind::Range(bound.0, bound.1), None))
        }

        Expression::BinaryOperation {
            left,
            operator: BinaryOperator::And,
            right,
        } => {
            if let Some((index, kind, _)) = find_index(&Some(*left.clone()), table) {
                return Some((index, kind, Some(*right.clone())));
            }
            if let Some((index, kind, _)) = find_index(&Some(*right.clone()), table) {
                return Some((index, kind, Some(*left.clone())));
            }
            None
        }

        // no usefull index found
        _ => None,
    }
}

fn extract_col_val<'a>(
    left: &Expression,
    right: &Expression,
    table: &'a TableMetadata,
) -> Option<(&'a IndexMetadata, Value)> {
    // col = val
    if let (Expression::Identifier(col), Expression::Value(val)) = (left, right) {
        let index = table.indexes.iter().find(|i| i.column.name == *col)?;
        return Some((index, val.clone()));
    }

    // val = col (reversed — some people write 5 = id)
    if let (Expression::Value(val), Expression::Identifier(col)) = (left, right) {
        let index = table.indexes.iter().find(|i| i.column.name == *col)?;
        return Some((index, val.clone()));
    }

    None // can't use index
}

fn operator_to_range_bounds(operator: &BinaryOperator, value: Value) -> (ScanBound, ScanBound) {
    match operator {
        // WHERE col > 5   → (5, +∞)
        BinaryOperator::Gt => (ScanBound::Exclusive(value), ScanBound::Unbounded),

        // WHERE col >= 5  → [5, +∞)
        BinaryOperator::GtEq => (ScanBound::Inclusive(value), ScanBound::Unbounded),

        // WHERE col < 5   → (-∞, 5)
        BinaryOperator::Lt => (ScanBound::Unbounded, ScanBound::Exclusive(value)),

        // WHERE col <= 5  → (-∞, 5]
        BinaryOperator::LtEq => (ScanBound::Unbounded, ScanBound::Inclusive(value)),

        // shouldn't be called with other operators
        _ => unreachable!("called with non-range operator: {:?}", operator),
    }
}
