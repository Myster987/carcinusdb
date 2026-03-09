use crate::{
    database::DatabaseTransaction,
    sql::parser::statement::{Assignment, Create, Drop, Expression, Statement},
    vm::{
        self,
        operator::{Operator, filter::Filter, projection::Projection, seq_scan::SeqScan},
    },
};

pub enum ExecutionPlan<'tx> {
    // iterator based
    Select(Box<dyn Operator + 'tx>),

    // literal - execute immediately
    Insert {
        into: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },
    Update {
        table: String,
        columns: Vec<Assignment>,
        r#where: Option<Expression>,
    },
    Delete {
        from: String,
        r#where: Option<Expression>,
    },

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

pub fn plan<'tx, Tx: DatabaseTransaction>(
    stmt: Statement,
    tx: &'tx Tx,
) -> vm::Result<ExecutionPlan<'tx>> {
    match stmt {
        Statement::Select {
            columns,
            from,
            r#where,
            order_by,
        } => Ok(ExecutionPlan::Select(plan_select(
            columns, from, r#where, order_by, tx,
        )?)),

        // literal - planner just forwards them
        Statement::Insert {
            into,
            columns,
            values,
        } => Ok(ExecutionPlan::Insert {
            into,
            columns,
            values,
        }),
        Statement::Update {
            table,
            columns,
            r#where,
        } => Ok(ExecutionPlan::Update {
            table,
            columns,
            r#where,
        }),
        Statement::Delete { from, r#where } => Ok(ExecutionPlan::Delete { from, r#where }),
        Statement::Create(s) => Ok(ExecutionPlan::Create(s)),
        Statement::Drop(s) => Ok(ExecutionPlan::Drop(s)),

        Statement::BeginTransaction => Ok(ExecutionPlan::BeginTransaction),
        Statement::Rollback => Ok(ExecutionPlan::Rollback),
        Statement::Commit => Ok(ExecutionPlan::Commit),

        // recursively plan the inner statement
        Statement::Explain(inner) => {
            let inner_plan = plan(*inner, tx)?;
            Ok(ExecutionPlan::Explain(Box::new(inner_plan)))
        }
    }
}

fn plan_select<'tx, Tx: DatabaseTransaction>(
    columns: Vec<Expression>,
    from: String,
    r#where: Option<Expression>,
    order_by: Vec<Expression>,
    tx: &'tx Tx,
) -> vm::Result<Box<dyn Operator + 'tx>> {
    let table = tx.catalog().get_table(&from)?;
    let cursor = tx.read_cursor(table.root);

    let mut plan: Box<dyn Operator + 'tx> = Box::new(SeqScan::new(cursor, table.schema.clone()));

    if let Some(expr) = r#where {
        plan = Box::new(Filter::new(plan, expr));
    }

    if !columns.is_empty() {
        plan = Box::new(Projection::new(plan, columns)?);
    }

    Ok(plan)
}
