use std::mem;

use crate::{
    sql::{
        self,
        parser::statement::{BinaryOperator, Expression, Statement, UnaryOperator},
        types::Value,
    },
    vm,
};

pub fn optimize(statement: &mut Statement) -> sql::Result<()> {
    match statement {
        Statement::Select {
            columns,
            r#where,
            order_by,
            ..
        } => {
            simplify_all(columns.iter_mut())?;
            simplfy_where(r#where)?;
            simplify_all(order_by.iter_mut())?;
        }

        Statement::Insert { values, .. } => {
            values
                .iter_mut()
                .try_for_each(|row| simplify_all(row.iter_mut()))?;
        }

        Statement::Delete { r#where, .. } => {
            simplfy_where(r#where)?;
        }

        Statement::Update {
            columns, r#where, ..
        } => {
            simplfy_where(r#where)?;
            simplify_all(columns.iter_mut().map(|col| &mut col.value))?;
        }

        Statement::Explain(inner) => {
            optimize(&mut *inner)?;
        }

        _ => {}
    };

    Ok(())
}

/// Simplifies an optional where clause.
fn simplfy_where(r#where: &mut Option<Expression>) -> sql::Result<()> {
    r#where.as_mut().map(simplify).unwrap_or(Ok(()))
}

/// Simplifies all the expressions in a list.
fn simplify_all<'e>(mut expressions: impl Iterator<Item = &'e mut Expression>) -> sql::Result<()> {
    expressions.try_for_each(simplify)
}

/// Expression reducer. This function tries to modify given expression in such
/// a way, that executer needs to do less work.
///
/// # Examples
///
/// ```text
/// val + 2 + 2 * 2
/// ```
///
/// Will be simplified into:
///
/// ```text
/// val + 8
/// ```
fn simplify(expression: &mut Expression) -> sql::Result<()> {
    match expression {
        Expression::UnaryOperation { expr, .. } => {
            simplify(expr)?;
            if let Expression::Value(_) = expr.as_ref() {
                *expression = resolve_literal_expression(expression)?;
            }
        }

        Expression::BinaryOperation {
            left,
            operator,
            right,
        } => {
            simplify(left.as_mut())?;
            simplify(right.as_mut())?;

            match (left.as_mut(), operator, right.as_mut()) {
                (Expression::Value(_), _, Expression::Value(_)) => {
                    *expression = resolve_literal_expression(expression)?;
                }

                // Resolve these expressions to "val":
                // 1 * val
                // val * 1
                // val / 1
                // val + 0
                // val - 0
                // 0 + val
                (
                    Expression::Value(Value::Int(1)),
                    BinaryOperator::Mul,
                    variable @ Expression::Identifier(_),
                )
                | (
                    variable @ Expression::Identifier(_),
                    BinaryOperator::Mul | BinaryOperator::Div,
                    Expression::Value(Value::Int(1)),
                )
                | (
                    variable @ Expression::Identifier(_),
                    BinaryOperator::Sub | BinaryOperator::Add,
                    Expression::Value(Value::Int(0)),
                )
                | (
                    Expression::Value(Value::Int(0)),
                    BinaryOperator::Add,
                    variable @ Expression::Identifier(_),
                ) => {
                    *expression = mem::replace(variable, Expression::Wildcard);
                }

                // Resolve these expressions to 0:
                // 0 * val
                // 0 / val
                // val * 0
                (
                    zero @ Expression::Value(Value::Int(0)),
                    BinaryOperator::Mul | BinaryOperator::Div,
                    Expression::Identifier(_),
                )
                | (
                    Expression::Identifier(_),
                    BinaryOperator::Mul,
                    zero @ Expression::Value(Value::Int(0)),
                ) => {
                    *expression = mem::replace(zero, Expression::Wildcard);
                }

                // Resolve binary operation `0 - x` to unary `-x`.
                (
                    Expression::Value(Value::Int(0)),
                    BinaryOperator::Sub,
                    Expression::Identifier(_),
                ) => match mem::replace(expression, Expression::Wildcard) {
                    Expression::BinaryOperation { right, .. } => {
                        *expression = Expression::UnaryOperation {
                            operator: UnaryOperator::Minus,
                            expr: right,
                        }
                    }
                    _ => unreachable!(),
                },

                // Attempt to simplify expressions like `val + 2 + 4` into `val + 6`.
                (
                    Expression::BinaryOperation {
                        left: variable,
                        operator: BinaryOperator::Add,
                        right: center_value,
                    },
                    BinaryOperator::Add,
                    right_value @ Expression::Value(_),
                ) if matches!(center_value.as_ref(), Expression::Value(_)) => {
                    // Swap "val" with 4.
                    mem::swap(variable.as_mut(), right_value);
                    // Compute 4 + 2.
                    *left.as_mut() = resolve_literal_expression(left)?;
                    // Swap 6 + val to make it val + 6
                    mem::swap(left, right);
                }

                // Turn expressions like `6 + val` into `val + 6` to make them work
                // with the case above.
                (
                    literal @ Expression::Value(_),
                    BinaryOperator::Add,
                    variable @ Expression::Identifier(_),
                ) => {
                    mem::swap(variable, literal);
                }

                _other => {}
            }
        }

        Expression::Nested(nested) => {
            simplify(nested.as_mut())?;
            *expression = mem::replace(nested.as_mut(), Expression::Wildcard);
        }

        Expression::Alias { expr, r#as: _ } => {
            simplify(expr.as_mut())?;
        }

        _ => {}
    };

    Ok(())
}

fn resolve_literal_expression(expression: &Expression) -> sql::Result<Expression> {
    vm::expression::resolve_literal_expression(expression).map(Expression::Value)
}
