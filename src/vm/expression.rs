use crate::sql::{
    self,
    parser::statement::{BinaryOperator, Expression, UnaryOperator},
    record::Record,
    schema::Schema,
    types::Value,
};

pub fn resolve_expression_to_value(
    record: &Record<'_>,
    schema: &Schema,
    expr: &Expression,
) -> sql::Result<Value> {
    match expr {
        Expression::Value(val) => Ok(val.clone()),

        Expression::Identifier(ident) => match schema.index_of(ident) {
            Some(idx) => Ok(record.get_value(idx).to_owned()),
            None => Err(sql::analyzer::Error::ColumnNotFound(ident.to_owned()).into()),
        },

        Expression::UnaryOperation { operator, expr } => {
            let value = resolve_expression_to_value(record, schema, expr)?;

            if let Value::Int(int) = value {
                match operator {
                    UnaryOperator::Plus => Ok(value),
                    UnaryOperator::Minus => Ok(Value::Int(-int)),
                }
            } else {
                Err(sql::analyzer::TypeError::CannotApplyUnary {
                    operator: *operator,
                    value,
                }
                .into())
            }
        }
        Expression::BinaryOperation {
            left,
            operator,
            right,
        } => {
            let left = resolve_expression_to_value(record, schema, left)?;
            let right = resolve_expression_to_value(record, schema, right)?;

            let mismatched_types = || {
                sql::Error::TypeError(sql::analyzer::TypeError::CannotApplyBinary {
                    left: Expression::Value(left.clone()),
                    operator: *operator,
                    right: Expression::Value(right.clone()),
                })
            };

            Ok(match operator {
                // arithmetic
                BinaryOperator::Add => left + right,
                BinaryOperator::Sub => left - right,
                BinaryOperator::Mul => left * right,
                BinaryOperator::Div => left / right,

                // comparison
                BinaryOperator::Eq => Value::Bool(left == right),
                BinaryOperator::Neq => Value::Bool(left != right),
                BinaryOperator::Lt => Value::Bool(left < right),
                BinaryOperator::LtEq => Value::Bool(left <= right),
                BinaryOperator::Gt => Value::Bool(left > right),
                BinaryOperator::GtEq => Value::Bool(left >= right),

                // logical
                logical @ (BinaryOperator::And | BinaryOperator::Or) => {
                    let (Value::Bool(left), Value::Bool(right)) = (&left, &right) else {
                        return Err(mismatched_types());
                    };

                    match logical {
                        BinaryOperator::And => Value::Bool(*left && *right),
                        BinaryOperator::Or => Value::Bool(*left || *right),
                        _ => unreachable!(),
                    }
                }

                _ => unreachable!(),
            })
        }

        Expression::Nested(expr) => resolve_expression_to_value(record, schema, expr),

        Expression::Alias { expr, r#as: _ } => resolve_expression_to_value(record, schema, expr),

        _ => unreachable!(),
    }
}

pub fn resolve_literal_expression(expr: &Expression) -> sql::Result<Value> {
    resolve_expression_to_value(&Record::empty(), &Schema::empty(), expr)
}
