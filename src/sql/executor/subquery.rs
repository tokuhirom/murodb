use super::*;

pub(super) fn value_to_expr(v: &Value) -> Expr {
    match v {
        Value::Integer(n) => Expr::IntLiteral(*n),
        Value::Float(n) => Expr::FloatLiteral(*n),
        Value::Date(n) => Expr::Cast {
            expr: Box::new(Expr::StringLiteral(format_date(*n))),
            target_type: DataType::Date,
        },
        Value::DateTime(n) => Expr::Cast {
            expr: Box::new(Expr::StringLiteral(format_datetime(*n))),
            target_type: DataType::DateTime,
        },
        Value::Timestamp(n) => Expr::Cast {
            expr: Box::new(Expr::StringLiteral(format_datetime(*n))),
            target_type: DataType::Timestamp,
        },
        Value::Varchar(s) => Expr::StringLiteral(s.clone()),
        Value::Varbinary(b) => Expr::BlobLiteral(b.clone()),
        Value::Null => Expr::Null,
    }
}

/// Execute a subquery SELECT and return all result rows.
pub(super) fn execute_subquery(
    sel: &Select,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<Vec<Vec<Value>>> {
    let result = exec_select(sel, pager, catalog)?;
    match result {
        ExecResult::Rows(rows) => Ok(rows
            .into_iter()
            .map(|r| r.values.into_iter().map(|(_, v)| v).collect())
            .collect()),
        _ => Err(MuroError::Execution("Subquery did not return rows".into())),
    }
}

/// Pre-materialize all subqueries in an expression tree.
/// Replaces InSubquery with InList, Exists with IntLiteral, ScalarSubquery with literal.
pub(super) fn materialize_subqueries(
    expr: &Expr,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<Expr> {
    match expr {
        Expr::InSubquery {
            expr: left,
            subquery,
            negated,
        } => {
            let left = materialize_subqueries(left, pager, catalog)?;
            let rows = execute_subquery(subquery, pager, catalog)?;
            let list: Vec<Expr> = rows
                .into_iter()
                .map(|row| {
                    if row.len() != 1 {
                        return Err(MuroError::Execution(
                            "Subquery in IN must return exactly one column".into(),
                        ));
                    }
                    Ok(value_to_expr(&row[0]))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::InList {
                expr: Box::new(left),
                list,
                negated: *negated,
            })
        }
        Expr::Exists { subquery, negated } => {
            // Inject LIMIT 1 to short-circuit: only need to know if any rows exist
            let mut limited = subquery.as_ref().clone();
            limited.limit = Some(1);
            let rows = execute_subquery(&limited, pager, catalog)?;
            let exists = !rows.is_empty();
            let val = if *negated { !exists } else { exists };
            Ok(Expr::IntLiteral(if val { 1 } else { 0 }))
        }
        Expr::ScalarSubquery(subquery) => {
            let rows = execute_subquery(subquery, pager, catalog)?;
            match rows.len() {
                0 => Ok(Expr::Null),
                1 => {
                    if rows[0].len() != 1 {
                        return Err(MuroError::Execution(
                            "Scalar subquery must return exactly one column".into(),
                        ));
                    }
                    Ok(value_to_expr(&rows[0][0]))
                }
                _ => Err(MuroError::Execution(
                    "Scalar subquery returned more than one row".into(),
                )),
            }
        }
        // Recurse into sub-expressions
        Expr::BinaryOp { left, op, right } => {
            let l = materialize_subqueries(left, pager, catalog)?;
            let r = materialize_subqueries(right, pager, catalog)?;
            Ok(Expr::BinaryOp {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            })
        }
        Expr::UnaryOp { op, operand } => {
            let o = materialize_subqueries(operand, pager, catalog)?;
            Ok(Expr::UnaryOp {
                op: *op,
                operand: Box::new(o),
            })
        }
        Expr::Like {
            expr: e,
            pattern,
            negated,
        } => {
            let e2 = materialize_subqueries(e, pager, catalog)?;
            let p2 = materialize_subqueries(pattern, pager, catalog)?;
            Ok(Expr::Like {
                expr: Box::new(e2),
                pattern: Box::new(p2),
                negated: *negated,
            })
        }
        Expr::InList {
            expr: e,
            list,
            negated,
        } => {
            let e2 = materialize_subqueries(e, pager, catalog)?;
            let list2: Vec<Expr> = list
                .iter()
                .map(|item| materialize_subqueries(item, pager, catalog))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::InList {
                expr: Box::new(e2),
                list: list2,
                negated: *negated,
            })
        }
        Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } => {
            let e2 = materialize_subqueries(e, pager, catalog)?;
            let l2 = materialize_subqueries(low, pager, catalog)?;
            let h2 = materialize_subqueries(high, pager, catalog)?;
            Ok(Expr::Between {
                expr: Box::new(e2),
                low: Box::new(l2),
                high: Box::new(h2),
                negated: *negated,
            })
        }
        Expr::IsNull { expr: e, negated } => {
            let e2 = materialize_subqueries(e, pager, catalog)?;
            Ok(Expr::IsNull {
                expr: Box::new(e2),
                negated: *negated,
            })
        }
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            let op2 = operand
                .as_ref()
                .map(|o| materialize_subqueries(o, pager, catalog).map(Box::new))
                .transpose()?;
            let wc2: Vec<(Expr, Expr)> = when_clauses
                .iter()
                .map(|(c, t)| {
                    Ok((
                        materialize_subqueries(c, pager, catalog)?,
                        materialize_subqueries(t, pager, catalog)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let ec2 = else_clause
                .as_ref()
                .map(|e| materialize_subqueries(e, pager, catalog).map(Box::new))
                .transpose()?;
            Ok(Expr::CaseWhen {
                operand: op2,
                when_clauses: wc2,
                else_clause: ec2,
            })
        }
        Expr::Cast {
            expr: e,
            target_type,
        } => {
            let e2 = materialize_subqueries(e, pager, catalog)?;
            Ok(Expr::Cast {
                expr: Box::new(e2),
                target_type: *target_type,
            })
        }
        Expr::FunctionCall { name, args } => {
            let args2: Vec<Expr> = args
                .iter()
                .map(|a| materialize_subqueries(a, pager, catalog))
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::FunctionCall {
                name: name.clone(),
                args: args2,
            })
        }
        Expr::AggregateFunc {
            name,
            arg,
            distinct,
        } => {
            let arg2 = arg
                .as_ref()
                .map(|a| materialize_subqueries(a, pager, catalog).map(Box::new))
                .transpose()?;
            Ok(Expr::AggregateFunc {
                name: name.clone(),
                arg: arg2,
                distinct: *distinct,
            })
        }
        Expr::GreaterThanZero(inner) => {
            let i2 = materialize_subqueries(inner, pager, catalog)?;
            Ok(Expr::GreaterThanZero(Box::new(i2)))
        }
        // Leaf nodes — no subqueries possible, return clone
        _ => Ok(expr.clone()),
    }
}

/// Check if an expression tree contains any subquery nodes.
pub(super) fn expr_contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::ScalarSubquery(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        Expr::UnaryOp { operand, .. } => expr_contains_subquery(operand),
        Expr::Like { expr, pattern, .. } => {
            expr_contains_subquery(expr) || expr_contains_subquery(pattern)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_subquery(expr) || list.iter().any(expr_contains_subquery)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_subquery(expr)
                || expr_contains_subquery(low)
                || expr_contains_subquery(high)
        }
        Expr::IsNull { expr, .. } => expr_contains_subquery(expr),
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand.as_deref().is_some_and(expr_contains_subquery)
                || when_clauses
                    .iter()
                    .any(|(c, t)| expr_contains_subquery(c) || expr_contains_subquery(t))
                || else_clause.as_deref().is_some_and(expr_contains_subquery)
        }
        Expr::Cast { expr, .. } => expr_contains_subquery(expr),
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_subquery),
        Expr::AggregateFunc { arg, .. } => arg.as_deref().is_some_and(expr_contains_subquery),
        Expr::GreaterThanZero(inner) => expr_contains_subquery(inner),
        _ => false,
    }
}

/// Check if a Select's columns contain any subqueries.
pub(super) fn select_columns_contain_subquery(columns: &[SelectColumn]) -> bool {
    columns.iter().any(|col| match col {
        SelectColumn::Star => false,
        SelectColumn::Expr(e, _) => expr_contains_subquery(e),
    })
}

/// Materialize subqueries in a Select, returning a modified Select.
pub(super) fn materialize_select_subqueries(
    sel: &Select,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<Select> {
    let where_clause = sel
        .where_clause
        .as_ref()
        .map(|e| materialize_subqueries(e, pager, catalog))
        .transpose()?;

    let columns: Vec<SelectColumn> = sel
        .columns
        .iter()
        .map(|col| match col {
            SelectColumn::Star => Ok(SelectColumn::Star),
            SelectColumn::Expr(e, alias) => {
                let e2 = materialize_subqueries(e, pager, catalog)?;
                Ok(SelectColumn::Expr(e2, alias.clone()))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let having = sel
        .having
        .as_ref()
        .map(|e| materialize_subqueries(e, pager, catalog))
        .transpose()?;

    Ok(Select {
        distinct: sel.distinct,
        columns,
        table_name: sel.table_name.clone(),
        table_alias: sel.table_alias.clone(),
        joins: sel.joins.clone(),
        where_clause,
        group_by: sel.group_by.clone(),
        having,
        order_by: sel.order_by.clone(),
        limit: sel.limit,
        offset: sel.offset,
    })
}
