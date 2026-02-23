use crate::error::{MuroError, Result};
use crate::sql::ast::*;
use crate::sql::parser::parse_sql;
use crate::types::{format_date, format_datetime, DataType, Value};

/// Parsed SQL template with positional bind parameters (`?`).
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    sql: String,
    template: Statement,
    parameter_count: usize,
}

impl PreparedStatement {
    /// Parse SQL once and keep the AST template for repeated execution.
    pub fn parse(sql: &str) -> Result<Self> {
        let template = parse_sql(sql).map_err(MuroError::Parse)?;
        let parameter_count = count_statement_bind_params(&template);
        Ok(Self {
            sql: sql.to_string(),
            template,
            parameter_count,
        })
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    pub fn bind(&self, params: &[Value]) -> Result<Statement> {
        if params.len() != self.parameter_count {
            return Err(MuroError::Execution(format!(
                "Prepared statement expects {} parameters, got {}",
                self.parameter_count,
                params.len()
            )));
        }

        let mut bound = self.template.clone();
        let mut next = 0usize;
        bind_statement_in_place(&mut bound, params, &mut next)?;
        debug_assert_eq!(next, params.len());
        Ok(bound)
    }
}

pub fn contains_bind_params(stmt: &Statement) -> bool {
    count_statement_bind_params(stmt) > 0
}

fn count_statement_bind_params(stmt: &Statement) -> usize {
    match stmt {
        Statement::CreateTable(ct) => {
            let mut total = 0usize;
            for col in &ct.columns {
                total += col
                    .default_value
                    .as_ref()
                    .map(count_expr_bind_params)
                    .unwrap_or(0);
                total += col
                    .check_expr
                    .as_ref()
                    .map(count_expr_bind_params)
                    .unwrap_or(0);
            }
            total
        }
        Statement::Insert(ins) => {
            let mut total = 0usize;
            for row in &ins.values {
                for expr in row {
                    total += count_expr_bind_params(expr);
                }
            }
            if let Some(dup) = &ins.on_duplicate_key_update {
                for (_, expr) in dup {
                    total += count_expr_bind_params(expr);
                }
            }
            total
        }
        Statement::Select(sel) => count_select_bind_params(sel),
        Statement::Update(upd) => {
            let mut total = 0usize;
            for (_, expr) in &upd.assignments {
                total += count_expr_bind_params(expr);
            }
            total
                + upd
                    .where_clause
                    .as_ref()
                    .map(count_expr_bind_params)
                    .unwrap_or(0)
        }
        Statement::Delete(del) => del
            .where_clause
            .as_ref()
            .map(count_expr_bind_params)
            .unwrap_or(0),
        Statement::SetQuery(sq) => {
            let mut total = count_select_bind_params(&sq.left);
            for (_, sel) in &sq.ops {
                total += count_select_bind_params(sel);
            }
            if let Some(order_by) = &sq.order_by {
                for item in order_by {
                    total += count_expr_bind_params(&item.expr);
                }
            }
            total
        }
        Statement::Explain(inner) => count_statement_bind_params(inner),
        Statement::AlterTable(at) => match &at.operation {
            AlterTableOp::AddColumn(spec)
            | AlterTableOp::ModifyColumn(spec)
            | AlterTableOp::ChangeColumn(_, spec) => {
                spec.default_value
                    .as_ref()
                    .map(count_expr_bind_params)
                    .unwrap_or(0)
                    + spec
                        .check_expr
                        .as_ref()
                        .map(count_expr_bind_params)
                        .unwrap_or(0)
            }
            AlterTableOp::DropColumn(_) => 0,
        },
        Statement::CreateIndex(_)
        | Statement::CreateFulltextIndex(_)
        | Statement::DropTable(_)
        | Statement::DropIndex(_)
        | Statement::RenameTable(_)
        | Statement::ShowTables
        | Statement::ShowCreateTable(_)
        | Statement::Describe(_)
        | Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::ShowCheckpointStats
        | Statement::ShowDatabaseStats
        | Statement::AnalyzeTable(_) => 0,
    }
}

fn count_select_bind_params(sel: &Select) -> usize {
    let mut total = 0usize;
    for col in &sel.columns {
        if let SelectColumn::Expr(expr, _) = col {
            total += count_expr_bind_params(expr);
        }
    }
    for join in &sel.joins {
        if let Some(on) = &join.on_condition {
            total += count_expr_bind_params(on);
        }
    }
    if let Some(where_clause) = &sel.where_clause {
        total += count_expr_bind_params(where_clause);
    }
    if let Some(group_by) = &sel.group_by {
        for expr in group_by {
            total += count_expr_bind_params(expr);
        }
    }
    if let Some(having) = &sel.having {
        total += count_expr_bind_params(having);
    }
    if let Some(order_by) = &sel.order_by {
        for item in order_by {
            total += count_expr_bind_params(&item.expr);
        }
    }
    total
}

fn count_expr_bind_params(expr: &Expr) -> usize {
    match expr {
        Expr::BindParam => 1,
        Expr::BinaryOp { left, right, .. } => {
            count_expr_bind_params(left) + count_expr_bind_params(right)
        }
        Expr::UnaryOp { operand, .. } => count_expr_bind_params(operand),
        Expr::Like { expr, pattern, .. } => {
            count_expr_bind_params(expr) + count_expr_bind_params(pattern)
        }
        Expr::InList { expr, list, .. } => {
            let mut total = count_expr_bind_params(expr);
            for item in list {
                total += count_expr_bind_params(item);
            }
            total
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            count_expr_bind_params(expr)
                + count_expr_bind_params(low)
                + count_expr_bind_params(high)
        }
        Expr::IsNull { expr, .. } => count_expr_bind_params(expr),
        Expr::FunctionCall { args, .. } => args.iter().map(count_expr_bind_params).sum(),
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            let mut total = operand
                .as_ref()
                .map(|e| count_expr_bind_params(e))
                .unwrap_or(0);
            for (when_expr, then_expr) in when_clauses {
                total += count_expr_bind_params(when_expr);
                total += count_expr_bind_params(then_expr);
            }
            total
                + else_clause
                    .as_ref()
                    .map(|e| count_expr_bind_params(e))
                    .unwrap_or(0)
        }
        Expr::Cast { expr, .. } => count_expr_bind_params(expr),
        Expr::AggregateFunc { arg, .. } => {
            arg.as_ref().map(|e| count_expr_bind_params(e)).unwrap_or(0)
        }
        Expr::GreaterThanZero(inner) => count_expr_bind_params(inner),
        Expr::InSubquery {
            expr,
            subquery,
            negated: _,
        } => count_expr_bind_params(expr) + count_select_bind_params(subquery),
        Expr::Exists {
            subquery,
            negated: _,
        }
        | Expr::ScalarSubquery(subquery) => count_select_bind_params(subquery),
        Expr::IntLiteral(_)
        | Expr::FloatLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BlobLiteral(_)
        | Expr::Null
        | Expr::DefaultValue
        | Expr::ColumnRef(_)
        | Expr::MatchAgainst { .. }
        | Expr::FtsSnippet { .. } => 0,
    }
}

fn bind_statement_in_place(stmt: &mut Statement, params: &[Value], next: &mut usize) -> Result<()> {
    match stmt {
        Statement::CreateTable(ct) => {
            for col in &mut ct.columns {
                bind_column_spec_in_place(col, params, next)?;
            }
        }
        Statement::Insert(ins) => {
            for row in &mut ins.values {
                for expr in row {
                    bind_expr_in_place(expr, params, next)?;
                }
            }
            if let Some(dup) = &mut ins.on_duplicate_key_update {
                for (_, expr) in dup {
                    bind_expr_in_place(expr, params, next)?;
                }
            }
        }
        Statement::Select(sel) => bind_select_in_place(sel, params, next)?,
        Statement::Update(upd) => {
            for (_, expr) in &mut upd.assignments {
                bind_expr_in_place(expr, params, next)?;
            }
            if let Some(where_clause) = &mut upd.where_clause {
                bind_expr_in_place(where_clause, params, next)?;
            }
        }
        Statement::Delete(del) => {
            if let Some(where_clause) = &mut del.where_clause {
                bind_expr_in_place(where_clause, params, next)?;
            }
        }
        Statement::SetQuery(sq) => {
            bind_select_in_place(&mut sq.left, params, next)?;
            for (_, sel) in &mut sq.ops {
                bind_select_in_place(sel, params, next)?;
            }
            if let Some(order_by) = &mut sq.order_by {
                for item in order_by {
                    bind_expr_in_place(&mut item.expr, params, next)?;
                }
            }
        }
        Statement::Explain(inner) => bind_statement_in_place(inner, params, next)?,
        Statement::AlterTable(at) => match &mut at.operation {
            AlterTableOp::AddColumn(spec)
            | AlterTableOp::ModifyColumn(spec)
            | AlterTableOp::ChangeColumn(_, spec) => {
                bind_column_spec_in_place(spec, params, next)?;
            }
            AlterTableOp::DropColumn(_) => {}
        },
        Statement::CreateIndex(_)
        | Statement::CreateFulltextIndex(_)
        | Statement::DropTable(_)
        | Statement::DropIndex(_)
        | Statement::RenameTable(_)
        | Statement::ShowTables
        | Statement::ShowCreateTable(_)
        | Statement::Describe(_)
        | Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::ShowCheckpointStats
        | Statement::ShowDatabaseStats
        | Statement::AnalyzeTable(_) => {}
    }

    Ok(())
}

fn bind_column_spec_in_place(
    spec: &mut ColumnSpec,
    params: &[Value],
    next: &mut usize,
) -> Result<()> {
    if spec.constraint_expr_order.is_empty() {
        // Fallback for manually-built specs that do not preserve parse order.
        if let Some(default_value) = &mut spec.default_value {
            bind_expr_in_place(default_value, params, next)?;
        }
        if let Some(check_expr) = &mut spec.check_expr {
            bind_expr_in_place(check_expr, params, next)?;
        }
        return Ok(());
    }

    for kind in &spec.constraint_expr_order {
        match kind {
            ColumnConstraintExprKind::Default => {
                if let Some(default_value) = &mut spec.default_value {
                    bind_expr_in_place(default_value, params, next)?;
                }
            }
            ColumnConstraintExprKind::Check => {
                if let Some(check_expr) = &mut spec.check_expr {
                    bind_expr_in_place(check_expr, params, next)?;
                }
            }
        }
    }

    Ok(())
}

fn bind_select_in_place(sel: &mut Select, params: &[Value], next: &mut usize) -> Result<()> {
    for col in &mut sel.columns {
        if let SelectColumn::Expr(expr, _) = col {
            bind_expr_in_place(expr, params, next)?;
        }
    }
    for join in &mut sel.joins {
        if let Some(on) = &mut join.on_condition {
            bind_expr_in_place(on, params, next)?;
        }
    }
    if let Some(where_clause) = &mut sel.where_clause {
        bind_expr_in_place(where_clause, params, next)?;
    }
    if let Some(group_by) = &mut sel.group_by {
        for expr in group_by {
            bind_expr_in_place(expr, params, next)?;
        }
    }
    if let Some(having) = &mut sel.having {
        bind_expr_in_place(having, params, next)?;
    }
    if let Some(order_by) = &mut sel.order_by {
        for item in order_by {
            bind_expr_in_place(&mut item.expr, params, next)?;
        }
    }

    Ok(())
}

fn bind_expr_in_place(expr: &mut Expr, params: &[Value], next: &mut usize) -> Result<()> {
    match expr {
        Expr::BindParam => {
            let idx = *next;
            let value = params.get(idx).ok_or_else(|| {
                MuroError::Execution(format!("Missing bind parameter at position {}", idx + 1))
            })?;
            *expr = value_to_expr(value);
            *next += 1;
        }
        Expr::BinaryOp { left, right, .. } => {
            bind_expr_in_place(left, params, next)?;
            bind_expr_in_place(right, params, next)?;
        }
        Expr::UnaryOp { operand, .. } => bind_expr_in_place(operand, params, next)?,
        Expr::Like { expr, pattern, .. } => {
            bind_expr_in_place(expr, params, next)?;
            bind_expr_in_place(pattern, params, next)?;
        }
        Expr::InList { expr, list, .. } => {
            bind_expr_in_place(expr, params, next)?;
            for item in list {
                bind_expr_in_place(item, params, next)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            bind_expr_in_place(expr, params, next)?;
            bind_expr_in_place(low, params, next)?;
            bind_expr_in_place(high, params, next)?;
        }
        Expr::IsNull { expr, .. } => bind_expr_in_place(expr, params, next)?,
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                bind_expr_in_place(arg, params, next)?;
            }
        }
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                bind_expr_in_place(operand, params, next)?;
            }
            for (when_expr, then_expr) in when_clauses {
                bind_expr_in_place(when_expr, params, next)?;
                bind_expr_in_place(then_expr, params, next)?;
            }
            if let Some(else_expr) = else_clause {
                bind_expr_in_place(else_expr, params, next)?;
            }
        }
        Expr::Cast { expr, .. } => bind_expr_in_place(expr, params, next)?,
        Expr::AggregateFunc { arg, .. } => {
            if let Some(arg) = arg {
                bind_expr_in_place(arg, params, next)?;
            }
        }
        Expr::GreaterThanZero(inner) => bind_expr_in_place(inner, params, next)?,
        Expr::InSubquery {
            expr,
            subquery,
            negated: _,
        } => {
            bind_expr_in_place(expr, params, next)?;
            bind_select_in_place(subquery, params, next)?;
        }
        Expr::Exists {
            subquery,
            negated: _,
        }
        | Expr::ScalarSubquery(subquery) => bind_select_in_place(subquery, params, next)?,
        Expr::IntLiteral(_)
        | Expr::FloatLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BlobLiteral(_)
        | Expr::Null
        | Expr::DefaultValue
        | Expr::ColumnRef(_)
        | Expr::MatchAgainst { .. }
        | Expr::FtsSnippet { .. } => {}
    }

    Ok(())
}

fn value_to_expr(v: &Value) -> Expr {
    match v {
        Value::Integer(n) => Expr::IntLiteral(*n),
        Value::Float(n) => Expr::FloatLiteral(*n),
        Value::Decimal(d) => {
            let normalized = d.normalize();
            let scale = normalized.scale();
            let mut precision = normalized
                .to_string()
                .chars()
                .filter(|c| c.is_ascii_digit())
                .count() as u32;
            if precision == 0 {
                precision = 1;
            }
            if precision < scale {
                precision = scale;
            }
            Expr::Cast {
                expr: Box::new(Expr::StringLiteral(normalized.to_string())),
                target_type: DataType::Decimal(precision, scale),
            }
        }
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
        Value::Uuid(b) => Expr::Cast {
            expr: Box::new(Expr::StringLiteral(crate::types::format_uuid(b))),
            target_type: DataType::Uuid,
        },
        Value::Null => Expr::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bind_param_count_mismatch() {
        let prepared = PreparedStatement::parse("SELECT ? + ?").unwrap();
        let err = prepared.bind(&[Value::Integer(1)]).unwrap_err();
        assert!(format!("{err}").contains("expects 2 parameters"));
    }

    #[test]
    fn test_bind_params_in_order() {
        let prepared = PreparedStatement::parse("SELECT ? + ?").unwrap();
        let stmt = prepared
            .bind(&[Value::Integer(2), Value::Integer(3)])
            .unwrap();

        let Statement::Select(sel) = stmt else {
            panic!("expected SELECT");
        };
        let SelectColumn::Expr(expr, _) = &sel.columns[0] else {
            panic!("expected expression column");
        };
        let Expr::BinaryOp { left, right, .. } = expr else {
            panic!("expected binary expression");
        };
        assert!(matches!(left.as_ref(), Expr::IntLiteral(2)));
        assert!(matches!(right.as_ref(), Expr::IntLiteral(3)));
    }

    #[test]
    fn test_bind_order_follows_column_constraint_parse_order() {
        let prepared =
            PreparedStatement::parse("CREATE TABLE t (c INT CHECK (?) DEFAULT ?)").unwrap();
        let stmt = prepared
            .bind(&[Value::Integer(11), Value::Integer(22)])
            .unwrap();

        let Statement::CreateTable(ct) = stmt else {
            panic!("expected CREATE TABLE");
        };
        let col = &ct.columns[0];
        assert!(matches!(col.check_expr, Some(Expr::IntLiteral(11))));
        assert!(matches!(col.default_value, Some(Expr::IntLiteral(22))));
    }

    #[test]
    fn test_bind_order_follows_alter_column_constraint_parse_order() {
        let prepared =
            PreparedStatement::parse("ALTER TABLE t ADD COLUMN c INT CHECK (?) DEFAULT ?").unwrap();
        let stmt = prepared
            .bind(&[Value::Integer(7), Value::Integer(8)])
            .unwrap();

        let Statement::AlterTable(at) = stmt else {
            panic!("expected ALTER TABLE");
        };
        let AlterTableOp::AddColumn(col) = at.operation else {
            panic!("expected ADD COLUMN");
        };
        assert!(matches!(col.check_expr, Some(Expr::IntLiteral(7))));
        assert!(matches!(col.default_value, Some(Expr::IntLiteral(8))));
    }
}
