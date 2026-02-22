/// SQL parser: converts token stream into AST.
/// Hand-written recursive descent parser.
use crate::sql::ast::*;
use crate::sql::lexer::Token;
use crate::types::DataType;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let token = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(token)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: &Token) -> Result<(), String> {
        match self.advance() {
            Some(ref t) if t == expected => Ok(()),
            Some(t) => Err(format!("Expected {:?}, got {:?}", expected, t)),
            None => Err(format!("Expected {:?}, got end of input", expected)),
        }
    }

    fn expect_ident(&mut self) -> Result<String, String> {
        match self.advance() {
            Some(Token::Ident(s)) => Ok(s),
            // Allow aggregate keywords to be used as identifiers (column names, aliases)
            Some(Token::Count) => Ok("count".to_string()),
            Some(Token::Sum) => Ok("sum".to_string()),
            Some(Token::Avg) => Ok("avg".to_string()),
            Some(Token::Min) => Ok("min".to_string()),
            Some(Token::Max) => Ok("max".to_string()),
            Some(Token::Group) => Ok("group".to_string()),
            Some(Token::Having) => Ok("having".to_string()),
            Some(Token::Distinct) => Ok("distinct".to_string()),
            Some(Token::Add) => Ok("add".to_string()),
            Some(Token::Column) => Ok("column".to_string()),
            Some(Token::Modify) => Ok("modify".to_string()),
            Some(Token::Change) => Ok("change".to_string()),
            Some(Token::Rename) => Ok("rename".to_string()),
            Some(Token::To) => Ok("to".to_string()),
            Some(Token::Key) => Ok("key".to_string()),
            Some(Token::Replace) => Ok("replace".to_string()),
            Some(Token::Duplicate) => Ok("duplicate".to_string()),
            Some(Token::Explain) => Ok("explain".to_string()),
            Some(Token::Analyze) => Ok("analyze".to_string()),
            Some(t) => Err(format!("Expected identifier, got {:?}", t)),
            None => Err("Expected identifier, got end of input".into()),
        }
    }

    pub fn parse(&mut self) -> Result<Statement, String> {
        let stmt = match self.peek() {
            Some(Token::Create) => self.parse_create()?,
            Some(Token::Drop) => self.parse_drop()?,
            Some(Token::Select) => self.parse_select_or_union()?,
            Some(Token::Insert) => Statement::Insert(self.parse_insert(false)?),
            Some(Token::Replace) => Statement::Insert(self.parse_insert(true)?),
            Some(Token::Explain) => {
                self.advance(); // EXPLAIN
                let inner = self.parse()?;
                return Ok(Statement::Explain(Box::new(inner)));
            }
            Some(Token::Update) => Statement::Update(self.parse_update()?),
            Some(Token::Delete) => Statement::Delete(self.parse_delete()?),
            Some(Token::Analyze) => self.parse_analyze()?,
            Some(Token::Alter) => self.parse_alter()?,
            Some(Token::Rename) => self.parse_rename()?,
            Some(Token::Show) => self.parse_show()?,
            Some(Token::Describe) => {
                self.advance();
                let table_name = self.expect_ident()?;
                Statement::Describe(table_name)
            }
            Some(Token::Desc) => {
                // DESC can be DESCRIBE (statement) if followed by an identifier
                // But DESC is also ORDER BY direction, handled elsewhere
                // At statement level, treat as DESCRIBE
                self.advance();
                let table_name = self.expect_ident()?;
                Statement::Describe(table_name)
            }
            Some(Token::Begin) => {
                self.advance();
                Statement::Begin
            }
            Some(Token::Commit) => {
                self.advance();
                Statement::Commit
            }
            Some(Token::Rollback) => {
                self.advance();
                Statement::Rollback
            }
            Some(t) => return Err(format!("Unexpected token: {:?}", t)),
            None => return Err("Empty input".into()),
        };

        // Skip optional trailing semicolon
        if self.peek() == Some(&Token::Semicolon) {
            self.advance();
        }

        Ok(stmt)
    }

    fn parse_analyze(&mut self) -> Result<Statement, String> {
        self.advance(); // ANALYZE
        self.expect(&Token::Table)?;
        let table_name = self.expect_ident()?;
        Ok(Statement::AnalyzeTable(table_name))
    }

    fn parse_create(&mut self) -> Result<Statement, String> {
        self.advance(); // consume CREATE

        match self.peek() {
            Some(Token::Table) => {
                self.advance();
                let if_not_exists = self.parse_if_not_exists()?;
                let mut ct = self.parse_create_table()?;
                ct.if_not_exists = if_not_exists;
                Ok(Statement::CreateTable(ct))
            }
            Some(Token::Unique) => {
                self.advance();
                self.expect(&Token::Index)?;
                let if_not_exists = self.parse_if_not_exists()?;
                let mut ci = self.parse_create_index(true)?;
                ci.if_not_exists = if_not_exists;
                Ok(Statement::CreateIndex(ci))
            }
            Some(Token::Index) => {
                self.advance();
                let if_not_exists = self.parse_if_not_exists()?;
                let mut ci = self.parse_create_index(false)?;
                ci.if_not_exists = if_not_exists;
                Ok(Statement::CreateIndex(ci))
            }
            Some(Token::Fulltext) => {
                self.advance();
                self.expect(&Token::Index)?;
                Ok(Statement::CreateFulltextIndex(
                    self.parse_create_fulltext_index()?,
                ))
            }
            _ => Err("Expected TABLE, INDEX, UNIQUE INDEX, or FULLTEXT INDEX after CREATE".into()),
        }
    }

    fn parse_if_not_exists(&mut self) -> Result<bool, String> {
        if self.peek() == Some(&Token::If) {
            self.advance(); // IF
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_if_exists(&mut self) -> Result<bool, String> {
        if self.peek() == Some(&Token::If) {
            self.advance(); // IF
            self.expect(&Token::Exists)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_drop(&mut self) -> Result<Statement, String> {
        self.advance(); // DROP

        match self.peek() {
            Some(Token::Table) => {
                self.advance();
                let if_exists = self.parse_if_exists()?;
                let table_name = self.expect_ident()?;
                Ok(Statement::DropTable(DropTable {
                    table_name,
                    if_exists,
                }))
            }
            Some(Token::Index) => {
                self.advance();
                let if_exists = self.parse_if_exists()?;
                let index_name = self.expect_ident()?;
                Ok(Statement::DropIndex(DropIndex {
                    index_name,
                    if_exists,
                }))
            }
            _ => Err("Expected TABLE or INDEX after DROP".into()),
        }
    }

    fn parse_alter(&mut self) -> Result<Statement, String> {
        self.advance(); // ALTER
        self.expect(&Token::Table)?;
        let table_name = self.expect_ident()?;

        let operation = match self.peek() {
            Some(Token::Add) => {
                self.advance(); // ADD
                                // Optional COLUMN keyword
                if self.peek() == Some(&Token::Column) {
                    self.advance();
                }
                let col_spec = self.parse_column_spec()?;
                AlterTableOp::AddColumn(col_spec)
            }
            Some(Token::Drop) => {
                self.advance(); // DROP
                self.expect(&Token::Column)?;
                let col_name = self.expect_ident()?;
                AlterTableOp::DropColumn(col_name)
            }
            Some(Token::Modify) => {
                self.advance(); // MODIFY
                                // Optional COLUMN keyword
                if self.peek() == Some(&Token::Column) {
                    self.advance();
                }
                let col_spec = self.parse_column_spec()?;
                AlterTableOp::ModifyColumn(col_spec)
            }
            Some(Token::Change) => {
                self.advance(); // CHANGE
                                // Optional COLUMN keyword
                if self.peek() == Some(&Token::Column) {
                    self.advance();
                }
                let old_name = self.expect_ident()?;
                let col_spec = self.parse_column_spec()?;
                AlterTableOp::ChangeColumn(old_name, col_spec)
            }
            _ => {
                return Err("Expected ADD, DROP, MODIFY, or CHANGE after ALTER TABLE <name>".into())
            }
        };

        Ok(Statement::AlterTable(AlterTable {
            table_name,
            operation,
        }))
    }

    fn parse_rename(&mut self) -> Result<Statement, String> {
        self.advance(); // RENAME
        self.expect(&Token::Table)?;
        let old_name = self.expect_ident()?;
        self.expect(&Token::To)?;
        let new_name = self.expect_ident()?;
        Ok(Statement::RenameTable(RenameTable { old_name, new_name }))
    }

    fn parse_create_table(&mut self) -> Result<CreateTable, String> {
        let table_name = self.expect_ident()?;
        self.expect(&Token::LParen)?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        loop {
            // Check for table-level constraints before trying to parse a column
            match self.peek() {
                Some(Token::PrimaryKey) => {
                    self.advance(); // PRIMARY KEY
                    self.expect(&Token::LParen)?;
                    let cols = self.parse_ident_list()?;
                    self.expect(&Token::RParen)?;
                    constraints.push(TableConstraint::PrimaryKey(cols));

                    match self.peek() {
                        Some(Token::Comma) => {
                            self.advance();
                        }
                        Some(Token::RParen) => {
                            self.advance();
                            break;
                        }
                        _ => return Err("Expected ',' or ')' after table constraint".into()),
                    }
                    continue;
                }
                Some(Token::Unique) => {
                    // Could be UNIQUE(col1, col2) table constraint or column named "unique" (unlikely)
                    // Peek ahead: UNIQUE followed by LParen means table constraint
                    if self.tokens.get(self.pos + 1) == Some(&Token::LParen) {
                        self.advance(); // UNIQUE
                        self.expect(&Token::LParen)?;
                        let cols = self.parse_ident_list()?;
                        self.expect(&Token::RParen)?;
                        constraints.push(TableConstraint::Unique(None, cols));

                        match self.peek() {
                            Some(Token::Comma) => {
                                self.advance();
                            }
                            Some(Token::RParen) => {
                                self.advance();
                                break;
                            }
                            _ => return Err("Expected ',' or ')' after table constraint".into()),
                        }
                        continue;
                    }
                }
                _ => {}
            }

            let col = self.parse_column_spec()?;
            columns.push(col);

            match self.peek() {
                Some(Token::Comma) => {
                    self.advance();
                }
                Some(Token::RParen) => {
                    self.advance();
                    break;
                }
                _ => return Err("Expected ',' or ')' in column list".into()),
            }
        }

        Ok(CreateTable {
            table_name,
            columns,
            constraints,
            if_not_exists: false,
        })
    }

    fn parse_ident_list(&mut self) -> Result<Vec<String>, String> {
        let mut names = Vec::new();
        names.push(self.expect_ident()?);
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            names.push(self.expect_ident()?);
        }
        Ok(names)
    }

    fn parse_column_spec(&mut self) -> Result<ColumnSpec, String> {
        let name = self.expect_ident()?;
        let data_type = self.parse_data_type()?;

        let mut is_primary_key = false;
        let mut is_unique = false;
        let mut is_nullable = true;
        let mut default_value = None;
        let mut auto_increment = false;
        let mut check_expr = None;

        loop {
            match self.peek() {
                Some(Token::PrimaryKey) => {
                    self.advance();
                    is_primary_key = true;
                    is_nullable = false;
                }
                Some(Token::Unique) => {
                    self.advance();
                    is_unique = true;
                }
                Some(Token::Not) => {
                    self.advance();
                    self.expect(&Token::Null)?;
                    is_nullable = false;
                }
                Some(Token::Default) => {
                    self.advance();
                    default_value = Some(self.parse_primary()?);
                }
                Some(Token::AutoIncrement) => {
                    self.advance();
                    auto_increment = true;
                }
                Some(Token::Check) => {
                    self.advance();
                    self.expect(&Token::LParen)?;
                    check_expr = Some(self.parse_expr()?);
                    self.expect(&Token::RParen)?;
                }
                _ => break,
            }
        }

        Ok(ColumnSpec {
            name,
            data_type,
            is_primary_key,
            is_unique,
            is_nullable,
            default_value,
            auto_increment,
            check_expr,
        })
    }

    fn parse_data_type(&mut self) -> Result<DataType, String> {
        match self.peek() {
            Some(Token::Boolean) => {
                self.advance();
                // BOOLEAN is alias for TINYINT
                // But we need to check it's not "BOOLEAN MODE" context
                // At this point, we're parsing a data type, so it's definitely BOOLEAN type
                Ok(DataType::TinyInt)
            }
            _ => match self.advance() {
                Some(Token::TinyIntType) => Ok(DataType::TinyInt),
                Some(Token::SmallIntType) => Ok(DataType::SmallInt),
                Some(Token::IntType) => Ok(DataType::Int),
                Some(Token::BigIntType) => Ok(DataType::BigInt),
                Some(Token::FloatType) => Ok(DataType::Float),
                Some(Token::DoubleType) => Ok(DataType::Double),
                Some(Token::DateType) => Ok(DataType::Date),
                Some(Token::DateTimeType) => Ok(DataType::DateTime),
                Some(Token::TimestampType) => Ok(DataType::Timestamp),
                Some(Token::VarcharType) => {
                    let size = self.parse_optional_size()?;
                    Ok(DataType::Varchar(size))
                }
                Some(Token::VarbinaryType) => {
                    let size = self.parse_optional_size()?;
                    Ok(DataType::Varbinary(size))
                }
                Some(Token::TextType) => Ok(DataType::Text),
                Some(t) => Err(format!("Expected data type, got {:?}", t)),
                None => Err("Expected data type".into()),
            },
        }
    }

    fn parse_optional_size(&mut self) -> Result<Option<u32>, String> {
        if self.peek() == Some(&Token::LParen) {
            self.advance(); // (
            let n = match self.advance() {
                Some(Token::Integer(n)) => {
                    if n < 0 {
                        return Err("Size must be a positive integer".into());
                    }
                    n as u32
                }
                _ => return Err("Expected integer size".into()),
            };
            self.expect(&Token::RParen)?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    fn parse_create_index(&mut self, is_unique: bool) -> Result<CreateIndex, String> {
        let index_name = self.expect_ident()?;
        self.expect(&Token::On)?;
        let table_name = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let column_names = self.parse_ident_list()?;
        self.expect(&Token::RParen)?;

        Ok(CreateIndex {
            index_name,
            table_name,
            column_names,
            is_unique,
            if_not_exists: false,
        })
    }

    fn parse_create_fulltext_index(&mut self) -> Result<CreateFulltextIndex, String> {
        let index_name = self.expect_ident()?;
        self.expect(&Token::On)?;
        let table_name = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let column_name = self.expect_ident()?;
        self.expect(&Token::RParen)?;

        // WITH PARSER ngram
        let mut parser = "ngram".to_string();
        let mut ngram_n = 2;
        let mut normalize = "nfkc".to_string();

        if self.peek() == Some(&Token::With) {
            self.advance();
            self.expect(&Token::Parser)?;
            parser = self.expect_ident()?;
        }

        // OPTIONS (n=2, normalize='nfkc')
        if self.peek() == Some(&Token::Options) {
            self.advance();
            self.expect(&Token::LParen)?;
            loop {
                let key = self.expect_ident()?;
                self.expect(&Token::Eq)?;
                match key.as_str() {
                    "n" => {
                        if let Some(Token::Integer(n)) = self.advance() {
                            ngram_n = n as usize;
                        }
                    }
                    "normalize" => {
                        if let Some(Token::StringLit(s)) = self.advance() {
                            normalize = s;
                        }
                    }
                    _ => return Err(format!("Unknown option: {}", key)),
                }
                if self.peek() == Some(&Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
        }

        Ok(CreateFulltextIndex {
            index_name,
            table_name,
            column_name,
            parser,
            ngram_n,
            normalize,
        })
    }

    fn parse_show(&mut self) -> Result<Statement, String> {
        self.advance(); // consume SHOW

        match self.peek() {
            Some(Token::Tables) => {
                self.advance();
                Ok(Statement::ShowTables)
            }
            Some(Token::Create) => {
                self.advance(); // CREATE
                self.expect(&Token::Table)?;
                let table_name = self.expect_ident()?;
                Ok(Statement::ShowCreateTable(table_name))
            }
            Some(Token::Checkpoint) => {
                self.advance(); // CHECKPOINT
                self.expect(&Token::Stats)?;
                Ok(Statement::ShowCheckpointStats)
            }
            Some(Token::Database) => {
                self.advance(); // DATABASE
                self.expect(&Token::Stats)?;
                Ok(Statement::ShowDatabaseStats)
            }
            _ => Err(
                "Expected TABLES, CREATE TABLE, CHECKPOINT STATS, or DATABASE STATS after SHOW"
                    .into(),
            ),
        }
    }

    fn parse_insert(&mut self, is_replace: bool) -> Result<Insert, String> {
        self.advance(); // INSERT or REPLACE
        self.expect(&Token::Into)?;
        let table_name = self.expect_ident()?;

        // Optional column list
        let columns = if self.peek() == Some(&Token::LParen) {
            self.advance();
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                match self.peek() {
                    Some(Token::Comma) => {
                        self.advance();
                    }
                    Some(Token::RParen) => {
                        self.advance();
                        break;
                    }
                    _ => return Err("Expected ',' or ')' in column list".into()),
                }
            }
            Some(cols)
        } else {
            None
        };

        self.expect(&Token::Values)?;

        let mut values = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_expr()?);
                match self.peek() {
                    Some(Token::Comma) => {
                        self.advance();
                    }
                    Some(Token::RParen) => {
                        self.advance();
                        break;
                    }
                    _ => return Err("Expected ',' or ')' in values list".into()),
                }
            }
            values.push(row);

            if self.peek() == Some(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Parse optional ON DUPLICATE KEY UPDATE
        let on_duplicate_key_update = if !is_replace && self.peek() == Some(&Token::On) {
            self.advance(); // ON
            self.expect(&Token::Duplicate)?;
            self.expect(&Token::Key)?;
            self.expect(&Token::Update)?;
            let mut assignments = Vec::new();
            loop {
                let col = self.expect_ident()?;
                self.expect(&Token::Eq)?;
                let val = self.parse_expr()?;
                assignments.push((col, val));
                if self.peek() == Some(&Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            Some(assignments)
        } else {
            None
        };

        Ok(Insert {
            table_name,
            columns,
            values,
            on_duplicate_key_update,
            is_replace,
        })
    }

    fn parse_select(&mut self) -> Result<Select, String> {
        self.advance(); // SELECT

        // Parse optional DISTINCT
        let distinct = if self.peek() == Some(&Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        let columns = self.parse_select_columns()?;

        let (table_name, table_alias) = if self.peek() == Some(&Token::From) {
            self.advance();
            let table_name = self.expect_ident()?;
            let alias = if self.peek() == Some(&Token::As) {
                self.advance();
                Some(self.expect_ident()?)
            } else if matches!(self.peek(), Some(Token::Ident(_))) && !self.is_keyword_ahead() {
                Some(self.expect_ident()?)
            } else {
                None
            };
            (Some(table_name), alias)
        } else {
            (None, None)
        };

        // Parse JOIN clauses
        let mut joins = Vec::new();
        if table_name.is_some() {
            loop {
                let join_type = match self.peek() {
                    Some(Token::Join) => {
                        self.advance();
                        Some(JoinType::Inner)
                    }
                    Some(Token::Inner) => {
                        self.advance();
                        self.expect(&Token::Join)?;
                        Some(JoinType::Inner)
                    }
                    Some(Token::Left) => {
                        self.advance();
                        // optional OUTER keyword (not a token, but could be an ident)
                        if matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case("OUTER"))
                        {
                            self.advance();
                        }
                        self.expect(&Token::Join)?;
                        Some(JoinType::Left)
                    }
                    Some(Token::Right) => {
                        self.advance();
                        // optional OUTER keyword (not a token, but could be an ident)
                        if matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case("OUTER"))
                        {
                            self.advance();
                        }
                        self.expect(&Token::Join)?;
                        Some(JoinType::Right)
                    }
                    Some(Token::Cross) => {
                        self.advance();
                        self.expect(&Token::Join)?;
                        Some(JoinType::Cross)
                    }
                    _ => None,
                };

                match join_type {
                    Some(jt) => {
                        let jt_table = self.expect_ident()?;
                        let jt_alias = if self.peek() == Some(&Token::As) {
                            self.advance();
                            Some(self.expect_ident()?)
                        } else if matches!(self.peek(), Some(Token::Ident(_)))
                            && !self.is_keyword_ahead()
                        {
                            Some(self.expect_ident()?)
                        } else {
                            None
                        };
                        let on_condition = if jt == JoinType::Cross {
                            None
                        } else {
                            self.expect(&Token::On)?;
                            Some(self.parse_expr()?)
                        };
                        joins.push(JoinClause {
                            join_type: jt,
                            table_name: jt_table,
                            alias: jt_alias,
                            on_condition,
                        });
                    }
                    None => break,
                }
            }
        }

        let where_clause = if self.peek() == Some(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        // GROUP BY
        let group_by = if self.peek() == Some(&Token::Group) {
            self.advance();
            self.expect(&Token::By)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expr()?);
                if self.peek() == Some(&Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            Some(exprs)
        } else {
            None
        };

        // HAVING
        let having = if self.peek() == Some(&Token::Having) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.peek() == Some(&Token::Order) {
            self.advance();
            self.expect(&Token::By)?;
            let mut items = Vec::new();
            loop {
                let expr = self.parse_expr()?;
                let descending = if self.peek() == Some(&Token::Desc) {
                    self.advance();
                    true
                } else if self.peek() == Some(&Token::Asc) {
                    self.advance();
                    false
                } else {
                    false
                };
                items.push(OrderByItem { expr, descending });
                if self.peek() == Some(&Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            Some(items)
        } else {
            None
        };

        let limit = if self.peek() == Some(&Token::Limit) {
            self.advance();
            match self.advance() {
                Some(Token::Integer(n)) => Some(n as u64),
                _ => return Err("Expected integer after LIMIT".into()),
            }
        } else {
            None
        };

        let offset = if self.peek() == Some(&Token::Offset) {
            self.advance();
            match self.advance() {
                Some(Token::Integer(n)) => Some(n as u64),
                _ => return Err("Expected integer after OFFSET".into()),
            }
        } else {
            None
        };

        if table_name.is_none() && columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
            return Err("SELECT * requires a FROM clause".into());
        }

        Ok(Select {
            distinct,
            columns,
            table_name,
            table_alias,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    /// Parse SELECT, potentially followed by UNION [ALL] chains.
    fn parse_select_or_union(&mut self) -> Result<Statement, String> {
        let mut first = self.parse_select()?;

        // Check for UNION
        if self.peek() != Some(&Token::Union) {
            return Ok(Statement::Select(Box::new(first)));
        }

        // Move ORDER BY / LIMIT / OFFSET from first SELECT to SetQuery level
        // (they belong to the whole UNION, not the individual SELECT)
        let mut set_order_by = first.order_by.take();
        let mut set_limit = first.limit.take();
        let mut set_offset = first.offset.take();

        let mut ops = Vec::new();

        while self.peek() == Some(&Token::Union) {
            self.advance(); // consume UNION
            let set_op = if self.peek() == Some(&Token::All) {
                self.advance();
                SetOp::UnionAll
            } else {
                SetOp::Union
            };

            let mut sel = self.parse_select()?;

            // If this SELECT has ORDER BY/LIMIT/OFFSET, treat them as final SetQuery-level clauses
            if sel.order_by.is_some() {
                set_order_by = sel.order_by.take();
            }
            if sel.limit.is_some() {
                set_limit = sel.limit.take();
            }
            if sel.offset.is_some() {
                set_offset = sel.offset.take();
            }

            ops.push((set_op, sel));
        }

        Ok(Statement::SetQuery(Box::new(SetQuery {
            left: first,
            ops,
            order_by: set_order_by,
            limit: set_limit,
            offset: set_offset,
        })))
    }

    /// Check if the next token is a SQL keyword (not a table alias).
    fn is_keyword_ahead(&self) -> bool {
        matches!(
            self.peek(),
            Some(
                Token::Where
                    | Token::Group
                    | Token::Having
                    | Token::Order
                    | Token::Limit
                    | Token::Offset
                    | Token::Join
                    | Token::Inner
                    | Token::Left
                    | Token::Right
                    | Token::Cross
                    | Token::On
                    | Token::Union
                    | Token::Semicolon
            )
        )
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, String> {
        if self.peek() == Some(&Token::Star) {
            self.advance();
            return Ok(vec![SelectColumn::Star]);
        }

        let mut columns = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let alias = if self.peek() == Some(&Token::As) {
                self.advance();
                Some(self.expect_ident()?)
            } else {
                None
            };
            columns.push(SelectColumn::Expr(expr, alias));

            if self.peek() == Some(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_update(&mut self) -> Result<Update, String> {
        self.advance(); // UPDATE
        let table_name = self.expect_ident()?;
        self.expect(&Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let val = self.parse_expr()?;
            assignments.push((col, val));

            if self.peek() == Some(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        let where_clause = if self.peek() == Some(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Update {
            table_name,
            assignments,
            where_clause,
        })
    }

    fn parse_delete(&mut self) -> Result<Delete, String> {
        self.advance(); // DELETE
        self.expect(&Token::From)?;
        let table_name = self.expect_ident()?;

        let where_clause = if self.peek() == Some(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Delete {
            table_name,
            where_clause,
        })
    }

    // Expression parsing with precedence:
    // parse_expr -> parse_or_expr -> parse_and_expr -> parse_not_expr
    //   -> parse_comparison -> parse_additive -> parse_multiplicative -> parse_unary -> parse_primary

    fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and_expr()?;
        while self.peek() == Some(&Token::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_not_expr()?;
        while self.peek() == Some(&Token::And) {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr, String> {
        if self.peek() == Some(&Token::Not) {
            // Check for NOT EXISTS
            if self.tokens.get(self.pos + 1) == Some(&Token::Exists) {
                self.advance(); // consume NOT
                self.advance(); // consume EXISTS
                self.expect(&Token::LParen)?;
                self.advance(); // consume SELECT
                let subquery = self.parse_select_body()?;
                self.expect(&Token::RParen)?;
                return Ok(Expr::Exists {
                    subquery: Box::new(subquery),
                    negated: true,
                });
            }
            self.advance();
            let operand = self.parse_comparison()?;
            Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                operand: Box::new(operand),
            })
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let left = self.parse_additive()?;

        // IS [NOT] NULL
        if self.peek() == Some(&Token::Is) {
            self.advance();
            let negated = if self.peek() == Some(&Token::Not) {
                self.advance();
                true
            } else {
                false
            };
            self.expect(&Token::Null)?;
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }

        // NOT LIKE / NOT IN / NOT BETWEEN
        if self.peek() == Some(&Token::Not) {
            let saved_pos = self.pos;
            self.advance();
            match self.peek() {
                Some(Token::Like) => {
                    self.advance();
                    let pattern = self.parse_additive()?;
                    return Ok(Expr::Like {
                        expr: Box::new(left),
                        pattern: Box::new(pattern),
                        negated: true,
                    });
                }
                Some(Token::In) => {
                    self.advance();
                    return self.parse_in_list_or_subquery(left, true);
                }
                Some(Token::Between) => {
                    self.advance();
                    return self.parse_between_rest(left, true);
                }
                _ => {
                    // Not a valid postfix NOT, rewind
                    self.pos = saved_pos;
                }
            }
        }

        // LIKE
        if self.peek() == Some(&Token::Like) {
            self.advance();
            let pattern = self.parse_additive()?;
            return Ok(Expr::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                negated: false,
            });
        }

        // IN
        if self.peek() == Some(&Token::In) {
            self.advance();
            return self.parse_in_list_or_subquery(left, false);
        }

        // BETWEEN
        if self.peek() == Some(&Token::Between) {
            self.advance();
            return self.parse_between_rest(left, false);
        }

        // REGEXP
        if self.peek() == Some(&Token::Regexp) {
            self.advance();
            let pattern = self.parse_additive()?;
            return Ok(Expr::FunctionCall {
                name: "REGEXP".to_string(),
                args: vec![left, pattern],
            });
        }

        let op = match self.peek() {
            Some(Token::Eq) => Some(BinaryOp::Eq),
            Some(Token::Ne) => Some(BinaryOp::Ne),
            Some(Token::Lt) => Some(BinaryOp::Lt),
            Some(Token::Gt) => Some(BinaryOp::Gt),
            Some(Token::Le) => Some(BinaryOp::Le),
            Some(Token::Ge) => Some(BinaryOp::Ge),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive()?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_in_list_or_subquery(&mut self, left: Expr, negated: bool) -> Result<Expr, String> {
        self.expect(&Token::LParen)?;
        // Check if this is a subquery: IN (SELECT ...)
        if self.peek() == Some(&Token::Select) {
            self.advance(); // consume SELECT
            let subquery = self.parse_select_body()?;
            self.expect(&Token::RParen)?;
            return Ok(Expr::InSubquery {
                expr: Box::new(left),
                subquery: Box::new(subquery),
                negated,
            });
        }
        // Otherwise parse as a regular value list
        let mut list = Vec::new();
        loop {
            list.push(self.parse_expr()?);
            if self.peek() == Some(&Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Expr::InList {
            expr: Box::new(left),
            list,
            negated,
        })
    }

    /// Parse a SELECT body (everything after the SELECT keyword).
    /// Used for subqueries where the caller has already consumed SELECT.
    fn parse_select_body(&mut self) -> Result<Select, String> {
        let distinct = if self.peek() == Some(&Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        let columns = self.parse_select_columns()?;

        let (table_name, table_alias) = if self.peek() == Some(&Token::From) {
            self.advance();
            let table_name = self.expect_ident()?;
            let alias = if self.peek() == Some(&Token::As) {
                self.advance();
                Some(self.expect_ident()?)
            } else if matches!(self.peek(), Some(Token::Ident(_))) && !self.is_keyword_ahead() {
                Some(self.expect_ident()?)
            } else {
                None
            };
            (Some(table_name), alias)
        } else {
            (None, None)
        };

        let mut joins = Vec::new();
        if table_name.is_some() {
            loop {
                let join_type = match self.peek() {
                    Some(Token::Join) => {
                        self.advance();
                        Some(JoinType::Inner)
                    }
                    Some(Token::Inner) => {
                        self.advance();
                        self.expect(&Token::Join)?;
                        Some(JoinType::Inner)
                    }
                    Some(Token::Left) => {
                        self.advance();
                        if matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case("OUTER"))
                        {
                            self.advance();
                        }
                        self.expect(&Token::Join)?;
                        Some(JoinType::Left)
                    }
                    Some(Token::Right) => {
                        self.advance();
                        if matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case("OUTER"))
                        {
                            self.advance();
                        }
                        self.expect(&Token::Join)?;
                        Some(JoinType::Right)
                    }
                    Some(Token::Cross) => {
                        self.advance();
                        self.expect(&Token::Join)?;
                        Some(JoinType::Cross)
                    }
                    _ => None,
                };

                match join_type {
                    Some(jt) => {
                        let jt_table = self.expect_ident()?;
                        let jt_alias = if self.peek() == Some(&Token::As) {
                            self.advance();
                            Some(self.expect_ident()?)
                        } else if matches!(self.peek(), Some(Token::Ident(_)))
                            && !self.is_keyword_ahead()
                        {
                            Some(self.expect_ident()?)
                        } else {
                            None
                        };
                        let on_condition = if jt == JoinType::Cross {
                            None
                        } else {
                            self.expect(&Token::On)?;
                            Some(self.parse_expr()?)
                        };
                        joins.push(JoinClause {
                            join_type: jt,
                            table_name: jt_table,
                            alias: jt_alias,
                            on_condition,
                        });
                    }
                    None => break,
                }
            }
        }

        let where_clause = if self.peek() == Some(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.peek() == Some(&Token::Group) {
            self.advance();
            self.expect(&Token::By)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expr()?);
                if self.peek() == Some(&Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            Some(exprs)
        } else {
            None
        };

        let having = if self.peek() == Some(&Token::Having) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.peek() == Some(&Token::Order) {
            self.advance();
            self.expect(&Token::By)?;
            let mut items = Vec::new();
            loop {
                let expr = self.parse_expr()?;
                let descending = if self.peek() == Some(&Token::Desc) {
                    self.advance();
                    true
                } else if self.peek() == Some(&Token::Asc) {
                    self.advance();
                    false
                } else {
                    false
                };
                items.push(OrderByItem { expr, descending });
                if self.peek() == Some(&Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            Some(items)
        } else {
            None
        };

        let limit = if self.peek() == Some(&Token::Limit) {
            self.advance();
            match self.advance() {
                Some(Token::Integer(n)) => Some(n as u64),
                _ => return Err("Expected integer after LIMIT".into()),
            }
        } else {
            None
        };

        let offset = if self.peek() == Some(&Token::Offset) {
            self.advance();
            match self.advance() {
                Some(Token::Integer(n)) => Some(n as u64),
                _ => return Err("Expected integer after OFFSET".into()),
            }
        } else {
            None
        };

        if table_name.is_none() && columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
            return Err("SELECT * requires a FROM clause".into());
        }

        Ok(Select {
            distinct,
            columns,
            table_name,
            table_alias,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_between_rest(&mut self, left: Expr, negated: bool) -> Result<Expr, String> {
        let low = self.parse_additive()?;
        self.expect(&Token::And)?;
        let high = self.parse_additive()?;
        Ok(Expr::Between {
            expr: Box::new(left),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        })
    }

    fn parse_additive(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Some(Token::Plus) => Some(BinaryOp::Add),
                Some(Token::Minus) => Some(BinaryOp::Sub),
                _ => None,
            };
            if let Some(op) = op {
                self.advance();
                let right = self.parse_multiplicative()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Some(Token::Star) => Some(BinaryOp::Mul),
                Some(Token::Slash) => Some(BinaryOp::Div),
                Some(Token::Percent) => Some(BinaryOp::Mod),
                _ => None,
            };
            if let Some(op) = op {
                self.advance();
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, String> {
        if self.peek() == Some(&Token::Minus) {
            self.advance();
            let operand = self.parse_primary()?;
            // Optimize: if it's an integer literal, negate it directly
            match operand {
                Expr::IntLiteral(n) => Ok(Expr::IntLiteral(-n)),
                Expr::FloatLiteral(n) => Ok(Expr::FloatLiteral(-n)),
                _ => Ok(Expr::UnaryOp {
                    op: UnaryOp::Neg,
                    operand: Box::new(operand),
                }),
            }
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.peek().cloned() {
            Some(Token::Integer(n)) => {
                self.advance();
                Ok(Expr::IntLiteral(n))
            }
            Some(Token::Float(n)) => {
                self.advance();
                Ok(Expr::FloatLiteral(n))
            }
            Some(Token::StringLit(s)) => {
                self.advance();
                Ok(Expr::StringLiteral(s))
            }
            Some(Token::Null) => {
                self.advance();
                Ok(Expr::Null)
            }
            Some(Token::Default) => {
                self.advance();
                Ok(Expr::DefaultValue)
            }
            Some(Token::Count) | Some(Token::Sum) | Some(Token::Avg) | Some(Token::Min)
            | Some(Token::Max) => self.parse_aggregate_func(),
            Some(Token::Match) => self.parse_match_against(),
            Some(Token::FtsSnippet) => self.parse_fts_snippet(),
            Some(Token::Case) => self.parse_case_when(),
            Some(Token::Cast) => self.parse_cast(),
            Some(Token::Exists) => {
                self.advance(); // consume EXISTS
                self.expect(&Token::LParen)?;
                self.advance(); // consume SELECT
                let subquery = self.parse_select_body()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Exists {
                    subquery: Box::new(subquery),
                    negated: false,
                })
            }
            Some(Token::LParen) => {
                self.advance();
                // Check for scalar subquery: (SELECT ...)
                if self.peek() == Some(&Token::Select) {
                    self.advance(); // consume SELECT
                    let subquery = self.parse_select_body()?;
                    self.expect(&Token::RParen)?;
                    return Ok(Expr::ScalarSubquery(Box::new(subquery)));
                }
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Some(Token::Ident(name)) => {
                self.advance();
                // Check for function call: ident followed by '('
                if self.peek() == Some(&Token::LParen) {
                    self.advance(); // consume '('
                    let mut args = Vec::new();
                    if self.peek() != Some(&Token::RParen) {
                        loop {
                            args.push(self.parse_expr()?);
                            if self.peek() == Some(&Token::Comma) {
                                self.advance();
                            } else {
                                break;
                            }
                        }
                    }
                    self.expect(&Token::RParen)?;
                    Ok(Expr::FunctionCall {
                        name: name.to_uppercase(),
                        args,
                    })
                } else if self.peek() == Some(&Token::Dot) {
                    self.advance(); // consume '.'
                                    // After dot: could be Ident or Star
                    if self.peek() == Some(&Token::Star) {
                        self.advance();
                        Ok(Expr::ColumnRef(format!("{}.*", name)))
                    } else {
                        let col = self.expect_ident()?;
                        Ok(Expr::ColumnRef(format!("{}.{}", name, col)))
                    }
                } else if name.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
                    Ok(Expr::FunctionCall {
                        name: "CURRENT_TIMESTAMP".to_string(),
                        args: Vec::new(),
                    })
                } else {
                    Ok(Expr::ColumnRef(name))
                }
            }
            // Handle keyword-named functions: IF, LEFT, RIGHT, REPLACE, etc.
            Some(Token::If) | Some(Token::Left) | Some(Token::Right) | Some(Token::Replace) => {
                let name = match self.peek() {
                    Some(Token::If) => "IF",
                    Some(Token::Left) => "LEFT",
                    Some(Token::Right) => "RIGHT",
                    Some(Token::Replace) => "REPLACE",
                    _ => unreachable!(),
                };
                // Only treat as function if followed by '('
                if self.tokens.get(self.pos + 1) == Some(&Token::LParen) {
                    let name = name.to_string();
                    self.advance(); // consume keyword
                    self.advance(); // consume '('
                    let mut args = Vec::new();
                    if self.peek() != Some(&Token::RParen) {
                        loop {
                            args.push(self.parse_expr()?);
                            if self.peek() == Some(&Token::Comma) {
                                self.advance();
                            } else {
                                break;
                            }
                        }
                    }
                    self.expect(&Token::RParen)?;
                    Ok(Expr::FunctionCall { name, args })
                } else {
                    Err(format!("Unexpected token in expression: {:?}", self.peek()))
                }
            }
            Some(t) => Err(format!("Unexpected token in expression: {:?}", t)),
            None => Err("Unexpected end of input in expression".into()),
        }
    }

    fn parse_case_when(&mut self) -> Result<Expr, String> {
        self.advance(); // consume CASE

        // Check for simple CASE (CASE expr WHEN val THEN ...) vs searched CASE (CASE WHEN cond THEN ...)
        let operand = if self.peek() != Some(&Token::When) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut when_clauses = Vec::new();
        while self.peek() == Some(&Token::When) {
            self.advance(); // WHEN
            let condition = self.parse_expr()?;
            self.expect(&Token::Then)?;
            let result = self.parse_expr()?;
            when_clauses.push((condition, result));
        }

        let else_clause = if self.peek() == Some(&Token::Else) {
            self.advance();
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        self.expect(&Token::End)?;

        Ok(Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        })
    }

    fn parse_cast(&mut self) -> Result<Expr, String> {
        self.advance(); // consume CAST
        self.expect(&Token::LParen)?;
        let expr = self.parse_expr()?;
        // expect AS keyword
        self.expect(&Token::As)?;
        let target_type = self.parse_data_type()?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            target_type,
        })
    }

    fn parse_aggregate_func(&mut self) -> Result<Expr, String> {
        let name = match self.advance() {
            Some(Token::Count) => "COUNT",
            Some(Token::Sum) => "SUM",
            Some(Token::Avg) => "AVG",
            Some(Token::Min) => "MIN",
            Some(Token::Max) => "MAX",
            _ => unreachable!(),
        };
        self.expect(&Token::LParen)?;

        if name == "COUNT" && self.peek() == Some(&Token::Star) {
            // COUNT(*)
            self.advance();
            self.expect(&Token::RParen)?;
            return Ok(Expr::AggregateFunc {
                name: name.to_string(),
                arg: None,
                distinct: false,
            });
        }

        let distinct = if self.peek() == Some(&Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        let arg = self.parse_expr()?;
        self.expect(&Token::RParen)?;

        Ok(Expr::AggregateFunc {
            name: name.to_string(),
            arg: Some(Box::new(arg)),
            distinct,
        })
    }

    fn parse_match_against(&mut self) -> Result<Expr, String> {
        self.advance(); // MATCH
        self.expect(&Token::LParen)?;
        let column = self.expect_ident()?;
        self.expect(&Token::RParen)?;
        self.expect(&Token::Against)?;
        self.expect(&Token::LParen)?;

        let query = match self.advance() {
            Some(Token::StringLit(s)) => s,
            _ => return Err("Expected string literal in AGAINST".into()),
        };

        // Parse mode
        let mode = if self.peek() == Some(&Token::In) {
            self.advance();
            match self.peek() {
                Some(Token::Natural) => {
                    self.advance();
                    self.expect(&Token::Language)?;
                    self.expect(&Token::Mode)?;
                    MatchMode::NaturalLanguage
                }
                Some(Token::Boolean) => {
                    self.advance();
                    self.expect(&Token::Mode)?;
                    MatchMode::Boolean
                }
                _ => return Err("Expected NATURAL LANGUAGE MODE or BOOLEAN MODE".into()),
            }
        } else {
            MatchMode::NaturalLanguage // default
        };

        self.expect(&Token::RParen)?;

        Ok(Expr::MatchAgainst {
            column,
            query,
            mode,
        })
    }

    fn parse_fts_snippet(&mut self) -> Result<Expr, String> {
        self.advance(); // fts_snippet
        self.expect(&Token::LParen)?;

        let column = self.expect_ident()?;
        self.expect(&Token::Comma)?;

        let query = match self.advance() {
            Some(Token::StringLit(s)) => s,
            _ => return Err("Expected string literal for snippet query".into()),
        };
        self.expect(&Token::Comma)?;

        let pre_tag = match self.advance() {
            Some(Token::StringLit(s)) => s,
            _ => return Err("Expected string literal for pre_tag".into()),
        };
        self.expect(&Token::Comma)?;

        let post_tag = match self.advance() {
            Some(Token::StringLit(s)) => s,
            _ => return Err("Expected string literal for post_tag".into()),
        };
        self.expect(&Token::Comma)?;

        let context_chars = match self.advance() {
            Some(Token::Integer(n)) => n as usize,
            _ => return Err("Expected integer for context_chars".into()),
        };
        self.expect(&Token::RParen)?;

        Ok(Expr::FtsSnippet {
            column,
            query,
            pre_tag,
            post_tag,
            context_chars,
        })
    }
}

/// Parse a SQL string into a statement.
pub fn parse_sql(sql: &str) -> Result<Statement, String> {
    let tokens = crate::sql::lexer::tokenize(sql)?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_table() {
        let stmt =
            parse_sql("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, data VARBINARY)")
                .unwrap();
        if let Statement::CreateTable(ct) = stmt {
            assert_eq!(ct.table_name, "users");
            assert_eq!(ct.columns.len(), 3);
            assert!(ct.columns[0].is_primary_key);
            assert_eq!(ct.columns[1].data_type, DataType::Varchar(None));
            assert!(!ct.if_not_exists);
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn test_parse_create_table_if_not_exists() {
        let stmt =
            parse_sql("CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY, name VARCHAR)")
                .unwrap();
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.if_not_exists);
            assert_eq!(ct.table_name, "users");
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse_sql("INSERT INTO t (id, name) VALUES (1, 'hello')").unwrap();
        if let Statement::Insert(ins) = stmt {
            assert_eq!(ins.table_name, "t");
            assert_eq!(
                ins.columns,
                Some(vec!["id".to_string(), "name".to_string()])
            );
            assert_eq!(ins.values.len(), 1);
            assert_eq!(ins.values[0].len(), 2);
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parse_select() {
        let stmt = parse_sql("SELECT * FROM t WHERE id = 42 ORDER BY id ASC LIMIT 10").unwrap();
        if let Statement::Select(sel) = stmt {
            assert_eq!(sel.table_name.as_deref(), Some("t"));
            assert!(sel.where_clause.is_some());
            assert!(sel.order_by.is_some());
            assert_eq!(sel.limit, Some(10));
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_select_with_offset() {
        let stmt = parse_sql("SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 5").unwrap();
        if let Statement::Select(sel) = stmt {
            assert_eq!(sel.limit, Some(10));
            assert_eq!(sel.offset, Some(5));
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse_sql("UPDATE t SET name = 'new' WHERE id = 1").unwrap();
        if let Statement::Update(upd) = stmt {
            assert_eq!(upd.table_name, "t");
            assert_eq!(upd.assignments.len(), 1);
        } else {
            panic!("Expected Update");
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse_sql("DELETE FROM t WHERE id = 1").unwrap();
        if let Statement::Delete(del) = stmt {
            assert_eq!(del.table_name, "t");
            assert!(del.where_clause.is_some());
        } else {
            panic!("Expected Delete");
        }
    }

    #[test]
    fn test_parse_create_unique_index() {
        let stmt = parse_sql("CREATE UNIQUE INDEX idx_email ON users(email)").unwrap();
        if let Statement::CreateIndex(ci) = stmt {
            assert_eq!(ci.index_name, "idx_email");
            assert!(ci.is_unique);
        } else {
            panic!("Expected CreateIndex");
        }
    }

    #[test]
    fn test_parse_create_fulltext_index() {
        let stmt = parse_sql(
            "CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc')",
        ).unwrap();
        if let Statement::CreateFulltextIndex(fi) = stmt {
            assert_eq!(fi.index_name, "ft_body");
            assert_eq!(fi.column_name, "body");
            assert_eq!(fi.ngram_n, 2);
        } else {
            panic!("Expected CreateFulltextIndex");
        }
    }

    #[test]
    fn test_parse_match_against() {
        let stmt = parse_sql(
            "SELECT * FROM t WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0",
        )
        .unwrap();
        if let Statement::Select(sel) = stmt {
            assert!(sel.where_clause.is_some());
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = parse_sql("DROP TABLE t").unwrap();
        if let Statement::DropTable(dt) = stmt {
            assert_eq!(dt.table_name, "t");
            assert!(!dt.if_exists);
        } else {
            panic!("Expected DropTable");
        }
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let stmt = parse_sql("DROP TABLE IF EXISTS t").unwrap();
        if let Statement::DropTable(dt) = stmt {
            assert_eq!(dt.table_name, "t");
            assert!(dt.if_exists);
        } else {
            panic!("Expected DropTable");
        }
    }

    #[test]
    fn test_parse_drop_index() {
        let stmt = parse_sql("DROP INDEX idx_name").unwrap();
        if let Statement::DropIndex(di) = stmt {
            assert_eq!(di.index_name, "idx_name");
            assert!(!di.if_exists);
        } else {
            panic!("Expected DropIndex");
        }
    }

    #[test]
    fn test_parse_show_create_table() {
        let stmt = parse_sql("SHOW CREATE TABLE users").unwrap();
        if let Statement::ShowCreateTable(name) = stmt {
            assert_eq!(name, "users");
        } else {
            panic!("Expected ShowCreateTable");
        }
    }

    #[test]
    fn test_parse_describe() {
        let stmt = parse_sql("DESCRIBE users").unwrap();
        if let Statement::Describe(name) = stmt {
            assert_eq!(name, "users");
        } else {
            panic!("Expected Describe");
        }
    }

    #[test]
    fn test_parse_like() {
        let stmt = parse_sql("SELECT * FROM t WHERE name LIKE '%foo%'").unwrap();
        if let Statement::Select(sel) = stmt {
            assert!(matches!(
                sel.where_clause,
                Some(Expr::Like { negated: false, .. })
            ));
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_not_like() {
        let stmt = parse_sql("SELECT * FROM t WHERE name NOT LIKE '%foo%'").unwrap();
        if let Statement::Select(sel) = stmt {
            assert!(matches!(
                sel.where_clause,
                Some(Expr::Like { negated: true, .. })
            ));
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_in() {
        let stmt = parse_sql("SELECT * FROM t WHERE id IN (1, 2, 3)").unwrap();
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::InList { list, negated, .. }) = sel.where_clause {
                assert!(!negated);
                assert_eq!(list.len(), 3);
            } else {
                panic!("Expected InList");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_between() {
        let stmt = parse_sql("SELECT * FROM t WHERE id BETWEEN 1 AND 10").unwrap();
        if let Statement::Select(sel) = stmt {
            assert!(matches!(
                sel.where_clause,
                Some(Expr::Between { negated: false, .. })
            ));
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_is_null() {
        let stmt = parse_sql("SELECT * FROM t WHERE name IS NULL").unwrap();
        if let Statement::Select(sel) = stmt {
            assert!(matches!(
                sel.where_clause,
                Some(Expr::IsNull { negated: false, .. })
            ));
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_is_not_null() {
        let stmt = parse_sql("SELECT * FROM t WHERE name IS NOT NULL").unwrap();
        if let Statement::Select(sel) = stmt {
            assert!(matches!(
                sel.where_clause,
                Some(Expr::IsNull { negated: true, .. })
            ));
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_arithmetic() {
        let stmt = parse_sql("SELECT a + b * c FROM t").unwrap();
        if let Statement::Select(sel) = stmt {
            // a + (b * c) due to precedence
            if let SelectColumn::Expr(
                Expr::BinaryOp {
                    op: BinaryOp::Add, ..
                },
                _,
            ) = &sel.columns[0]
            {
                // good
            } else {
                panic!("Expected addition at top level");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_unary_minus() {
        let stmt = parse_sql("SELECT * FROM t WHERE id = -1").unwrap();
        if let Statement::Select(sel) = stmt {
            if let Some(Expr::BinaryOp { right, .. }) = sel.where_clause {
                assert!(matches!(*right, Expr::IntLiteral(-1)));
            } else {
                panic!("Expected BinaryOp");
            }
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_default_value() {
        let stmt =
            parse_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, status INT DEFAULT 0)").unwrap();
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.columns[1].default_value.is_some());
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn test_parse_auto_increment() {
        let stmt = parse_sql("CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR)")
            .unwrap();
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.columns[0].auto_increment);
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn test_parse_check_constraint() {
        let stmt =
            parse_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, age INT CHECK (age > 0))").unwrap();
        if let Statement::CreateTable(ct) = stmt {
            assert!(ct.columns[1].check_expr.is_some());
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn test_parse_boolean_type() {
        let stmt =
            parse_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, active BOOLEAN DEFAULT 0)").unwrap();
        if let Statement::CreateTable(ct) = stmt {
            assert_eq!(ct.columns[1].data_type, DataType::TinyInt);
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn test_parse_not_operator() {
        let stmt = parse_sql("SELECT * FROM t WHERE NOT id = 1").unwrap();
        if let Statement::Select(sel) = stmt {
            assert!(matches!(
                sel.where_clause,
                Some(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    ..
                })
            ));
        } else {
            panic!("Expected Select");
        }
    }

    #[test]
    fn test_parse_analyze_table() {
        let stmt = parse_sql("ANALYZE TABLE users").unwrap();
        if let Statement::AnalyzeTable(name) = stmt {
            assert_eq!(name, "users");
        } else {
            panic!("Expected AnalyzeTable");
        }
    }
}
