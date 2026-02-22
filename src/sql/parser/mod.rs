/// SQL parser: converts token stream into AST.
/// Hand-written recursive descent parser.
use crate::sql::ast::*;
use crate::sql::lexer::Token;
use crate::types::DataType;

mod expr_and_select;

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
        let mut stop_filter = false;
        let mut stop_df_ratio_ppm = 200_000u32;

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
                    "stop_filter" => match self.advance() {
                        Some(Token::Integer(n)) => stop_filter = n != 0,
                        Some(Token::On) => stop_filter = true,
                        Some(Token::StringLit(s)) | Some(Token::Ident(s)) => {
                            let v = s.to_ascii_lowercase();
                            stop_filter = match v.as_str() {
                                "on" | "true" | "1" => true,
                                "off" | "false" | "0" => false,
                                _ => return Err(format!("Invalid stop_filter value: {}", s)),
                            };
                        }
                        Some(tok) => {
                            return Err(format!("Invalid stop_filter token: {:?}", tok));
                        }
                        None => return Err("Expected stop_filter value".into()),
                    },
                    "stop_df_ratio_ppm" => match self.advance() {
                        Some(Token::Integer(n)) if n >= 0 => {
                            let n_u64 = n as u64;
                            if n_u64 > u32::MAX as u64 {
                                return Err(format!(
                                    "stop_df_ratio_ppm is too large: {} (max {})",
                                    n,
                                    u32::MAX
                                ));
                            }
                            stop_df_ratio_ppm = n_u64 as u32;
                        }
                        Some(Token::Integer(_)) => {
                            return Err("stop_df_ratio_ppm must be >= 0".into());
                        }
                        Some(tok) => {
                            return Err(format!("Invalid stop_df_ratio_ppm token: {:?}", tok));
                        }
                        None => return Err("Expected stop_df_ratio_ppm value".into()),
                    },
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
            stop_filter,
            stop_df_ratio_ppm,
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
}

/// Parse a SQL string into a statement.
pub fn parse_sql(sql: &str) -> Result<Statement, String> {
    let tokens = crate::sql::lexer::tokenize(sql)?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

#[cfg(test)]
mod tests;
