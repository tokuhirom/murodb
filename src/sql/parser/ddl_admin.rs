use super::*;
use crate::types::DataType;

impl Parser {
    pub(super) fn parse_analyze(&mut self) -> Result<Statement, String> {
        self.advance(); // ANALYZE
        self.expect(&Token::Table)?;
        let table_name = self.expect_ident()?;
        Ok(Statement::AnalyzeTable(table_name))
    }

    pub(super) fn parse_create(&mut self) -> Result<Statement, String> {
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

    pub(super) fn parse_if_not_exists(&mut self) -> Result<bool, String> {
        if self.peek() == Some(&Token::If) {
            self.advance(); // IF
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(super) fn parse_if_exists(&mut self) -> Result<bool, String> {
        if self.peek() == Some(&Token::If) {
            self.advance(); // IF
            self.expect(&Token::Exists)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(super) fn parse_drop(&mut self) -> Result<Statement, String> {
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

    pub(super) fn parse_alter(&mut self) -> Result<Statement, String> {
        self.advance(); // ALTER
        if self.peek() != Some(&Token::Table) {
            return Err("Only ALTER TABLE is supported".into());
        }
        self.expect(&Token::Table)?;
        let table_name = self.expect_ident()?;

        let operation = match self.peek() {
            Some(Token::Add) => {
                self.advance(); // ADD
                if self.peek() == Some(&Token::Foreign) {
                    let fk = self.parse_foreign_key_spec()?;
                    AlterTableOp::AddForeignKey(fk)
                } else {
                    // Optional COLUMN keyword
                    if self.peek() == Some(&Token::Column) {
                        self.advance();
                    }
                    let col_spec = self.parse_column_spec()?;
                    AlterTableOp::AddColumn(col_spec)
                }
            }
            Some(Token::Drop) => {
                self.advance(); // DROP
                match self.peek() {
                    Some(Token::Column) => {
                        self.advance();
                        let col_name = self.expect_ident()?;
                        AlterTableOp::DropColumn(col_name)
                    }
                    Some(Token::Foreign) => {
                        self.advance(); // FOREIGN
                        self.expect(&Token::Key)?;
                        self.expect(&Token::LParen)?;
                        let cols = self.parse_ident_list()?;
                        self.expect(&Token::RParen)?;
                        AlterTableOp::DropForeignKey(cols)
                    }
                    _ => return Err("Expected COLUMN or FOREIGN KEY after DROP".into()),
                }
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

    pub(super) fn parse_rename(&mut self) -> Result<Statement, String> {
        self.advance(); // RENAME
        self.expect(&Token::Table)?;
        let old_name = self.expect_ident()?;
        self.expect(&Token::To)?;
        let new_name = self.expect_ident()?;
        Ok(Statement::RenameTable(RenameTable { old_name, new_name }))
    }

    pub(super) fn parse_create_table(&mut self) -> Result<CreateTable, String> {
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
                Some(Token::Foreign) => {
                    let fk = self.parse_foreign_key_spec()?;
                    constraints.push(TableConstraint::ForeignKey {
                        columns: fk.columns,
                        ref_table: fk.ref_table,
                        ref_columns: fk.ref_columns,
                        on_delete: fk.on_delete,
                        on_update: fk.on_update,
                    });

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

    pub(super) fn parse_ident_list(&mut self) -> Result<Vec<String>, String> {
        let mut names = Vec::new();
        names.push(self.expect_ident()?);
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            names.push(self.expect_ident()?);
        }
        Ok(names)
    }

    pub(super) fn parse_foreign_key_spec(&mut self) -> Result<ForeignKeySpec, String> {
        self.expect(&Token::Foreign)?;
        self.expect(&Token::Key)?;
        self.expect(&Token::LParen)?;
        let columns = self.parse_ident_list()?;
        self.expect(&Token::RParen)?;
        self.expect(&Token::References)?;
        let ref_table = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let ref_columns = self.parse_ident_list()?;
        self.expect(&Token::RParen)?;

        let mut on_delete = ForeignKeyAction::Restrict;
        let mut on_update = ForeignKeyAction::Restrict;
        loop {
            if self.peek() != Some(&Token::On) {
                break;
            }
            self.advance(); // ON
            match self.peek() {
                Some(Token::Delete) => {
                    self.advance();
                    on_delete = self.parse_foreign_key_action()?;
                }
                Some(Token::Update) => {
                    self.advance();
                    on_update = self.parse_foreign_key_action()?;
                }
                _ => return Err("Expected DELETE or UPDATE after ON".into()),
            }
        }

        Ok(ForeignKeySpec {
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
        })
    }

    pub(super) fn parse_foreign_key_action(&mut self) -> Result<ForeignKeyAction, String> {
        match self.peek() {
            Some(Token::Cascade) => {
                self.advance();
                Ok(ForeignKeyAction::Cascade)
            }
            Some(Token::Set) => {
                self.advance();
                self.expect(&Token::Null)?;
                Ok(ForeignKeyAction::SetNull)
            }
            Some(Token::Restrict) => {
                self.advance();
                Ok(ForeignKeyAction::Restrict)
            }
            _ => Err("Expected CASCADE, SET NULL, or RESTRICT".into()),
        }
    }

    pub(super) fn parse_column_spec(&mut self) -> Result<ColumnSpec, String> {
        let name = self.expect_ident()?;
        let data_type = self.parse_data_type()?;

        let mut is_primary_key = false;
        let mut is_unique = false;
        let mut is_nullable = true;
        let mut default_value = None;
        let mut auto_increment = false;
        let mut check_expr = None;
        let mut constraint_expr_order = Vec::new();

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
                    constraint_expr_order.retain(|k| *k != ColumnConstraintExprKind::Default);
                    constraint_expr_order.push(ColumnConstraintExprKind::Default);
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
                    constraint_expr_order.retain(|k| *k != ColumnConstraintExprKind::Check);
                    constraint_expr_order.push(ColumnConstraintExprKind::Check);
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
            constraint_expr_order,
        })
    }

    pub(super) fn parse_data_type(&mut self) -> Result<DataType, String> {
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
                Some(Token::JsonbType) => Ok(DataType::Jsonb),
                Some(Token::UuidType) => Ok(DataType::Uuid),
                Some(Token::DecimalType) => {
                    // Parse optional (precision, scale) with defaults (10, 0)
                    if self.peek() == Some(&Token::LParen) {
                        self.advance(); // (
                        let precision = match self.advance() {
                            Some(Token::Integer(n)) if (1..=28).contains(&n) => n as u32,
                            Some(Token::Integer(n)) => {
                                return Err(format!(
                                    "DECIMAL precision must be between 1 and 28, got {}",
                                    n
                                ))
                            }
                            _ => return Err("Expected integer precision for DECIMAL".into()),
                        };
                        let scale = if self.peek() == Some(&Token::Comma) {
                            self.advance(); // ,
                            match self.advance() {
                                Some(Token::Integer(n)) if (0..=precision as i64).contains(&n) => {
                                    n as u32
                                }
                                Some(Token::Integer(n)) => {
                                    return Err(format!(
                                        "DECIMAL scale must be between 0 and {}, got {}",
                                        precision, n
                                    ))
                                }
                                _ => return Err("Expected integer scale for DECIMAL".into()),
                            }
                        } else {
                            0
                        };
                        self.expect(&Token::RParen)?;
                        Ok(DataType::Decimal(precision, scale))
                    } else {
                        Ok(DataType::Decimal(10, 0))
                    }
                }
                Some(t) => Err(format!("Expected data type, got {:?}", t)),
                None => Err("Expected data type".into()),
            },
        }
    }

    pub(super) fn parse_optional_size(&mut self) -> Result<Option<u32>, String> {
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

    pub(super) fn parse_create_index(&mut self, is_unique: bool) -> Result<CreateIndex, String> {
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

    pub(super) fn parse_create_fulltext_index(&mut self) -> Result<CreateFulltextIndex, String> {
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

    pub(super) fn parse_show(&mut self) -> Result<Statement, String> {
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
}
