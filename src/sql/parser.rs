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
            Some(t) => Err(format!("Expected identifier, got {:?}", t)),
            None => Err("Expected identifier, got end of input".into()),
        }
    }

    pub fn parse(&mut self) -> Result<Statement, String> {
        let stmt = match self.peek() {
            Some(Token::Create) => self.parse_create()?,
            Some(Token::Select) => Statement::Select(self.parse_select()?),
            Some(Token::Insert) => Statement::Insert(self.parse_insert()?),
            Some(Token::Update) => Statement::Update(self.parse_update()?),
            Some(Token::Delete) => Statement::Delete(self.parse_delete()?),
            Some(Token::Show) => self.parse_show()?,
            Some(t) => return Err(format!("Unexpected token: {:?}", t)),
            None => return Err("Empty input".into()),
        };

        // Skip optional trailing semicolon
        if self.peek() == Some(&Token::Semicolon) {
            self.advance();
        }

        Ok(stmt)
    }

    fn parse_create(&mut self) -> Result<Statement, String> {
        self.advance(); // consume CREATE

        match self.peek() {
            Some(Token::Table) => {
                self.advance();
                Ok(Statement::CreateTable(self.parse_create_table()?))
            }
            Some(Token::Unique) => {
                self.advance();
                self.expect(&Token::Index)?;
                Ok(Statement::CreateIndex(self.parse_create_index(true)?))
            }
            Some(Token::Index) => {
                self.advance();
                Ok(Statement::CreateIndex(self.parse_create_index(false)?))
            }
            Some(Token::Fulltext) => {
                self.advance();
                self.expect(&Token::Index)?;
                Ok(Statement::CreateFulltextIndex(self.parse_create_fulltext_index()?))
            }
            _ => Err("Expected TABLE, INDEX, UNIQUE INDEX, or FULLTEXT INDEX after CREATE".into()),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTable, String> {
        let table_name = self.expect_ident()?;
        self.expect(&Token::LParen)?;

        let mut columns = Vec::new();
        loop {
            let col = self.parse_column_spec()?;
            columns.push(col);

            match self.peek() {
                Some(Token::Comma) => { self.advance(); }
                Some(Token::RParen) => { self.advance(); break; }
                _ => return Err("Expected ',' or ')' in column list".into()),
            }
        }

        Ok(CreateTable { table_name, columns })
    }

    fn parse_column_spec(&mut self) -> Result<ColumnSpec, String> {
        let name = self.expect_ident()?;
        let data_type = self.parse_data_type()?;

        let mut is_primary_key = false;
        let mut is_unique = false;
        let mut is_nullable = true;

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
                _ => break,
            }
        }

        Ok(ColumnSpec {
            name,
            data_type,
            is_primary_key,
            is_unique,
            is_nullable,
        })
    }

    fn parse_data_type(&mut self) -> Result<DataType, String> {
        match self.advance() {
            Some(Token::Int64Type) => Ok(DataType::Int64),
            Some(Token::VarcharType) => Ok(DataType::Varchar),
            Some(Token::VarbinaryType) => Ok(DataType::Varbinary),
            Some(t) => Err(format!("Expected data type, got {:?}", t)),
            None => Err("Expected data type".into()),
        }
    }

    fn parse_create_index(&mut self, is_unique: bool) -> Result<CreateIndex, String> {
        let index_name = self.expect_ident()?;
        self.expect(&Token::On)?;
        let table_name = self.expect_ident()?;
        self.expect(&Token::LParen)?;
        let column_name = self.expect_ident()?;
        self.expect(&Token::RParen)?;

        Ok(CreateIndex {
            index_name,
            table_name,
            column_name,
            is_unique,
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
            _ => Err("Expected TABLES after SHOW".into()),
        }
    }

    fn parse_insert(&mut self) -> Result<Insert, String> {
        self.advance(); // INSERT
        self.expect(&Token::Into)?;
        let table_name = self.expect_ident()?;

        // Optional column list
        let columns = if self.peek() == Some(&Token::LParen) {
            self.advance();
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                match self.peek() {
                    Some(Token::Comma) => { self.advance(); }
                    Some(Token::RParen) => { self.advance(); break; }
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
                    Some(Token::Comma) => { self.advance(); }
                    Some(Token::RParen) => { self.advance(); break; }
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

        Ok(Insert {
            table_name,
            columns,
            values,
        })
    }

    fn parse_select(&mut self) -> Result<Select, String> {
        self.advance(); // SELECT

        let columns = self.parse_select_columns()?;

        self.expect(&Token::From)?;
        let table_name = self.expect_ident()?;

        let where_clause = if self.peek() == Some(&Token::Where) {
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

        Ok(Select {
            columns,
            table_name,
            where_clause,
            order_by,
            limit,
        })
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

    // Expression parsing with precedence

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
        let mut left = self.parse_comparison()?;
        while self.peek() == Some(&Token::And) {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let left = self.parse_primary()?;

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
            let right = self.parse_primary()?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.peek().cloned() {
            Some(Token::Integer(n)) => {
                self.advance();
                Ok(Expr::IntLiteral(n))
            }
            Some(Token::StringLit(s)) => {
                self.advance();
                Ok(Expr::StringLiteral(s))
            }
            Some(Token::Null) => {
                self.advance();
                Ok(Expr::Null)
            }
            Some(Token::Match) => self.parse_match_against(),
            Some(Token::FtsSnippet) => self.parse_fts_snippet(),
            Some(Token::LParen) => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Some(Token::Ident(name)) => {
                self.advance();
                Ok(Expr::ColumnRef(name))
            }
            Some(t) => Err(format!("Unexpected token in expression: {:?}", t)),
            None => Err("Unexpected end of input in expression".into()),
        }
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
        let stmt = parse_sql("CREATE TABLE users (id INT64 PRIMARY KEY, name VARCHAR, data VARBINARY)").unwrap();
        if let Statement::CreateTable(ct) = stmt {
            assert_eq!(ct.table_name, "users");
            assert_eq!(ct.columns.len(), 3);
            assert!(ct.columns[0].is_primary_key);
            assert_eq!(ct.columns[1].data_type, DataType::Varchar);
        } else {
            panic!("Expected CreateTable");
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse_sql("INSERT INTO t (id, name) VALUES (1, 'hello')").unwrap();
        if let Statement::Insert(ins) = stmt {
            assert_eq!(ins.table_name, "t");
            assert_eq!(ins.columns, Some(vec!["id".to_string(), "name".to_string()]));
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
            assert_eq!(sel.table_name, "t");
            assert!(sel.where_clause.is_some());
            assert!(sel.order_by.is_some());
            assert_eq!(sel.limit, Some(10));
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
        ).unwrap();
        if let Statement::Select(sel) = stmt {
            assert!(sel.where_clause.is_some());
        } else {
            panic!("Expected Select");
        }
    }
}
