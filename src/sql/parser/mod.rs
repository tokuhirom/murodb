/// SQL parser: converts token stream into AST.
/// Hand-written recursive descent parser.
use crate::sql::ast::*;
use crate::sql::lexer::Token;

mod ddl_admin;
mod expr_and_select;
mod insert_stmt;
mod query_common;
mod select_stmt;

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
            Some(Token::Set) => self.parse_set_runtime_option()?,
            Some(t) => return Err(format!("Unexpected token: {:?}", t)),
            None => return Err("Empty input".into()),
        };

        // Skip optional trailing semicolon
        if self.peek() == Some(&Token::Semicolon) {
            self.advance();
        }

        // Reject trailing unparsed tokens
        if self.peek().is_some() {
            return Err(format!(
                "Unexpected trailing token: {:?}",
                self.peek().unwrap()
            ));
        }

        Ok(stmt)
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
