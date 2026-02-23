use super::*;

impl Parser {
    pub(super) fn parse_insert(&mut self, is_replace: bool) -> Result<Insert, String> {
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
}
