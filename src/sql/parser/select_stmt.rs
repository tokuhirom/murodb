use super::*;

impl Parser {
    pub(super) fn parse_select(&mut self) -> Result<Select, String> {
        self.advance(); // SELECT

        // Parse optional DISTINCT
        let distinct = if self.peek() == Some(&Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        let columns = self.parse_select_columns()?;

        let (table_name, table_alias, index_hints) = if self.peek() == Some(&Token::From) {
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
            let index_hints = self.parse_index_hints()?;
            (Some(table_name), alias, index_hints)
        } else {
            (None, None, Vec::new())
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
            index_hints,
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
