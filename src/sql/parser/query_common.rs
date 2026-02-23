use super::*;

impl Parser {
    pub(super) fn parse_select_or_union(&mut self) -> Result<Statement, String> {
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
    pub(super) fn is_keyword_ahead(&self) -> bool {
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
                    | Token::Force
                    | Token::Use
                    | Token::Ignore
            )
        )
    }

    pub(super) fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, String> {
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

    /// Parse optional index hints: FORCE INDEX (idx1, idx2) | USE INDEX (idx1) | IGNORE INDEX (idx1)
    pub(super) fn parse_index_hints(&mut self) -> Result<Vec<IndexHint>, String> {
        let mut hints = Vec::new();
        loop {
            let hint_type = match self.peek() {
                Some(Token::Force) => {
                    if self.tokens.get(self.pos + 1) == Some(&Token::Index) {
                        self.advance(); // FORCE
                        self.advance(); // INDEX
                        Some(IndexHintType::Force)
                    } else {
                        break;
                    }
                }
                Some(Token::Use) => {
                    if self.tokens.get(self.pos + 1) == Some(&Token::Index) {
                        self.advance(); // USE
                        self.advance(); // INDEX
                        Some(IndexHintType::Use)
                    } else {
                        break;
                    }
                }
                Some(Token::Ignore) => {
                    if self.tokens.get(self.pos + 1) == Some(&Token::Index) {
                        self.advance(); // IGNORE
                        self.advance(); // INDEX
                        Some(IndexHintType::Ignore)
                    } else {
                        break;
                    }
                }
                _ => break,
            };
            if let Some(ht) = hint_type {
                self.expect(&Token::LParen)?;
                let mut index_names = Vec::new();
                loop {
                    index_names.push(self.expect_ident()?);
                    if self.peek() == Some(&Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.expect(&Token::RParen)?;
                hints.push(IndexHint {
                    hint_type: ht,
                    index_names,
                });
            }
        }
        Ok(hints)
    }

    pub(super) fn parse_update(&mut self) -> Result<Update, String> {
        self.advance(); // UPDATE
        let table_name = self.expect_ident()?;
        let index_hints = self.parse_index_hints()?;
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
            index_hints,
            assignments,
            where_clause,
        })
    }

    pub(super) fn parse_delete(&mut self) -> Result<Delete, String> {
        self.advance(); // DELETE
        self.expect(&Token::From)?;
        let table_name = self.expect_ident()?;
        let index_hints = self.parse_index_hints()?;

        let where_clause = if self.peek() == Some(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Delete {
            table_name,
            index_hints,
            where_clause,
        })
    }

    // Expression parsing with precedence:
    // parse_expr -> parse_or_expr -> parse_and_expr -> parse_not_expr
    //   -> parse_comparison -> parse_additive -> parse_multiplicative -> parse_unary -> parse_primary

    pub(super) fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_or_expr()
    }

    pub(super) fn parse_or_expr(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_and_expr(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_not_expr(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_comparison(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_in_list_or_subquery(
        &mut self,
        left: Expr,
        negated: bool,
    ) -> Result<Expr, String> {
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
    pub(super) fn parse_select_body(&mut self) -> Result<Select, String> {
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
