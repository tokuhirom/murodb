use super::*;

impl Parser {
    pub(super) fn parse_between_rest(&mut self, left: Expr, negated: bool) -> Result<Expr, String> {
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

    pub(super) fn parse_additive(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_multiplicative(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_unary(&mut self) -> Result<Expr, String> {
        if self.peek() == Some(&Token::Minus) {
            self.advance();
            // Handle i64::MIN: the lexer cannot parse 9223372036854775808 as i64,
            // so it becomes an Ident. We detect this case and parse "-{digits}" as i64.
            if let Some(Token::Ident(s)) = self.peek() {
                if s.chars().all(|c| c.is_ascii_digit()) {
                    let neg_str = format!("-{}", s);
                    if let Ok(n) = neg_str.parse::<i64>() {
                        self.advance();
                        return Ok(Expr::IntLiteral(n));
                    }
                }
            }
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

    pub(super) fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.peek().cloned() {
            Some(Token::Question) => {
                self.advance();
                Ok(Expr::BindParam)
            }
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
            Some(Token::HexLiteral(bytes)) => {
                self.advance();
                Ok(Expr::BlobLiteral(bytes))
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

    pub(super) fn parse_case_when(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_cast(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_aggregate_func(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_match_against(&mut self) -> Result<Expr, String> {
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

    pub(super) fn parse_fts_snippet(&mut self) -> Result<Expr, String> {
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
