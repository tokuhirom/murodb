/// SQL lexer (tokenizer) using nom.
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, digit1, multispace0},
    combinator::{opt, value},
    IResult,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Create,
    Table,
    Index,
    Unique,
    Fulltext,
    With,
    Parser,
    Options,
    On,
    Select,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    From,
    Where,
    And,
    Or,
    Not,
    Null,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    As,
    Match,
    Against,
    In,
    Natural,
    Language,
    Mode,
    Boolean,
    Show,
    Tables,
    PrimaryKey,    // "PRIMARY KEY" as a combined token
    Int64Type,     // "INT64"
    VarcharType,   // "VARCHAR"
    VarbinaryType, // "VARBINARY"

    // Literals
    Integer(i64),
    StringLit(String),

    // Identifiers
    Ident(String),

    // Symbols
    LParen,
    RParen,
    Comma,
    Star,
    Semicolon,
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Dot,

    // Special function names
    FtsSnippet, // "fts_snippet"
}

/// Tokenize a SQL string.
pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let mut remaining = input.trim();

    while !remaining.is_empty() {
        // Skip whitespace
        match multispace0::<&str, nom::error::Error<&str>>(remaining) {
            Ok((rest, _)) => remaining = rest,
            Err(_) => break,
        }

        if remaining.is_empty() {
            break;
        }

        // Try to match a token
        match lex_token(remaining) {
            Ok((rest, token)) => {
                tokens.push(token);
                remaining = rest;
            }
            Err(_) => {
                return Err(format!(
                    "Unexpected character at: '{}'",
                    &remaining[..remaining.len().min(20)]
                ));
            }
        }
    }

    Ok(tokens)
}

fn lex_token(input: &str) -> IResult<&str, Token> {
    alt((
        lex_symbol,
        lex_string_literal,
        lex_number,
        lex_keyword_or_ident,
    ))(input)
}

fn lex_symbol(input: &str) -> IResult<&str, Token> {
    alt((
        value(Token::Le, tag("<=")),
        value(Token::Ge, tag(">=")),
        value(Token::Ne, alt((tag("!="), tag("<>")))),
        value(Token::LParen, char('(')),
        value(Token::RParen, char(')')),
        value(Token::Comma, char(',')),
        value(Token::Star, char('*')),
        value(Token::Semicolon, char(';')),
        value(Token::Eq, char('=')),
        value(Token::Lt, char('<')),
        value(Token::Gt, char('>')),
        value(Token::Dot, char('.')),
    ))(input)
}

fn lex_string_literal(input: &str) -> IResult<&str, Token> {
    let (input, _) = char('\'')(input)?;
    let mut result = String::new();
    let mut chars = input.chars();
    let mut consumed = 0;

    loop {
        match chars.next() {
            Some('\'') => {
                consumed += 1;
                // Check for escaped quote ''
                if chars.clone().next() == Some('\'') {
                    chars.next();
                    consumed += 1;
                    result.push('\'');
                } else {
                    break;
                }
            }
            Some(c) => {
                consumed += c.len_utf8();
                result.push(c);
            }
            None => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Char,
                )));
            }
        }
    }

    Ok((&input[consumed..], Token::StringLit(result)))
}

fn lex_number(input: &str) -> IResult<&str, Token> {
    let (input, neg) = opt(char('-'))(input)?;
    let (input, digits) = digit1(input)?;

    let mut num: i64 = digits.parse().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;

    if neg.is_some() {
        num = -num;
    }

    Ok((input, Token::Integer(num)))
}

fn lex_keyword_or_ident(input: &str) -> IResult<&str, Token> {
    let (remaining, word) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    let upper = word.to_uppercase();

    let token = match upper.as_str() {
        "CREATE" => Token::Create,
        "TABLE" => Token::Table,
        "INDEX" => Token::Index,
        "UNIQUE" => Token::Unique,
        "FULLTEXT" => Token::Fulltext,
        "WITH" => Token::With,
        "PARSER" => Token::Parser,
        "OPTIONS" => Token::Options,
        "ON" => Token::On,
        "SELECT" => Token::Select,
        "INSERT" => Token::Insert,
        "INTO" => Token::Into,
        "VALUES" => Token::Values,
        "UPDATE" => Token::Update,
        "SET" => Token::Set,
        "DELETE" => Token::Delete,
        "FROM" => Token::From,
        "WHERE" => Token::Where,
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "NULL" => Token::Null,
        "ORDER" => Token::Order,
        "BY" => Token::By,
        "ASC" => Token::Asc,
        "DESC" => Token::Desc,
        "LIMIT" => Token::Limit,
        "AS" => Token::As,
        "MATCH" => Token::Match,
        "AGAINST" => Token::Against,
        "IN" => Token::In,
        "NATURAL" => Token::Natural,
        "LANGUAGE" => Token::Language,
        "MODE" => Token::Mode,
        "BOOLEAN" => Token::Boolean,
        "SHOW" => Token::Show,
        "TABLES" => Token::Tables,
        "PRIMARY" => {
            // Check if next tokens form "PRIMARY KEY"
            let rest = remaining.trim_start();
            if rest.len() >= 3 && rest[..3].eq_ignore_ascii_case("KEY") {
                let after_key = &rest[3..];
                // Make sure "KEY" is not part of a longer word
                if after_key.is_empty() || !after_key.chars().next().unwrap().is_alphanumeric() {
                    return Ok((after_key, Token::PrimaryKey));
                }
            }
            Token::Ident(word.to_string())
        }
        "INT64" => Token::Int64Type,
        "VARCHAR" => Token::VarcharType,
        "VARBINARY" => Token::VarbinaryType,
        "FTS_SNIPPET" => Token::FtsSnippet,
        _ => Token::Ident(word.to_string()),
    };

    Ok((remaining, token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_create_table() {
        let tokens = tokenize("CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)").unwrap();
        assert_eq!(tokens[0], Token::Create);
        assert_eq!(tokens[1], Token::Table);
        assert_eq!(tokens[2], Token::Ident("t".to_string()));
        assert_eq!(tokens[3], Token::LParen);
        assert_eq!(tokens[4], Token::Ident("id".to_string()));
        assert_eq!(tokens[5], Token::Int64Type);
        assert_eq!(tokens[6], Token::PrimaryKey);
    }

    #[test]
    fn test_tokenize_select() {
        let tokens = tokenize("SELECT * FROM t WHERE id = 42").unwrap();
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
        assert_eq!(tokens[3], Token::Ident("t".to_string()));
        assert_eq!(tokens[4], Token::Where);
        assert_eq!(tokens[5], Token::Ident("id".to_string()));
        assert_eq!(tokens[6], Token::Eq);
        assert_eq!(tokens[7], Token::Integer(42));
    }

    #[test]
    fn test_tokenize_string_literal() {
        let tokens = tokenize("INSERT INTO t VALUES (1, 'hello world')").unwrap();
        assert!(tokens.contains(&Token::StringLit("hello world".to_string())));
    }

    #[test]
    fn test_tokenize_match_against() {
        let tokens = tokenize(
            "SELECT * FROM t WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0",
        )
        .unwrap();
        assert!(tokens.contains(&Token::Match));
        assert!(tokens.contains(&Token::Against));
        assert!(tokens.contains(&Token::Natural));
    }
}
