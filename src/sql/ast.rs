use crate::types::DataType;

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    CreateFulltextIndex(CreateFulltextIndex),
    Insert(Insert),
    Select(Select),
    Update(Update),
    Delete(Delete),
    ShowTables,
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub table_name: String,
    pub columns: Vec<ColumnSpec>,
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_nullable: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    pub is_unique: bool,
}

#[derive(Debug, Clone)]
pub struct CreateFulltextIndex {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    pub parser: String,    // e.g. "ngram"
    pub ngram_n: usize,    // e.g. 2
    pub normalize: String, // e.g. "nfkc"
}

#[derive(Debug, Clone)]
pub struct Insert {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Cross,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table_name: String,
    pub alias: Option<String>,
    pub on_condition: Option<Expr>, // None for CROSS JOIN
}

#[derive(Debug, Clone)]
pub struct Select {
    pub columns: Vec<SelectColumn>,
    pub table_name: String,
    pub table_alias: Option<String>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    Star,
    Expr(Expr, Option<String>), // expression, optional alias
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub struct Update {
    pub table_name: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct Delete {
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum Expr {
    IntLiteral(i64),
    StringLiteral(String),
    BlobLiteral(Vec<u8>),
    Null,
    ColumnRef(String),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    MatchAgainst {
        column: String,
        query: String,
        mode: MatchMode,
    },
    FtsSnippet {
        column: String,
        query: String,
        pre_tag: String,
        post_tag: String,
        context_chars: usize,
    },
    /// Comparison result: expr > 0 (used as a where clause)
    GreaterThanZero(Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchMode {
    NaturalLanguage,
    Boolean,
}
