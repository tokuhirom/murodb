use crate::types::DataType;

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    CreateFulltextIndex(CreateFulltextIndex),
    DropTable(DropTable),
    DropIndex(DropIndex),
    AlterTable(AlterTable),
    RenameTable(RenameTable),
    Insert(Insert),
    Select(Box<Select>),
    Update(Update),
    Delete(Delete),
    ShowTables,
    ShowCreateTable(String),
    Describe(String),
    Begin,
    Commit,
    Rollback,
    ShowCheckpointStats,
    ShowDatabaseStats,
}

#[derive(Debug, Clone)]
pub enum AlterTableOp {
    AddColumn(ColumnSpec),
    DropColumn(String),
    ModifyColumn(ColumnSpec),
    ChangeColumn(String, ColumnSpec), // (old_name, new_spec)
}

#[derive(Debug, Clone)]
pub struct AlterTable {
    pub table_name: String,
    pub operation: AlterTableOp,
}

#[derive(Debug, Clone)]
pub struct RenameTable {
    pub old_name: String,
    pub new_name: String,
}

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub table_name: String,
    pub columns: Vec<ColumnSpec>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_nullable: bool,
    pub default_value: Option<Expr>,
    pub auto_increment: bool,
    pub check_expr: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    pub is_unique: bool,
    pub if_not_exists: bool,
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
pub struct DropTable {
    pub table_name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropIndex {
    pub index_name: String,
    pub if_exists: bool,
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
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    pub table_name: String,
    pub table_alias: Option<String>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Expr>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
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
    DefaultValue,
    ColumnRef(String),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expr>,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool, // true = IS NOT NULL
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
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    CaseWhen {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        target_type: DataType,
    },
    AggregateFunc {
        name: String,           // COUNT, SUM, AVG, MIN, MAX
        arg: Option<Box<Expr>>, // None for COUNT(*)
        distinct: bool,         // COUNT(DISTINCT col)
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
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchMode {
    NaturalLanguage,
    Boolean,
}
