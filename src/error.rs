use thiserror::Error;

#[derive(Error, Debug)]
pub enum MuroError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Encryption error: {0}")]
    Encryption(String),

    #[error("Decryption error: page may be corrupted or tampered")]
    Decryption,

    #[error("Page overflow: data exceeds page capacity")]
    PageOverflow,

    #[error("Page not found: page_id={0}")]
    PageNotFound(u64),

    #[error("Invalid page format")]
    InvalidPage,

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("SQL parse error: {0}")]
    Parse(String),

    #[error("SQL execution error: {0}")]
    Execution(String),

    #[error("Unique constraint violation: {0}")]
    UniqueViolation(String),

    #[error("Type error: {0}")]
    Type(String),

    #[error("Lock error: {0}")]
    Lock(String),

    #[error("FTS error: {0}")]
    Fts(String),

    #[error("KDF error: {0}")]
    Kdf(String),

    #[error("Data corruption: {0}")]
    Corruption(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, MuroError>;
