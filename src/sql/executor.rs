/// SQL executor: executes statements against the B-tree storage.
use std::collections::HashMap;
use std::collections::HashSet;

use crate::btree::key_encoding::{
    encode_composite_key, encode_f32, encode_f64, encode_i16, encode_i32, encode_i64, encode_i8,
};
use crate::btree::ops::BTree;
use crate::error::{MuroError, Result};
use crate::fts::index::{FtsIndex, FtsPendingOp};
use crate::fts::query::{query_boolean, query_natural_with_config, FtsQueryConfig, FtsResult};
use crate::fts::snippet::fts_snippet;
use crate::schema::catalog::{ForeignKeyDef, SystemCatalog, TableDef};
use crate::schema::column::{ColumnDef, DefaultValue};
use crate::schema::index::{IndexDef, IndexType};
use crate::sql::ast::*;
use crate::sql::eval::{eval_expr, is_truthy};
use crate::sql::parser::parse_sql;
use crate::sql::planner::{
    choose_nested_loop_order, estimate_plan_rows_hint, plan_cost_hint_with_stats,
    plan_select_with_hints, IndexPlanStat, JoinLoopOrder, Plan, PlannerStats,
};
use crate::storage::page::PageId;
use crate::storage::page_store::PageStore;
use crate::types::{
    format_date, format_datetime, parse_date_string, parse_datetime_string, parse_timestamp_string,
    parse_uuid_string, DataType, Value, ValueKey,
};

mod aggregation;
mod alter;
mod codec;
mod ddl;
mod foreign_key;
mod fts;
mod indexing;
mod insert;
mod mutation;
mod row_format;
mod select_join;
mod select_meta;
mod select_query;
mod show;
mod subquery;

pub use codec::{deserialize_row, deserialize_row_versioned, encode_value, serialize_row};

use aggregation::{cmp_values, execute_aggregation, execute_aggregation_join, has_aggregates};
use alter::*;
use codec::default_value_for_column;
use ddl::*;
use foreign_key::{
    enforce_child_foreign_keys, enforce_parent_restrict_on_delete,
    enforce_parent_restrict_on_update,
};
use fts::{
    build_fts_eval_context, execute_fts_scan_rows, free_btree_pages, fts_allocate_doc_id,
    fts_delete_doc_mapping, fts_get_doc_id, fts_put_doc_mapping, fts_set_next_doc_id,
    materialize_fts_expr, populate_fts_row_doc_ids, validate_fulltext_parser, validate_value,
    value_to_fts_text, FtsEvalContext,
};
use indexing::{
    check_unique_index_constraints, check_unique_index_constraints_excluding,
    delete_from_secondary_indexes, encode_index_key_from_row, encode_pk_key, eval_index_seek_key,
    eval_pk_seek_key, find_unique_index_conflict, index_seek_pk_keys, index_seek_pk_keys_range,
    insert_into_secondary_indexes, persist_indexes,
};
use insert::*;
use mutation::*;
use row_format::*;
use select_join::*;
use select_meta::*;
use select_query::*;
use show::*;
use subquery::*;

/// A result row.
#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<(String, Value)>,
}

impl Row {
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.values.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }
}

/// Execution result.
#[derive(Debug)]
pub enum ExecResult {
    Rows(Vec<Row>),
    RowsAffected(u64),
    Ok,
}
pub fn execute(
    sql: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let stmt = parse_sql(sql).map_err(MuroError::Parse)?;
    execute_statement(&stmt, pager, catalog)
}

pub fn execute_statement(
    stmt: &Statement,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table(ct, pager, catalog),
        Statement::CreateIndex(ci) => exec_create_index(ci, pager, catalog),
        Statement::CreateFulltextIndex(fi) => exec_create_fulltext_index(fi, pager, catalog),
        Statement::AnalyzeTable(table_name) => exec_analyze_table(table_name, pager, catalog),
        Statement::DropTable(dt) => exec_drop_table(dt, pager, catalog),
        Statement::DropIndex(di) => exec_drop_index(di, pager, catalog),
        Statement::AlterTable(at) => exec_alter_table(at, pager, catalog),
        Statement::RenameTable(rt) => exec_rename_table(rt, pager, catalog),
        Statement::Insert(ins) => exec_insert(ins, pager, catalog),
        Statement::Select(sel) => exec_select(sel, pager, catalog),
        Statement::Explain(inner) => exec_explain(inner, pager, catalog),
        Statement::SetQuery(sq) => exec_set_query(sq, pager, catalog),
        Statement::Update(upd) => exec_update(upd, pager, catalog),
        Statement::Delete(del) => exec_delete(del, pager, catalog),
        Statement::ShowTables => exec_show_tables(pager, catalog),
        Statement::ShowCreateTable(name) => exec_show_create_table(name, pager, catalog),
        Statement::Describe(name) => exec_describe(name, pager, catalog),
        Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::ShowCheckpointStats
        | Statement::ShowDatabaseStats
        | Statement::SetRuntimeOption(_) => Err(MuroError::Execution(
            "BEGIN/COMMIT/ROLLBACK/SHOW CHECKPOINT STATS/SHOW DATABASE STATS/SET runtime option must be handled by Session".into(),
        )),
    }
}

#[cfg(test)]
mod tests;
