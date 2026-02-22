use super::*;

pub(super) const SQL_FTS_TERM_KEY: [u8; 32] = [0x55u8; 32];
pub(super) const SQL_FTS_SCORE_SCALE: f64 = 1_000_000.0;
pub(super) const SQL_FTS_NEXT_DOC_ID_KEY: &[u8] = b"__next_doc_id__";
pub(super) const SQL_FTS_PK2DOC_PREFIX: &[u8] = b"__pk2doc__";
pub(super) const SQL_FTS_DOC2PK_PREFIX: &[u8] = b"__doc2pk__";

#[derive(Clone)]
pub(super) struct FtsEvalContext {
    pub(super) doc_ids: HashMap<MatchExprKey, u64>,
    pub(super) score_maps: HashMap<MatchExprKey, HashMap<u64, i64>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MatchExprKey {
    pub(super) column: String,
    pub(super) query: String,
    pub(super) mode: MatchMode,
}

pub(super) fn execute_fts_scan_rows(
    table_def: &TableDef,
    indexes: &[IndexDef],
    column: &str,
    query: &str,
    mode: MatchMode,
    pager: &mut impl PageStore,
) -> Result<Vec<(u64, Vec<Value>)>> {
    let fts = open_fulltext_index(indexes, &table_def.name, column);
    let fts = fts?;
    let idx = indexes
        .iter()
        .find(|i| {
            i.index_type == IndexType::Fulltext
                && i.column_names.first().map(|c| c.as_str()) == Some(column)
        })
        .ok_or_else(|| {
            MuroError::Execution(format!(
                "FULLTEXT index not found for column '{}' on table '{}'",
                column, table_def.name
            ))
        })?;
    let results = match mode {
        MatchMode::NaturalLanguage => query_natural_with_config(
            &fts,
            pager,
            query,
            FtsQueryConfig {
                stop_filter: idx.fts_stop_filter,
                stop_df_ratio_ppm: idx.fts_stop_df_ratio_ppm,
            },
        )?,
        MatchMode::Boolean => query_boolean(&fts, pager, query)?,
    };

    let data_btree = BTree::open(table_def.data_btree_root);
    let mut rows = Vec::new();
    let meta_btree = BTree::open(idx.btree_root);
    for r in results {
        let pk_key = if let Some(pk_key) = fts_get_pk_key_by_doc_id(&meta_btree, pager, r.doc_id)? {
            pk_key
        } else {
            // Backward compatibility for legacy BIGINT-pk-based doc_id layouts.
            let doc_id_i64 = i64::try_from(r.doc_id).map_err(|_| {
                MuroError::Execution(format!("FTS doc_id {} is out of BIGINT range", r.doc_id))
            })?;
            encode_i64(doc_id_i64).to_vec()
        };
        if let Some(data) = data_btree.search(pager, &pk_key)? {
            let values =
                deserialize_row_versioned(&data, &table_def.columns, table_def.row_format_version)?;
            rows.push((r.doc_id, values));
        }
    }
    Ok(rows)
}

pub(super) fn open_fulltext_index(
    indexes: &[IndexDef],
    table_name: &str,
    column: &str,
) -> Result<FtsIndex> {
    let idx = find_fulltext_index(indexes, table_name, column)?;
    Ok(FtsIndex::open(idx.btree_root, SQL_FTS_TERM_KEY))
}

pub(super) fn find_fulltext_index<'a>(
    indexes: &'a [IndexDef],
    table_name: &str,
    column: &str,
) -> Result<&'a IndexDef> {
    indexes
        .iter()
        .find(|i| {
            i.index_type == IndexType::Fulltext
                && i.column_names.first().map(|c| c.as_str()) == Some(column)
        })
        .ok_or_else(|| {
            MuroError::Execution(format!(
                "FULLTEXT index not found for column '{}' on table '{}'",
                column, table_name
            ))
        })
}

pub(super) fn populate_fts_row_doc_ids(
    fts_ctx: &mut FtsEvalContext,
    pk_key: &[u8],
    indexes: &[IndexDef],
    table_name: &str,
    pager: &mut impl PageStore,
) -> Result<()> {
    fts_ctx.doc_ids.clear();
    let match_keys: Vec<MatchExprKey> = fts_ctx.score_maps.keys().cloned().collect();
    for key in match_keys {
        let idx = find_fulltext_index(indexes, table_name, &key.column)?;
        let meta_btree = BTree::open(idx.btree_root);
        if let Some(doc_id) = fts_get_doc_id(&meta_btree, pager, pk_key)? {
            fts_ctx.doc_ids.insert(key, doc_id);
        }
    }
    Ok(())
}

pub(super) fn build_fts_eval_context(
    select_columns: &[SelectColumn],
    where_clause: &Option<Expr>,
    table_name: &str,
    indexes: &[IndexDef],
    pager: &mut impl PageStore,
) -> Result<FtsEvalContext> {
    let mut keys: HashSet<MatchExprKey> = HashSet::new();
    if let Some(where_expr) = where_clause {
        collect_match_expr_keys(where_expr, &mut keys);
    }
    for col in select_columns {
        if let SelectColumn::Expr(expr, _) = col {
            collect_match_expr_keys(expr, &mut keys);
        }
    }

    let mut score_maps: HashMap<MatchExprKey, HashMap<u64, i64>> = HashMap::new();
    for key in keys {
        let idx = find_fulltext_index(indexes, table_name, &key.column)?;
        let fts = open_fulltext_index(indexes, table_name, &key.column)?;
        let results = match key.mode {
            MatchMode::NaturalLanguage => query_natural_with_config(
                &fts,
                pager,
                &key.query,
                FtsQueryConfig {
                    stop_filter: idx.fts_stop_filter,
                    stop_df_ratio_ppm: idx.fts_stop_df_ratio_ppm,
                },
            )?,
            MatchMode::Boolean => query_boolean(&fts, pager, &key.query)?,
        };
        let mut scores = HashMap::new();
        for result in results {
            scores.insert(result.doc_id, scale_fts_score(&result));
        }
        score_maps.insert(key, scores);
    }

    Ok(FtsEvalContext {
        doc_ids: HashMap::new(),
        score_maps,
    })
}

pub(super) fn collect_match_expr_keys(expr: &Expr, keys: &mut HashSet<MatchExprKey>) {
    match expr {
        Expr::MatchAgainst {
            column,
            query,
            mode,
        } => {
            keys.insert(MatchExprKey {
                column: column.clone(),
                query: query.clone(),
                mode: *mode,
            });
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_match_expr_keys(left, keys);
            collect_match_expr_keys(right, keys);
        }
        Expr::UnaryOp { operand, .. } => collect_match_expr_keys(operand, keys),
        Expr::Like { expr, pattern, .. } => {
            collect_match_expr_keys(expr, keys);
            collect_match_expr_keys(pattern, keys);
        }
        Expr::InList { expr, list, .. } => {
            collect_match_expr_keys(expr, keys);
            for e in list {
                collect_match_expr_keys(e, keys);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_match_expr_keys(expr, keys);
            collect_match_expr_keys(low, keys);
            collect_match_expr_keys(high, keys);
        }
        Expr::IsNull { expr, .. } => collect_match_expr_keys(expr, keys),
        Expr::FtsSnippet { .. } => {}
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_match_expr_keys(arg, keys);
            }
        }
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                collect_match_expr_keys(op, keys);
            }
            for (c, t) in when_clauses {
                collect_match_expr_keys(c, keys);
                collect_match_expr_keys(t, keys);
            }
            if let Some(e) = else_clause {
                collect_match_expr_keys(e, keys);
            }
        }
        Expr::Cast { expr, .. } => collect_match_expr_keys(expr, keys),
        Expr::GreaterThanZero(inner) => collect_match_expr_keys(inner, keys),
        Expr::InSubquery { expr, .. } => collect_match_expr_keys(expr, keys),
        _ => {}
    }
}

pub(super) fn materialize_fts_expr(
    expr: &Expr,
    table_def: &TableDef,
    values: &[Value],
    fts_ctx: Option<&FtsEvalContext>,
) -> Expr {
    match expr {
        Expr::MatchAgainst {
            column,
            query,
            mode,
        } => {
            let key = MatchExprKey {
                column: column.clone(),
                query: query.clone(),
                mode: *mode,
            };
            let score = fts_ctx
                .and_then(|ctx| {
                    ctx.doc_ids.get(&key).and_then(|doc_id| {
                        ctx.score_maps
                            .get(&key)
                            .and_then(|scores| scores.get(doc_id).copied())
                    })
                })
                .unwrap_or(0);
            Expr::IntLiteral(score)
        }
        Expr::FtsSnippet {
            column,
            query,
            pre_tag,
            post_tag,
            context_chars,
        } => {
            let snippet = table_def
                .column_index(column)
                .and_then(|i| values.get(i))
                .and_then(value_to_fts_text)
                .map(|text| fts_snippet(&text, query, pre_tag, post_tag, *context_chars))
                .unwrap_or_default();
            Expr::StringLiteral(snippet)
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(materialize_fts_expr(left, table_def, values, fts_ctx)),
            op: *op,
            right: Box::new(materialize_fts_expr(right, table_def, values, fts_ctx)),
        },
        Expr::UnaryOp { op, operand } => Expr::UnaryOp {
            op: *op,
            operand: Box::new(materialize_fts_expr(operand, table_def, values, fts_ctx)),
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(materialize_fts_expr(expr, table_def, values, fts_ctx)),
            pattern: Box::new(materialize_fts_expr(pattern, table_def, values, fts_ctx)),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(materialize_fts_expr(expr, table_def, values, fts_ctx)),
            list: list
                .iter()
                .map(|e| materialize_fts_expr(e, table_def, values, fts_ctx))
                .collect(),
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(materialize_fts_expr(expr, table_def, values, fts_ctx)),
            low: Box::new(materialize_fts_expr(low, table_def, values, fts_ctx)),
            high: Box::new(materialize_fts_expr(high, table_def, values, fts_ctx)),
            negated: *negated,
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(materialize_fts_expr(expr, table_def, values, fts_ctx)),
            negated: *negated,
        },
        Expr::FunctionCall { name, args } => Expr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| materialize_fts_expr(a, table_def, values, fts_ctx))
                .collect(),
        },
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => Expr::CaseWhen {
            operand: operand
                .as_ref()
                .map(|o| Box::new(materialize_fts_expr(o, table_def, values, fts_ctx))),
            when_clauses: when_clauses
                .iter()
                .map(|(c, t)| {
                    (
                        materialize_fts_expr(c, table_def, values, fts_ctx),
                        materialize_fts_expr(t, table_def, values, fts_ctx),
                    )
                })
                .collect(),
            else_clause: else_clause
                .as_ref()
                .map(|e| Box::new(materialize_fts_expr(e, table_def, values, fts_ctx))),
        },
        Expr::Cast { expr, target_type } => Expr::Cast {
            expr: Box::new(materialize_fts_expr(expr, table_def, values, fts_ctx)),
            target_type: *target_type,
        },
        Expr::GreaterThanZero(inner) => Expr::GreaterThanZero(Box::new(materialize_fts_expr(
            inner, table_def, values, fts_ctx,
        ))),
        _ => expr.clone(),
    }
}

pub(super) fn scale_fts_score(r: &FtsResult) -> i64 {
    (r.score * SQL_FTS_SCORE_SCALE).round() as i64
}

pub(super) fn free_btree_pages(pager: &mut impl PageStore, root_page_id: u64) {
    let btree = BTree::open(root_page_id);
    if let Ok(pages) = btree.collect_all_pages(pager) {
        for page_id in pages {
            pager.free_page(page_id);
        }
    }
}

pub(super) fn fts_pk_to_doc_key(pk_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(SQL_FTS_PK2DOC_PREFIX.len() + pk_key.len());
    key.extend_from_slice(SQL_FTS_PK2DOC_PREFIX);
    key.extend_from_slice(pk_key);
    key
}

pub(super) fn fts_doc_to_pk_key(doc_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(SQL_FTS_DOC2PK_PREFIX.len() + 8);
    key.extend_from_slice(SQL_FTS_DOC2PK_PREFIX);
    key.extend_from_slice(&doc_id.to_le_bytes());
    key
}

pub(super) fn fts_get_doc_id(
    meta_btree: &BTree,
    pager: &mut impl PageStore,
    pk_key: &[u8],
) -> Result<Option<u64>> {
    let key = fts_pk_to_doc_key(pk_key);
    match meta_btree.search(pager, &key)? {
        Some(data) if data.len() >= 8 => {
            let doc_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
            Ok(Some(doc_id))
        }
        Some(_) => Err(MuroError::Corruption(
            "invalid FULLTEXT pk->doc mapping payload".into(),
        )),
        None => Ok(None),
    }
}

pub(super) fn fts_get_pk_key_by_doc_id(
    meta_btree: &BTree,
    pager: &mut impl PageStore,
    doc_id: u64,
) -> Result<Option<Vec<u8>>> {
    let key = fts_doc_to_pk_key(doc_id);
    meta_btree.search(pager, &key)
}

pub(super) fn fts_put_doc_mapping(
    meta_btree: &mut BTree,
    pager: &mut impl PageStore,
    pk_key: &[u8],
    doc_id: u64,
) -> Result<()> {
    let pk2doc_key = fts_pk_to_doc_key(pk_key);
    meta_btree.insert(pager, &pk2doc_key, &doc_id.to_le_bytes())?;
    let doc2pk_key = fts_doc_to_pk_key(doc_id);
    meta_btree.insert(pager, &doc2pk_key, pk_key)?;
    Ok(())
}

pub(super) fn fts_delete_doc_mapping(
    meta_btree: &mut BTree,
    pager: &mut impl PageStore,
    pk_key: &[u8],
    doc_id: u64,
) -> Result<()> {
    let pk2doc_key = fts_pk_to_doc_key(pk_key);
    meta_btree.delete(pager, &pk2doc_key)?;
    let doc2pk_key = fts_doc_to_pk_key(doc_id);
    meta_btree.delete(pager, &doc2pk_key)?;
    Ok(())
}

pub(super) fn fts_set_next_doc_id(
    meta_btree: &mut BTree,
    pager: &mut impl PageStore,
    next_doc_id: u64,
) -> Result<()> {
    meta_btree.insert(pager, SQL_FTS_NEXT_DOC_ID_KEY, &next_doc_id.to_le_bytes())?;
    Ok(())
}

pub(super) fn fts_allocate_doc_id(
    meta_btree: &mut BTree,
    pager: &mut impl PageStore,
) -> Result<u64> {
    let next = match meta_btree.search(pager, SQL_FTS_NEXT_DOC_ID_KEY)? {
        Some(data) if data.len() >= 8 => u64::from_le_bytes(data[0..8].try_into().unwrap()),
        Some(_) => {
            return Err(MuroError::Corruption(
                "invalid FULLTEXT next_doc_id payload".into(),
            ));
        }
        None => 1,
    };
    let new_next = next
        .checked_add(1)
        .ok_or_else(|| MuroError::Execution("FULLTEXT doc_id overflow".into()))?;
    fts_set_next_doc_id(meta_btree, pager, new_next)?;
    Ok(next)
}

pub(super) fn validate_fulltext_parser(fi: &CreateFulltextIndex) -> Result<()> {
    if !fi.parser.eq_ignore_ascii_case("ngram") {
        return Err(MuroError::Execution(format!(
            "Unsupported FULLTEXT parser '{}'; currently only 'ngram' is available",
            fi.parser
        )));
    }
    if fi.ngram_n != 2 {
        return Err(MuroError::Execution(format!(
            "Unsupported ngram size {}; currently only n=2 is available",
            fi.ngram_n
        )));
    }
    if !fi.normalize.eq_ignore_ascii_case("nfkc") {
        return Err(MuroError::Execution(format!(
            "Unsupported normalize='{}'; currently only 'nfkc' is available",
            fi.normalize
        )));
    }
    if fi.stop_df_ratio_ppm > 1_000_000 {
        return Err(MuroError::Execution(format!(
            "stop_df_ratio_ppm={} is out of range (0..=1000000)",
            fi.stop_df_ratio_ppm
        )));
    }
    Ok(())
}

pub(super) fn value_to_fts_text(value: &Value) -> Option<String> {
    match value {
        Value::Varchar(s) => Some(s.clone()),
        Value::Null => None,
        Value::Integer(n) => Some(n.to_string()),
        Value::Float(n) => Some(n.to_string()),
        Value::Date(n) => Some(format_date(*n)),
        Value::DateTime(n) => Some(format_datetime(*n)),
        Value::Timestamp(n) => Some(format_datetime(*n)),
        Value::Varbinary(_) => None,
    }
}

/// Validate that a value fits within the constraints of the data type.
pub(super) fn validate_value(value: &Value, data_type: &DataType) -> Result<()> {
    match (value, data_type) {
        (Value::Integer(n), DataType::TinyInt) if *n < -128 || *n > 127 => {
            Err(MuroError::Execution(format!(
                "Value {} out of range for TINYINT (-128 to 127)",
                n
            )))
        }
        (Value::Integer(n), DataType::SmallInt) if *n < -32768 || *n > 32767 => {
            Err(MuroError::Execution(format!(
                "Value {} out of range for SMALLINT (-32768 to 32767)",
                n
            )))
        }
        (Value::Integer(n), DataType::Int) if *n < i32::MIN as i64 || *n > i32::MAX as i64 => {
            Err(MuroError::Execution(format!(
                "Value {} out of range for INT ({} to {})",
                n,
                i32::MIN,
                i32::MAX
            )))
        }
        (Value::Float(n), DataType::Float) if !n.is_finite() => {
            Err(MuroError::Execution("FLOAT must be a finite value".into()))
        }
        (Value::Float(n), DataType::Double) if !n.is_finite() => {
            Err(MuroError::Execution("DOUBLE must be a finite value".into()))
        }
        (Value::Float(n), DataType::Float) if *n < f32::MIN as f64 || *n > f32::MAX as f64 => Err(
            MuroError::Execution(format!("Value {} out of range for FLOAT", n)),
        ),
        (Value::Varchar(s), DataType::Varchar(Some(max))) if s.len() as u32 > *max => {
            Err(MuroError::Execution(format!(
                "String length {} exceeds VARCHAR({})",
                s.len(),
                max
            )))
        }
        (Value::Varbinary(b), DataType::Varbinary(Some(max))) if b.len() as u32 > *max => {
            Err(MuroError::Execution(format!(
                "Binary length {} exceeds VARBINARY({})",
                b.len(),
                max
            )))
        }
        _ => Ok(()),
    }
}
