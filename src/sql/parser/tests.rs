use super::*;

#[test]
fn test_parse_create_table() {
    let stmt =
        parse_sql("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, data VARBINARY)")
            .unwrap();
    if let Statement::CreateTable(ct) = stmt {
        assert_eq!(ct.table_name, "users");
        assert_eq!(ct.columns.len(), 3);
        assert!(ct.columns[0].is_primary_key);
        assert_eq!(ct.columns[1].data_type, DataType::Varchar(None));
        assert!(!ct.if_not_exists);
    } else {
        panic!("Expected CreateTable");
    }
}

#[test]
fn test_parse_create_table_if_not_exists() {
    let stmt = parse_sql("CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    if let Statement::CreateTable(ct) = stmt {
        assert!(ct.if_not_exists);
        assert_eq!(ct.table_name, "users");
    } else {
        panic!("Expected CreateTable");
    }
}

#[test]
fn test_parse_insert() {
    let stmt = parse_sql("INSERT INTO t (id, name) VALUES (1, 'hello')").unwrap();
    if let Statement::Insert(ins) = stmt {
        assert_eq!(ins.table_name, "t");
        assert_eq!(
            ins.columns,
            Some(vec!["id".to_string(), "name".to_string()])
        );
        assert_eq!(ins.values.len(), 1);
        assert_eq!(ins.values[0].len(), 2);
    } else {
        panic!("Expected Insert");
    }
}

#[test]
fn test_parse_select() {
    let stmt = parse_sql("SELECT * FROM t WHERE id = 42 ORDER BY id ASC LIMIT 10").unwrap();
    if let Statement::Select(sel) = stmt {
        assert_eq!(sel.table_name.as_deref(), Some("t"));
        assert!(sel.where_clause.is_some());
        assert!(sel.order_by.is_some());
        assert_eq!(sel.limit, Some(10));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_select_with_offset() {
    let stmt = parse_sql("SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 5").unwrap();
    if let Statement::Select(sel) = stmt {
        assert_eq!(sel.limit, Some(10));
        assert_eq!(sel.offset, Some(5));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_update() {
    let stmt = parse_sql("UPDATE t SET name = 'new' WHERE id = 1").unwrap();
    if let Statement::Update(upd) = stmt {
        assert_eq!(upd.table_name, "t");
        assert_eq!(upd.assignments.len(), 1);
    } else {
        panic!("Expected Update");
    }
}

#[test]
fn test_parse_delete() {
    let stmt = parse_sql("DELETE FROM t WHERE id = 1").unwrap();
    if let Statement::Delete(del) = stmt {
        assert_eq!(del.table_name, "t");
        assert!(del.where_clause.is_some());
    } else {
        panic!("Expected Delete");
    }
}

#[test]
fn test_parse_create_unique_index() {
    let stmt = parse_sql("CREATE UNIQUE INDEX idx_email ON users(email)").unwrap();
    if let Statement::CreateIndex(ci) = stmt {
        assert_eq!(ci.index_name, "idx_email");
        assert!(ci.is_unique);
    } else {
        panic!("Expected CreateIndex");
    }
}

#[test]
fn test_parse_create_fulltext_index() {
    let stmt = parse_sql(
            "CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc', stop_filter='on', stop_df_ratio_ppm=150000)",
        ).unwrap();
    if let Statement::CreateFulltextIndex(fi) = stmt {
        assert_eq!(fi.index_name, "ft_body");
        assert_eq!(fi.column_name, "body");
        assert_eq!(fi.ngram_n, 2);
        assert!(fi.stop_filter);
        assert_eq!(fi.stop_df_ratio_ppm, 150_000);
    } else {
        panic!("Expected CreateFulltextIndex");
    }
}

#[test]
fn test_parse_create_fulltext_index_defaults_stop_filter_options() {
    let stmt =
        parse_sql("CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (n=2)")
            .unwrap();
    if let Statement::CreateFulltextIndex(fi) = stmt {
        assert!(!fi.stop_filter);
        assert_eq!(fi.stop_df_ratio_ppm, 200_000);
    } else {
        panic!("Expected CreateFulltextIndex");
    }
}

#[test]
fn test_parse_create_fulltext_index_accepts_unquoted_stop_filter_on() {
    let stmt = parse_sql(
        "CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (stop_filter=on)",
    )
    .unwrap();
    if let Statement::CreateFulltextIndex(fi) = stmt {
        assert!(fi.stop_filter);
    } else {
        panic!("Expected CreateFulltextIndex");
    }
}

#[test]
fn test_parse_create_fulltext_index_rejects_too_large_stop_df_ratio_ppm() {
    let err = parse_sql(
            "CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (stop_df_ratio_ppm=4294967297)",
        )
        .unwrap_err();
    assert!(err.contains("stop_df_ratio_ppm is too large"));
}

#[test]
fn test_parse_match_against() {
    let stmt = parse_sql(
        "SELECT * FROM t WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0",
    )
    .unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(sel.where_clause.is_some());
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_drop_table() {
    let stmt = parse_sql("DROP TABLE t").unwrap();
    if let Statement::DropTable(dt) = stmt {
        assert_eq!(dt.table_name, "t");
        assert!(!dt.if_exists);
    } else {
        panic!("Expected DropTable");
    }
}

#[test]
fn test_parse_drop_table_if_exists() {
    let stmt = parse_sql("DROP TABLE IF EXISTS t").unwrap();
    if let Statement::DropTable(dt) = stmt {
        assert_eq!(dt.table_name, "t");
        assert!(dt.if_exists);
    } else {
        panic!("Expected DropTable");
    }
}

#[test]
fn test_parse_drop_index() {
    let stmt = parse_sql("DROP INDEX idx_name").unwrap();
    if let Statement::DropIndex(di) = stmt {
        assert_eq!(di.index_name, "idx_name");
        assert!(!di.if_exists);
    } else {
        panic!("Expected DropIndex");
    }
}

#[test]
fn test_parse_show_create_table() {
    let stmt = parse_sql("SHOW CREATE TABLE users").unwrap();
    if let Statement::ShowCreateTable(name) = stmt {
        assert_eq!(name, "users");
    } else {
        panic!("Expected ShowCreateTable");
    }
}

#[test]
fn test_parse_describe() {
    let stmt = parse_sql("DESCRIBE users").unwrap();
    if let Statement::Describe(name) = stmt {
        assert_eq!(name, "users");
    } else {
        panic!("Expected Describe");
    }
}

#[test]
fn test_parse_like() {
    let stmt = parse_sql("SELECT * FROM t WHERE name LIKE '%foo%'").unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(matches!(
            sel.where_clause,
            Some(Expr::Like { negated: false, .. })
        ));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_not_like() {
    let stmt = parse_sql("SELECT * FROM t WHERE name NOT LIKE '%foo%'").unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(matches!(
            sel.where_clause,
            Some(Expr::Like { negated: true, .. })
        ));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_in() {
    let stmt = parse_sql("SELECT * FROM t WHERE id IN (1, 2, 3)").unwrap();
    if let Statement::Select(sel) = stmt {
        if let Some(Expr::InList { list, negated, .. }) = sel.where_clause {
            assert!(!negated);
            assert_eq!(list.len(), 3);
        } else {
            panic!("Expected InList");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_between() {
    let stmt = parse_sql("SELECT * FROM t WHERE id BETWEEN 1 AND 10").unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(matches!(
            sel.where_clause,
            Some(Expr::Between { negated: false, .. })
        ));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_is_null() {
    let stmt = parse_sql("SELECT * FROM t WHERE name IS NULL").unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(matches!(
            sel.where_clause,
            Some(Expr::IsNull { negated: false, .. })
        ));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_is_not_null() {
    let stmt = parse_sql("SELECT * FROM t WHERE name IS NOT NULL").unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(matches!(
            sel.where_clause,
            Some(Expr::IsNull { negated: true, .. })
        ));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_arithmetic() {
    let stmt = parse_sql("SELECT a + b * c FROM t").unwrap();
    if let Statement::Select(sel) = stmt {
        // a + (b * c) due to precedence
        if let SelectColumn::Expr(
            Expr::BinaryOp {
                op: BinaryOp::Add, ..
            },
            _,
        ) = &sel.columns[0]
        {
            // good
        } else {
            panic!("Expected addition at top level");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_unary_minus() {
    let stmt = parse_sql("SELECT * FROM t WHERE id = -1").unwrap();
    if let Statement::Select(sel) = stmt {
        if let Some(Expr::BinaryOp { right, .. }) = sel.where_clause {
            assert!(matches!(*right, Expr::IntLiteral(-1)));
        } else {
            panic!("Expected BinaryOp");
        }
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_default_value() {
    let stmt = parse_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, status INT DEFAULT 0)").unwrap();
    if let Statement::CreateTable(ct) = stmt {
        assert!(ct.columns[1].default_value.is_some());
    } else {
        panic!("Expected CreateTable");
    }
}

#[test]
fn test_parse_auto_increment() {
    let stmt =
        parse_sql("CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR)").unwrap();
    if let Statement::CreateTable(ct) = stmt {
        assert!(ct.columns[0].auto_increment);
    } else {
        panic!("Expected CreateTable");
    }
}

#[test]
fn test_parse_check_constraint() {
    let stmt =
        parse_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, age INT CHECK (age > 0))").unwrap();
    if let Statement::CreateTable(ct) = stmt {
        assert!(ct.columns[1].check_expr.is_some());
    } else {
        panic!("Expected CreateTable");
    }
}

#[test]
fn test_parse_boolean_type() {
    let stmt =
        parse_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, active BOOLEAN DEFAULT 0)").unwrap();
    if let Statement::CreateTable(ct) = stmt {
        assert_eq!(ct.columns[1].data_type, DataType::TinyInt);
    } else {
        panic!("Expected CreateTable");
    }
}

#[test]
fn test_parse_not_operator() {
    let stmt = parse_sql("SELECT * FROM t WHERE NOT id = 1").unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(matches!(
            sel.where_clause,
            Some(Expr::UnaryOp {
                op: UnaryOp::Not,
                ..
            })
        ));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_parse_analyze_table() {
    let stmt = parse_sql("ANALYZE TABLE users").unwrap();
    if let Statement::AnalyzeTable(name) = stmt {
        assert_eq!(name, "users");
    } else {
        panic!("Expected AnalyzeTable");
    }
}
