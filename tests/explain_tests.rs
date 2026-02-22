use murodb::crypto::aead::MasterKey;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::{execute, ExecResult};
use murodb::storage::pager::Pager;
use murodb::types::Value;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn setup() -> (Pager, SystemCatalog, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    (pager, catalog, dir)
}

fn query_rows(
    sql: &str,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Vec<Vec<(String, Value)>> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows.into_iter().map(|r| r.values).collect(),
        other => panic!("Expected Rows, got {:?}", other),
    }
}

#[test]
fn test_explain_full_scan() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows("EXPLAIN SELECT * FROM t", &mut pager, &mut catalog);
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    // Check column names
    assert_eq!(row[0].0, "id");
    assert_eq!(row[1].0, "select_type");
    assert_eq!(row[2].0, "table");
    assert_eq!(row[3].0, "type");
    assert_eq!(row[4].0, "key");
    assert_eq!(row[5].0, "rows");
    assert_eq!(row[6].0, "cost");
    assert_eq!(row[7].0, "Extra");

    // Check values for full scan
    assert_eq!(row[1].1, Value::Varchar("SIMPLE".to_string()));
    assert_eq!(row[2].1, Value::Varchar("t".to_string()));
    assert_eq!(row[3].1, Value::Varchar("ALL".to_string()));
    assert_eq!(row[4].1, Value::Null); // no key used
}

#[test]
fn test_explain_pk_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("const".to_string()));
    assert_eq!(row[4].1, Value::Varchar("PRIMARY".to_string()));
}

#[test]
fn test_explain_index_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, email VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "CREATE UNIQUE INDEX idx_email ON t(email)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE email = 'test@example.com'",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("ref".to_string()));
    assert_eq!(row[4].1, Value::Varchar("idx_email".to_string()));
}

#[test]
fn test_explain_full_scan_with_where() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE name = 'Alice'",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("ALL".to_string())); // full scan, no index on name
    assert_eq!(row[7].1, Value::Varchar("Using where".to_string()));
}

#[test]
fn test_explain_update_pk_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows(
        "EXPLAIN UPDATE t SET name = 'x' WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[1].1, Value::Varchar("UPDATE".to_string()));
    assert_eq!(row[3].1, Value::Varchar("const".to_string()));
    assert_eq!(row[4].1, Value::Varchar("PRIMARY".to_string()));
}

#[test]
fn test_explain_delete_index_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_name ON t(name)", &mut pager, &mut catalog).unwrap();

    let rows = query_rows(
        "EXPLAIN DELETE FROM t WHERE name = 'Bob'",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[1].1, Value::Varchar("DELETE".to_string()));
    assert_eq!(row[3].1, Value::Varchar("ref".to_string()));
    assert_eq!(row[4].1, Value::Varchar("idx_name".to_string()));
}

#[test]
fn test_explain_composite_index_range_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT, c VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_ab ON t(a, b)", &mut pager, &mut catalog).unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a = 10 AND b >= 3 AND b <= 7",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("range".to_string()));
    assert_eq!(row[4].1, Value::Varchar("idx_ab".to_string()));
    assert!(matches!(row[5].1, Value::Integer(_)));
    assert!(matches!(row[6].1, Value::Integer(_)));
}

#[test]
fn test_explain_prefers_composite_range_over_single_index_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_a ON t(a)", &mut pager, &mut catalog).unwrap();
    execute("CREATE INDEX idx_ab ON t(a, b)", &mut pager, &mut catalog).unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a = 10 AND b >= 3 AND b <= 7",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("range".to_string()));
    assert_eq!(row[4].1, Value::Varchar("idx_ab".to_string()));
}

#[test]
fn test_explain_range_bounds_are_merged_order_independent() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_a ON t(a)", &mut pager, &mut catalog).unwrap();

    for i in 1..=200 {
        execute(
            &format!("INSERT INTO t (id, a) VALUES ({}, {})", i, i),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    let rows1 = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a > 100 AND a > 1",
        &mut pager,
        &mut catalog,
    );
    let rows2 = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a > 1 AND a > 100",
        &mut pager,
        &mut catalog,
    );

    let r1 = &rows1[0];
    let r2 = &rows2[0];
    assert_eq!(r1[3].1, Value::Varchar("range".to_string()));
    assert_eq!(r2[3].1, Value::Varchar("range".to_string()));
    assert_eq!(r1[5].1, Value::Integer(100));
    assert_eq!(r2[5].1, Value::Integer(100));
}

#[test]
fn test_explain_does_not_use_range_for_fts_dependent_bound() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, body TEXT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_a ON t(a)", &mut pager, &mut catalog).unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a < MATCH(body) AGAINST('foo' IN NATURAL LANGUAGE MODE)",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("ALL".to_string()));
}

#[test]
fn test_explain_range_rows_improve_after_analyze_with_numeric_bounds() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_a ON t(a)", &mut pager, &mut catalog).unwrap();

    for i in 1..=1000 {
        execute(
            &format!("INSERT INTO t (id, a) VALUES ({}, {})", i, i),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    let before = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a >= 100 AND a <= 110",
        &mut pager,
        &mut catalog,
    );
    let before_row = &before[0];
    assert_eq!(before_row[3].1, Value::Varchar("range".to_string()));
    let before_rows = match before_row[5].1 {
        Value::Integer(n) => n,
        ref other => panic!("expected integer rows estimate, got {:?}", other),
    };

    execute("ANALYZE TABLE t", &mut pager, &mut catalog).unwrap();
    let after = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a >= 100 AND a <= 110",
        &mut pager,
        &mut catalog,
    );
    let after_row = &after[0];
    assert_eq!(after_row[3].1, Value::Varchar("range".to_string()));
    let after_rows = match after_row[5].1 {
        Value::Integer(n) => n,
        ref other => panic!("expected integer rows estimate, got {:?}", other),
    };

    assert!(after_rows < before_rows);
    assert!(after_rows <= 20);
}

#[test]
fn test_explain_date_range_rows_improve_after_analyze() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, d DATE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_d ON t(d)", &mut pager, &mut catalog).unwrap();

    for day in 1..=200 {
        execute(
            &format!(
                "INSERT INTO t (id, d) VALUES ({}, '2026-03-{:02}')",
                day,
                day.min(28)
            ),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    let before = query_rows(
        "EXPLAIN SELECT * FROM t WHERE d >= CAST('2026-03-10' AS DATE) AND d <= CAST('2026-03-11' AS DATE)",
        &mut pager,
        &mut catalog,
    );
    let before_rows = match before[0][5].1 {
        Value::Integer(n) => n,
        ref other => panic!("expected integer rows estimate, got {:?}", other),
    };

    execute("ANALYZE TABLE t", &mut pager, &mut catalog).unwrap();
    let after = query_rows(
        "EXPLAIN SELECT * FROM t WHERE d >= CAST('2026-03-10' AS DATE) AND d <= CAST('2026-03-11' AS DATE)",
        &mut pager,
        &mut catalog,
    );
    let after_rows = match after[0][5].1 {
        Value::Integer(n) => n,
        ref other => panic!("expected integer rows estimate, got {:?}", other),
    };

    assert!(after_rows < before_rows);
}

#[test]
fn test_explain_histogram_improves_skewed_numeric_range_estimate() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_a ON t(a)", &mut pager, &mut catalog).unwrap();

    // Two dense edge clusters with an empty middle range.
    for i in 1..=100 {
        execute(
            &format!("INSERT INTO t (id, a) VALUES ({}, {})", i, i),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }
    for i in 101..=200 {
        execute(
            &format!("INSERT INTO t (id, a) VALUES ({}, {})", i, 900 + (i - 100)),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    let before = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a >= 500 AND a <= 600",
        &mut pager,
        &mut catalog,
    );
    let before_rows = match before[0][5].1 {
        Value::Integer(n) => n,
        ref other => panic!("expected integer rows estimate, got {:?}", other),
    };

    execute("ANALYZE TABLE t", &mut pager, &mut catalog).unwrap();
    let after = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a >= 500 AND a <= 600",
        &mut pager,
        &mut catalog,
    );
    let after_rows = match after[0][5].1 {
        Value::Integer(n) => n,
        ref other => panic!("expected integer rows estimate, got {:?}", other),
    };

    assert!(after_rows < before_rows);
    assert!(after_rows <= 5);
}

#[test]
fn test_explain_histogram_handles_narrow_span_without_collapsing_to_one() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_a ON t(a)", &mut pager, &mut catalog).unwrap();

    // Very narrow numeric domain (span=1) with many rows.
    for i in 1..=100 {
        execute(
            &format!("INSERT INTO t (id, a) VALUES ({}, 7)", i),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    execute("ANALYZE TABLE t", &mut pager, &mut catalog).unwrap();
    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a >= 7 AND a <= 7",
        &mut pager,
        &mut catalog,
    );
    let est = match rows[0][5].1 {
        Value::Integer(n) => n,
        ref other => panic!("expected integer rows estimate, got {:?}", other),
    };
    assert!(est >= 50);
}

#[test]
fn test_explain_join_extra_shows_loop_order_choice() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t1 (id BIGINT PRIMARY KEY, v INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE t2 (id BIGINT PRIMARY KEY, t1_id BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    for i in 1..=20 {
        execute(
            &format!("INSERT INTO t1 (id, v) VALUES ({}, {})", i, i),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }
    for i in 1..=3 {
        execute(
            &format!("INSERT INTO t2 (id, t1_id) VALUES ({}, {})", i, i),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id",
        &mut pager,
        &mut catalog,
    );
    let extra = match &rows[0][7].1 {
        Value::Varchar(s) => s.clone(),
        other => panic!("expected VARCHAR extra, got {:?}", other),
    };
    assert!(extra.contains("Join loops:"));
    assert!(extra.contains("j1=right_outer"));
}
