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

fn setup_users_orders(pager: &mut Pager, catalog: &mut SystemCatalog) {
    execute(
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)",
        pager,
        catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, product VARCHAR)",
        pager,
        catalog,
    )
    .unwrap();

    execute("INSERT INTO users VALUES (1, 'Alice')", pager, catalog).unwrap();
    execute("INSERT INTO users VALUES (2, 'Bob')", pager, catalog).unwrap();
    execute("INSERT INTO users VALUES (3, 'Charlie')", pager, catalog).unwrap();

    execute(
        "INSERT INTO orders VALUES (10, 1, 'Widget')",
        pager,
        catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO orders VALUES (11, 1, 'Gadget')",
        pager,
        catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO orders VALUES (12, 2, 'Doohickey')",
        pager,
        catalog,
    )
    .unwrap();
}

#[test]
fn test_inner_join_basic() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_users_orders(&mut pager, &mut catalog);

    let result = execute(
        "SELECT users.name, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id ORDER BY orders.id",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0].get("users.name"),
            Some(&Value::Varchar("Alice".into()))
        );
        assert_eq!(
            rows[0].get("orders.product"),
            Some(&Value::Varchar("Widget".into()))
        );
        assert_eq!(
            rows[1].get("orders.product"),
            Some(&Value::Varchar("Gadget".into()))
        );
        assert_eq!(
            rows[2].get("users.name"),
            Some(&Value::Varchar("Bob".into()))
        );
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_inner_join_implicit() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_users_orders(&mut pager, &mut catalog);

    // JOIN without INNER keyword
    let result = execute(
        "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.id",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 3);
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_left_join_with_null() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_users_orders(&mut pager, &mut catalog);

    // Charlie has no orders -> should appear with NULL product
    let result = execute(
        "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 4); // Alice(2) + Bob(1) + Charlie(1 with NULL)
                                   // Charlie's row should have NULL product
        let charlie_row = rows
            .iter()
            .find(|r| r.get("users.name") == Some(&Value::Varchar("Charlie".into())))
            .expect("Charlie should be in results");
        assert_eq!(charlie_row.get("orders.product"), Some(&Value::Null));
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_cross_join() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE colors (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE sizes (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO colors VALUES (1, 'Red')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO colors VALUES (2, 'Blue')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO sizes VALUES (1, 'S')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO sizes VALUES (2, 'M')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO sizes VALUES (3, 'L')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT colors.name, sizes.name FROM colors CROSS JOIN sizes",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 6); // 2 colors * 3 sizes
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_join_with_alias() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_users_orders(&mut pager, &mut catalog);

    let result = execute(
        "SELECT u.name, o.product FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id ORDER BY o.id",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("u.name"), Some(&Value::Varchar("Alice".into())));
        assert_eq!(
            rows[0].get("o.product"),
            Some(&Value::Varchar("Widget".into()))
        );
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_join_with_alias_no_as() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_users_orders(&mut pager, &mut catalog);

    // Alias without AS keyword
    let result = execute(
        "SELECT u.name, o.product FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.id",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("u.name"), Some(&Value::Varchar("Alice".into())));
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_join_with_where() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_users_orders(&mut pager, &mut catalog);

    let result = execute(
        "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE orders.product = 'Widget'",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("users.name"),
            Some(&Value::Varchar("Alice".into()))
        );
        assert_eq!(
            rows[0].get("orders.product"),
            Some(&Value::Varchar("Widget".into()))
        );
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_join_with_order_by_and_limit() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_users_orders(&mut pager, &mut catalog);

    let result = execute(
        "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.id DESC LIMIT 2",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 2);
        // order_id DESC: 12, 11, 10 → first two: Doohickey, Gadget
        assert_eq!(
            rows[0].get("orders.product"),
            Some(&Value::Varchar("Doohickey".into()))
        );
        assert_eq!(
            rows[1].get("orders.product"),
            Some(&Value::Varchar("Gadget".into()))
        );
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_join_select_star() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_users_orders(&mut pager, &mut catalog);

    let result = execute(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.id LIMIT 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        // Should have columns from both tables
        assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alice".into())));
        assert_eq!(
            rows[0].get("product"),
            Some(&Value::Varchar("Widget".into()))
        );
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_multiple_joins() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, product_id BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO users VALUES (1, 'Alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO products VALUES (100, 'Widget')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO products VALUES (101, 'Gadget')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO orders VALUES (10, 1, 100)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO orders VALUES (11, 1, 101)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT u.name, p.name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id ORDER BY p.name",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].get("p.name"),
            Some(&Value::Varchar("Gadget".into()))
        );
        assert_eq!(
            rows[1].get("p.name"),
            Some(&Value::Varchar("Widget".into()))
        );
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_left_join_empty_right() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE a (id BIGINT PRIMARY KEY, val VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE b (id BIGINT PRIMARY KEY, a_id BIGINT, val VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute("INSERT INTO a VALUES (1, 'x')", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO a VALUES (2, 'y')", &mut pager, &mut catalog).unwrap();
    // b is empty

    let result = execute(
        "SELECT a.val, b.val FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("a.val"), Some(&Value::Varchar("x".into())));
        assert_eq!(rows[0].get("b.val"), Some(&Value::Null));
        assert_eq!(rows[1].get("a.val"), Some(&Value::Varchar("y".into())));
        assert_eq!(rows[1].get("b.val"), Some(&Value::Null));
    } else {
        panic!("Expected rows");
    }
}
