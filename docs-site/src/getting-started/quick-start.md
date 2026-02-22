# Quick Start

## Create a new database

```bash
murodb mydb.db --create -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"
```

You will be prompted for an encryption password.

If you need plaintext mode, opt in explicitly:

```bash
murodb mydb_plain.db --create --encryption off -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"
```

## Insert data

```bash
murodb mydb.db -e "INSERT INTO t (id, name) VALUES (1, 'hello')"
```

## Query data

```bash
murodb mydb.db -e "SELECT * FROM t"
```

## Show tables

```bash
murodb mydb.db -e "SHOW TABLES"
```

## Interactive REPL

```bash
murodb mydb.db
```

Start without `-e` to enter the interactive REPL mode.
