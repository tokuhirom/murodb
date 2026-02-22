# Quick Start

This page is a concise command reference.  
For a guided first run, see [First Session (10 Minutes)](first-session.md).

## Create a database

```bash
murodb mydb.db --create -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"
```

You will be prompted for an encryption password.

If you need plaintext mode, opt in explicitly:

```bash
murodb mydb_plain.db --create --encryption off -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"
```

## Insert rows

```bash
murodb mydb.db -e "INSERT INTO t (id, name) VALUES (1, 'hello'), (2, 'world')"
```

## Query rows

```bash
murodb mydb.db -e "SELECT id, name FROM t ORDER BY id"
```

## Show tables

```bash
murodb mydb.db -e "SHOW TABLES"
```

## Run with JSON output

```bash
murodb mydb.db --format json -e "SELECT id, name FROM t ORDER BY id"
```

## Interactive REPL

```bash
murodb mydb.db
```

Start without `-e` to enter the interactive REPL mode.
