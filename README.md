# CarcinusDB

> Simple sqlite-like database writen in Rust from scratch.

CarcinusDB is an educational database engine built entirely in Rust,
implementing core database internals from the ground up — including a
B-link tree storage layer, write-ahead log, a Volcano-model query
executor, and SQL parsing. I decided to built it, because I always wanted
to know how databases like sqlite or postgres work under the hood.
Also this two projects heavily influenced design decisions durring
development and are worth checking:

- https://github.com/antoniosarosi/mkdb
- https://github.com/tursodatabase/turso

---

## Features

- **B-link Tree Storage** — Concurrent-safe B-link tree implementation 
  for both table and index pages, with proper high-key fencing, sibling
  pointer chaining and structural correctness across splits, merges, and
  root collapse.

- **Write-Ahead Log (WAL)** — Crash-safe transaction durability.

- **Volcano Iterator Model** — Query execution via a composable iterator
  (pull-based) pipeline: `SeqScan`, `IndexScan`, `Filter`, `Project`,
  `Sort`, `Limit`, and more.

- **SQL Query Execution** — Parses and executes a practical subset of
  SQL, including `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`,
  and `CREATE INDEX`.

- **Transaction System** — Explicit transaction control with a
  `Transaction` struct and lazy-escalating `LockLevel` enum for
  efficient lock management.

---

## Architecture Layers

1. **SQL parser** -- verifies, optimizes and prepares statements
2. **VM planner** -- chooses the best strategy and creates execution plan
4. **VM query result** -- pull-based row iterator
5. **VM operators** -- composable Vulcano-model executor
6. **B-link tree cursor** -- allows using on-disk b-trees
7. **Pager & WAL** -- manages concurrency and safe data access
8. **Cache & BufferPool** -- manages in-memory pages  

### Storage Layer

Pages are fixed-size and managed by a `Pager` with an in-memory buffer pool. The B-link tree operates over two page types:

- **`TableLeaf` / `TableInternal`** — Row data indexed by integer primary key.
- **`IndexLeaf` / `IndexInternal`** — Secondary index entries with high-key fencing for concurrent traversal safety.

### WAL & Transactions

All mutations are written to a WAL before being applied to the buffer
pool. Transactions are explicit objects carrying a `LockLevel` that
escalates lazily from `Read` → `Write` as operations demand. `COMMIT`
flushes and syncs the WAL; `ROLLBACK` replays inverse log records.

### Query Execution

The Volcano model composes iterators top-down. Each operator implements
`next()`. The planner selects between full table scans and index scans
based on available indexes and query predicates.

---

## SQL Reference

CarcinusDB implements a subset of standard SQL. The sections below document every supported statement, clause, and modifier.

---

### `CREATE TABLE`

Creates a new table with the specified columns and types.

```sql
CREATE TABLE users (
    id      INT PRIMARY KEY,
    name    TEXT,
    email   TEXT,
    age     INT
);
```

**Supported column types:** `INT` (varint), `TEXT`, `BLOB`, `BOOL`

---

### `INSERT INTO`

Inserts one or more rows into a table.

**Single row:**
```sql
INSERT INTO users (id, name, email, age)
VALUES (1, 'Alice', 'alice@example.com', 30);
```

**Multiple rows (multi-value insert):**
```sql
INSERT INTO users (id, name, email, age)
VALUES
    (1, 'Alice', 'alice@example.com', 30),
    (2, 'Bob',   'bob@example.com',   25),
    (3, 'Carol', 'carol@example.com', 28);
```

All rows are inserted atomically — if any row fails validation, no rows are written.

---

### `SELECT`

Retrieves rows from a table.

**All columns:**
```sql
SELECT * FROM users;
```

**Specific columns:**
```sql
SELECT id, name FROM users;
```

**With a filter:**
```sql
SELECT id, name FROM users WHERE age > 25;
```

---

### `WHERE`

Filters rows by a condition. Supported in `SELECT`, `UPDATE`, and `DELETE`.

**Comparison operators:** `=`, `!=`, `<`, `>`, `<=`, `>=`

**Logical operators:** `AND`, `OR`, `NOT`

```sql
SELECT * FROM users WHERE age >= 18 AND email != '';

UPDATE users SET age = 31 WHERE name = 'Alice';

DELETE FROM users WHERE age < 18 OR name = 'unknown';
```

String values must be quoted with single quotes. NULL comparisons use `IS NULL` / `IS NOT NULL`.

```sql
SELECT * FROM users WHERE email IS NOT NULL;
```

---

### `UPDATE`

Modifies existing rows. Requires a `WHERE` clause to target specific rows; omitting it updates every row in the table.

```sql
UPDATE users
SET age = 31, email = 'alice@new.com'
WHERE id = 1;
```

Multiple columns can be updated in a single statement by comma-separating the `SET` assignments.

---

### `DELETE`

Removes rows from a table. Like `UPDATE`, a missing `WHERE` clause deletes all rows.

```sql
DELETE FROM users WHERE id = 3;
```

---

### `RETURNING`

Returns the affected rows after a write operation. Supported with `INSERT`, `UPDATE`, and `DELETE`. Accepts either `*` or a specific column list.

```sql
-- Get the full inserted row back
INSERT INTO users (id, name, email, age)
VALUES (4, 'Dave', 'dave@example.com', 22)
RETURNING *;

-- Calculate something after operation
UPDATE users SET age = 32 WHERE name = 'Alice'
RETURNING id, age + 10;

-- Confirm which rows were deleted
DELETE FROM users WHERE age < 20
RETURNING id, name;
```

`RETURNING` is evaluated after the write completes and reflects the final state of the row.

---

### `ORDER BY` *(planned)*

> **Not yet implemented.** `ORDER BY` is on the roadmap; queries that include it will return an error.

The planned syntax follows the SQL standard:

```sql
-- Not yet supported
SELECT * FROM users ORDER BY age DESC;
SELECT id, name FROM users ORDER BY name ASC, id DESC;
```

Disk-backed external merge sort is going to be implemented internally and will power this feature once the query planner layer is wired up.
