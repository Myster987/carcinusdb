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
