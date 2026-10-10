---
name: snowflake-postgres-table-design
description: Designs and reviews tables for Snowflake Postgres (Snowflake's managed PostgreSQL 16, 17, 18) and the Snowflake analytics tables they feed through data mirroring, pg_lake Iceberg tables, and dbt-core 1.10 to 1.12. Use when writing or reviewing Postgres DDL on this stack (keys, column types, constraints, indexes, partitions, JSONB), when a table must be mirrored into Snowflake, when picking Snowflake column types or clustering keys, or when writing dbt-snowflake or dbt-postgres model configs (incremental strategies, dynamic tables, Iceberg, UDFs). Trigger on Snowflake Postgres, CREATE_MIRROR, pg_lake, PgBouncer, snowflake_cdc, dynamic tables, or dbt model configs, even when the user only says "add a table", "write the migration", or "model this in dbt".
---

# Snowflake Postgres Table Design

Rules and decision points for tables on Snowflake Postgres, the path those tables take into
Snowflake, and the dbt-core 1.1x configs that model them. Detail lives in `references/`:

- `references/snowflake-postgres.md`: instance tiers, versions, extension catalog, PgBouncer, server settings, logical replication.
- `references/snowflake-bridge.md`: data mirroring requirements, pg_lake, and Snowflake-side table design (types, clustering, transient tables).
- `references/dbt-core-1-1x.md`: dbt 1.10, 1.11, 1.12 changes, dbt-snowflake and dbt-postgres configs, modeling mirrored tables.

## Which side does the table belong on

- **Snowflake Postgres** for high-concurrency reads and writes, row-level transactions, enforced foreign keys, or an existing Postgres app. It is real Postgres: `psql`, drivers, and dbt-postgres work unchanged.
- **Snowflake** for analytics, history, wide scans, and dbt marts. Data gets there by a mirror (continuous, whole tables, Postgres to Snowflake only) or pg_lake (selective, Iceberg or Parquet, both directions).
- **Snowflake hybrid tables** only when the app needs enforced PK/UNIQUE and point lookups inside Snowflake itself and nothing Postgres-specific. They cannot take clustering keys.

Design the Postgres table for its OLTP job first, then run it through the mirror checklist.

## Core rules

- Every reference table gets a **PRIMARY KEY**, `BIGINT GENERATED ALWAYS AS IDENTITY`. Use `UUID` only for opaque or client-generated IDs: `uuidv7()` is built in on PG 18; on 16 and 17 run `CREATE EXTENSION pg_uuidv7` and call `uuid_generate_v7()`.
- **Normalize to 3NF** in Postgres. Denormalize in Snowflake with dbt, not in the OLTP schema.
- **NOT NULL** wherever the value is semantically required, with `DEFAULT` for common values.
- Carry `created_at` and `updated_at` as `TIMESTAMPTZ NOT NULL DEFAULT now()` on every mirrored table, with `updated_at` maintained by a `moddatetime` trigger. dbt incremental models filter on it.
- Index what you query: PK and UNIQUE are automatic, **FK columns are not**, add them. Then frequent filters, sorts, and join keys. Every index costs writes and WAL.
- Prefer `TIMESTAMPTZ`, `NUMERIC(p,s)` for money, `TEXT`, `BIGINT`, `DOUBLE PRECISION`.

## Mirror-safe table checklist

A table that will be mirrored into Snowflake must:

- Have a primary key. Without one the CDC extension sets `REPLICA IDENTITY NOTHING`, the mirror is insert-only, and Postgres rejects UPDATE and DELETE on the table.
- Be a logged, ordinary or partitioned table. `UNLOGGED`, views, materialized views, foreign tables, and extension-owned tables are not replicated.
- Use no virtual generated columns (`STORED` is fine) and no ANSI-reserved words as column names.
- Keep `uuid` as a standalone column, never inside an array or composite.
- Have a `schema.table` name that is unique case-insensitively and does not end in `$changes` or `$live`.
- Treat `jsonb` as opaque on the Snowflake side. Promote fields that analytics filters on to real columns.
- Live on a STANDARD or HIGHMEM instance. Burstable tiers cannot mirror or run pg_lake.

Budget one replication slot per mirror (default limit 10) and keep WAL under `max_slot_wal_keep_size` (one tenth of disk), or the slot is dropped and every table re-snapshots.

## Snowflake Postgres gotchas

- **PgBouncer** runs in transaction mode on port 5431. Session `SET`, SQL-level `PREPARE`/`EXECUTE` across transactions, advisory locks, `LISTEN`, and temp tables do not carry between transactions there. Run migrations and dbt-postgres on port 5432.
- **No TimescaleDB.** Use declarative partitioning with `pg_partman` for partition creation and retention, `pg_cron` for scheduled maintenance, and `pg_ivm` for incrementally maintained materialized views.
- **Settings are instance parameters** managed from Snowflake, not `ALTER SYSTEM`. `max_connections` is 500 and needs a restart. `wal_level` is already `logical`.
- **Identifiers** fold to lowercase unquoted. Use `snake_case` everywhere.
- **UNIQUE allows multiple NULLs** unless `NULLS NOT DISTINCT` (PG 15+).
- **No silent coercion**: precision and length overflows error. **Identity gaps are normal.**
- **Heap storage and MVCC**: no clustered PK, updates leave dead tuples. Keep hot update columns narrow and unindexed so HOT updates apply.

## Data types

- **IDs**: `BIGINT GENERATED ALWAYS AS IDENTITY`; `UUID` as above.
- **Numbers**: `BIGINT`, `DOUBLE PRECISION`, `NUMERIC(p,s)` for exact decimals.
- **Strings**: `TEXT` plus `CHECK (length(col) <= n)` when a limit matters. Case-insensitive lookups use an index on `lower(col)`; `citext` only for case-insensitive constraints.
- **Time**: `TIMESTAMPTZ`, `DATE`, `INTERVAL`. `now()` is transaction start, `clock_timestamp()` is wall clock.
- **Booleans**: `BOOLEAN NOT NULL`. **Enums**: `TEXT` + `CHECK` or a lookup table for business values; `CREATE TYPE ... AS ENUM` only for small fixed sets.
- **JSONB** over JSON, GIN-indexed, for optional attributes only. Extract hot scalars with `GENERATED ALWAYS AS (...) STORED`.
- Avoid `timestamp` without time zone, `char(n)`, `varchar(n)`, `money`, `timetz`, `serial`.

## Constraints and indexes

- **FK**: always state `ON DELETE` and `ON UPDATE`, index the referencing column, `DEFERRABLE INITIALLY DEFERRED` for cycles.
- **CHECK** is row-local and NULL passes, so pair it with `NOT NULL`. **EXCLUDE USING gist** prevents overlaps on ranges.
- **B-tree** default. **Composite**: leftmost prefix, most selective first. **Covering**: `INCLUDE (...)`. **Partial**: `WHERE status = 'active'`. **Expression**: the query must repeat the expression.
- **GIN** for JSONB, arrays, `pg_trgm`, full text. **GiST** for ranges and PostGIS. **BRIN** for append-only time series.

## Partitioning

- Partition above roughly 100M rows when queries filter on the key or retention follows it. `RANGE` for time, `LIST` for discrete values, `HASH` for spread.
- Let `pg_partman` create and drop partitions on a `pg_cron` schedule. Never use table inheritance.
- Include the partition key in the PK and every UNIQUE constraint. Partitioned tables are eligible for mirroring.

## Snowflake and dbt quick rules

- Snowflake: `NUMBER`, `VARCHAR` without a length, `TIMESTAMP_TZ` or NTZ in UTC, `VARIANT` for semi-structured. Only `NOT NULL` is enforced on standard tables. Cluster only multi-TB tables, 3 to 4 columns, low to high cardinality.
- dbt-core 1.1x: `incremental` + `merge` keyed on the Postgres PK and filtered on `updated_at`; `delete+insert` when the key is not unique; `dynamic_table` with `target_lag` for fresh marts; `table_format='iceberg'` for open tables. Custom properties go under `config.meta`; the 1.10 deprecations are errors in dbt v2.

## Example

```sql
CREATE EXTENSION IF NOT EXISTS moddatetime;

CREATE TABLE orders (
  order_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id    BIGINT NOT NULL REFERENCES users (user_id) ON DELETE RESTRICT,
  status     TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paid', 'canceled')),
  total      NUMERIC(12,2) NOT NULL CHECK (total >= 0),
  attrs      JSONB NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(attrs) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON orders (user_id);
CREATE INDEX ON orders (created_at);
CREATE TRIGGER orders_updated_at BEFORE UPDATE ON orders
  FOR EACH ROW EXECUTE FUNCTION moddatetime (updated_at);
```

The matching `CREATE_MIRROR` call and the dbt models that consume it are in the references.
