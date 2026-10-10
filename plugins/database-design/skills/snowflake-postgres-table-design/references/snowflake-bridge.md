# Snowflake bridge: mirroring, pg_lake, and the analytics side

How a Snowflake Postgres table reaches Snowflake, what that demands of the table, and how to
design the Snowflake table that receives it. Platform facts for the Postgres side are in
`snowflake-postgres.md`; dbt configs are in `dbt-core-1-1x.md`.

## Contents

1. [Choosing the path](#choosing-the-path)
2. [Data mirroring](#data-mirroring)
3. [pg_lake](#pg_lake)
4. [Snowflake table design](#snowflake-table-design)
5. [Hybrid tables](#hybrid-tables)

## Choosing the path

| Need | Use |
|---|---|
| A live copy of whole tables or schemas in Snowflake, with history of changes | Data mirroring |
| Selected datasets, file exchange through stages, or Snowflake writing back to Postgres | pg_lake |
| Postgres querying Parquet or Iceberg that Snowflake produced | pg_lake |
| Enforced PK/UNIQUE and point lookups inside Snowflake without a Postgres app | Hybrid tables |

Mirroring and pg_lake coexist on one instance. Both need a STANDARD or HIGHMEM tier.

## Data mirroring

### How it works

- The `snowflake_cdc` extension creates a publication and a logical replication slot per mirror. A background CDC worker writes per-table `$changes` Iceberg tables and a per-mirror metalog through pg_lake.
- In Snowflake, a serverless task `APPLY_MIRROR_<mirror_name>` applies changes on the `refresh_interval` (default 10 minutes, maximum 1 day) into the target tables. Each apply is one transaction using delete-then-append, so a failed run rolls back and resumes from the same position.
- Per target table Snowflake exposes the table itself, a `<table>$changes` table (7-day feed of inserts, updates, deletes), and a `<table>$live` view that unions pending changes for roughly 30-second lag.
- DDL on the source (add, drop, rename column, rename table, truncate) applies immediately and resets that table's `$changes` feed.
- Rows from one source transaction become visible together, so cross-table invariants hold on the target.
- Mirrors are one-directional, Postgres to Snowflake, and cannot cross accounts, regions, or clouds.

### Source table requirements

- Primary key. Without one the CDC extension sets `REPLICA IDENTITY NOTHING`: the mirror is insert-only and the source rejects `UPDATE` and `DELETE`.
- Ordinary or partitioned, logged table. Not `UNLOGGED`, not a view, materialized view, foreign table, sequence, or temp table. Not owned by an extension.
- No virtual generated columns. `STORED` generated columns are fine.
- No ANSI-reserved words as column names.
- `uuid` only as a standalone column, not inside arrays or composites.
- `schema.table` unique case-insensitively; names must not end in `$changes` or `$live`.
- `jsonb` arrives as VARIANT, VARCHAR, or BINARY depending on the mirror type mapping. Check the type mapping reference before relying on any non-scalar type, and promote filtered fields to real columns in Postgres.

### Capacity

- One replication slot per mirror; `max_replication_slots` defaults to 10.
- `snowflake_cdc.max_publications` defaults to twice the online CPU count.
- `max_slot_wal_keep_size` defaults to one tenth of disk. Past it the slot is invalidated and all tables re-snapshot. Size disk for the largest bulk update you expect to run on mirrored tables.
- Sustained write throughput wants `STANDARD_L` or larger and more than 250 GB disk.
- Cost: serverless task compute (`SERVERLESS_TASK_HISTORY` filtered on `APPLY_MIRROR_%`) plus storage on both sides. Longer `refresh_interval` costs less; `$live` covers the freshness gap for the queries that need it.

### Creating a mirror

```sql
-- Snowflake side, once
GRANT APPLICATION ROLE snowflake.postgres_mirror_admin TO ROLE data_platform_admin;
GRANT USAGE ON POSTGRES INSTANCE "orders_pg" TO APPLICATION snowflake;

CALL SNOWFLAKE.POSTGRES.CREATE_MIRROR(
    mirror_name       => 'orders_mirror',
    postgres_instance => 'orders_pg',
    postgres_database => 'postgres',
    target_database   => 'PG_ORDERS',          -- must not already exist, name cannot be reused
    postgres_tables   => ['public.users', 'public.orders'],
    postgres_schemas  => NULL,                 -- or ['public'] to include future tables
    refresh_interval  => '1 minute'
);
```

Add `target_table_type => 'ICEBERG'` (and `external_volume => '<volume>'` for customer-owned
storage) to land the target as Iceberg. Table-level mirrors only follow the tables listed;
schema-level mirrors pick up new tables automatically. `ALTER_MIRROR`, `DESCRIBE_MIRROR`,
`LIST_MIRRORS`, and `DROP_MIRROR` manage it afterwards.

## pg_lake

Snowflake's open-source Postgres extension for Iceberg and Parquet. Preview and moving fast, so
confirm current syntax against the docs before shipping.

```sql
CREATE EXTENSION pg_lake CASCADE;

CREATE TABLE events_archive (
  event_id   BIGINT,
  device_id  BIGINT,
  recorded_at TIMESTAMPTZ,
  payload    JSONB
) USING iceberg;
```

- Tables created `USING iceberg` keep their data files in object storage and the metadata pointer in Postgres. Snowflake reads them through a catalog integration for Snowflake Postgres, so no copy is needed.
- Three patterns: shared Iceberg (as above), stages (file exchange through a Snowflake stage), and customer-managed cloud storage.
- Use it for archive tiers (move cold partitions out of the heap table), for selective syncs where mirroring the whole table is wasteful, and for reading Snowflake-produced datasets back into Postgres.
- Iceberg tables are not OLTP tables. Keep the hot, indexed, constraint-bearing data in ordinary heap tables.

## Snowflake table design

Rules for the tables dbt builds on top of mirrored or pg_lake data.

### Types

| Postgres habit | Snowflake choice | Why |
|---|---|---|
| `BIGINT`, `INTEGER` | `NUMBER(38,0)` (what `INT`, `BIGINT` resolve to) | Integer synonyms cannot carry precision. |
| `NUMERIC(12,2)` | `NUMBER(12,2)` | Same semantics. `DECIMAL`, `NUMERIC` are synonyms. |
| `DOUBLE PRECISION` | `FLOAT` | All float spellings are one 64-bit type. `DECFLOAT` for exact base-10 with a dynamic exponent. |
| `TEXT` | `VARCHAR` with no length | Default is 16 MiB, max 128 MiB. A length is a constraint, not a storage saving. |
| `TIMESTAMPTZ` | `TIMESTAMP_TZ`, or `TIMESTAMP_NTZ` stored in UTC by convention | Bare `TIMESTAMP` is `TIMESTAMP_NTZ` by default. `TIMESTAMP_LTZ` depends on session time zone. Pick one project-wide and set it in dbt. |
| `BOOLEAN` | `BOOLEAN` | |
| `JSONB` | `VARIANT` | Query with `col:path::type`. Flatten arrays with `LATERAL FLATTEN`. |
| Typed JSON | Structured `OBJECT(...)`, `ARRAY(...)`, `MAP(...)` | Enforced shape, needed for Iceberg. |
| `UUID` | `VARCHAR(36)` or `BINARY(16)` | Snowflake lists a `UUID` type; check adapter support before using it in dbt contracts. |
| PostGIS `GEOGRAPHY` | `GEOGRAPHY` | `GEOMETRY` for planar. |
| `vector` | `VECTOR(FLOAT, n)` | |

### Constraints

- `NOT NULL` is enforced on standard tables. `PRIMARY KEY`, `UNIQUE`, and `FOREIGN KEY` are metadata only. Declare them for lineage and tools, and enforce with dbt tests (`unique`, `not_null`, `relationships`).
- Hybrid tables enforce PK, UNIQUE, and FK.

### Clustering

- Micro-partitions prune on their own for data loaded in time order. Add a clustering key only for multi-terabyte tables with selective filters on the same few columns, after measuring with `SYSTEM$CLUSTERING_INFORMATION`.
- 3 or 4 columns at most, ordered low to high cardinality, filter and join columns first. Reduce cardinality with an expression that preserves order (`TO_DATE(ts)`), never a raw high-cardinality timestamp.
- Only the first few bytes of a VARCHAR count. Use a substring if prefixes repeat.
- Reclustering costs credits and storage (old micro-partitions stay for Time Travel and Fail-safe). Frequent DML on a clustered table multiplies that.
- Clustering keys cannot go on hybrid tables or directly on VARIANT columns (use a typed path expression).

### Storage and lifecycle

- dbt creates `transient` tables by default: no Fail-safe, one day Time Travel at most. Right for rebuildable models. Set `+transient: false` on tables that are the system of record.
- Iceberg tables (`table_format='iceberg'`) for open access from other engines. `iceberg_version: 3` adds VARIANT support and deletion vectors; it cannot be changed after creation.
- Dynamic tables refresh themselves on a `target_lag`. They replace scheduled incremental models when the SQL is deterministic and the lag budget is minutes to hours.

## Hybrid tables

- Row-oriented storage inside Snowflake with enforced `PRIMARY KEY`, `UNIQUE`, and `FOREIGN KEY`, plus secondary indexes. Built for point reads and writes at high concurrency.
- No clustering keys. Not a replacement for Postgres when the app needs extensions, PL/pgSQL, FKs to many tables, or an existing Postgres driver stack.
- Choose Snowflake Postgres for the application database and hybrid tables only for small, Snowflake-native operational state (configuration, lookup, control tables for pipelines).
