# Snowflake Postgres: platform facts

Platform-specific material for the `snowflake-postgres-table-design` skill. Everything here is
about the managed service itself. Mirroring and the Snowflake side are in `snowflake-bridge.md`,
dbt in `dbt-core-1-1x.md`.

## Contents

1. [Versions and regions](#versions-and-regions)
2. [Instance tiers](#instance-tiers)
3. [Connecting](#connecting)
4. [PgBouncer rules](#pgbouncer-rules)
5. [Server settings](#server-settings)
6. [Extension catalog](#extension-catalog)
7. [Replacing what is not available](#replacing-what-is-not-available)
8. [Logical replication](#logical-replication)
9. [Workload patterns](#workload-patterns)
10. [Safe schema evolution](#safe-schema-evolution)

## Versions and regions

- Major versions 16, 17, and 18. New instances take the latest minor (16.13, 17.9, 18.3 at the time of writing).
- Runs on AWS and Azure. Not on GCP.
- Each instance is a dedicated VM in a private network, reached through firewall rules or Private Link, with PgBouncer built in.
- Procedural language listed as supported: PL/pgSQL.
- Version-dependent design choices:
  - `uuidv7()` is a core function on 18. On 16 and 17 use `CREATE EXTENSION pg_uuidv7` and `uuid_generate_v7()`.
  - Virtual generated columns (`GENERATED ALWAYS AS (...) VIRTUAL`) exist on 18 but block mirroring. Use `STORED`.
  - `UNIQUE NULLS NOT DISTINCT` is available on all three versions.
  - Logical replication failover slots need 17 or later.

## Instance tiers

| Tier | Sizes | Notes |
|---|---|---|
| Burstable | `BURST_S` (2 cores, 2 GB), `BURST_M` (2 cores, 4 GB) | Max 100 GB storage. No HA. Cannot mirror or run pg_lake. CPU credits deplete under sustained load, then the instance throttles. Dev only. |
| General purpose | `STANDARD_M` (1 core, 4 GB) through `STANDARD_24XL` (96 cores, 384 GB) | HA on every size except `STANDARD_M`. Mirroring with sustained writes wants `STANDARD_L` or larger with more than 250 GB disk. |
| Memory optimized | `HIGHMEM_L` (2 cores, 16 GB) through `HIGHMEM_48XL` (192 cores, 1.5 TB) | 8 GB per core. For large working sets and heavy sort or hash work. |

`work_mem` defaults to `(TOTAL_MEMORY * 0.75) / (NUM_CORES * 8)` per operation, so the tier
sets how much sorting happens in memory. Size the tier before tuning queries.

## Connecting

- Host is the instance hostname (`<id>.snowflake.app`), port 5432 for a direct connection, 5431 for PgBouncer.
- SSL is required. Minimum is `sslmode=require`; certificate verification options are on the SSL certificates page. Set `PGSSLMODE=require` for tools without an SSL option.
- `snowflake_admin` is the default administrative role. Application and dbt users get their own roles.
- Snowflake token authentication exists for interactive sessions (`GENERATE_POSTGRES_ACCESS_TOKEN_FOR_USER`). Services use passwords.
- The connection string survives instance management operations unless access is reset for a role.

## PgBouncer rules

PgBouncer listens on 5431 and defaults to **transaction** pool mode. The `snowflake_pooler`
extension must be installed in each database that accepts pooled connections. Roles with
superuser or replication privileges cannot use the pooler.

What transaction mode means for schema and application design:

- A server connection is handed back after every transaction. Session state does not persist: `SET` (use `SET LOCAL`), `PREPARE`/`EXECUTE` split across transactions, advisory locks held across transactions, `LISTEN`/`NOTIFY`, and temp tables are unreliable on 5431.
- Protocol-level prepared statements (what most drivers use) work. `max_prepared_statements` defaults to 250.
- `pool_mode` can be set to `session` (full session semantics, fewer effective connections) or `statement` (no multi-statement transactions).
- `default_pool_size` is 497 against `max_connections` 500, which leaves almost nothing for direct connections. Keep migrations, dbt-postgres, and replication on 5432 and keep their count small.
- `DROP EXTENSION snowflake_pooler` disables pooled access to that database.

## Server settings

Settings are instance parameters changed from Snowflake (Snowsight or `ALTER POSTGRES INSTANCE`).
Do not plan on `ALTER SYSTEM`, filesystem access, or editing system catalogs.

| Setting | Default | Restart |
|---|---|---|
| `max_connections` | 500 | yes |
| `work_mem` | `(TOTAL_MEMORY * 0.75) / (NUM_CORES * 8)` | no |
| `max_wal_size` | `MIN(10 GB, STORAGE_GB * 0.1)` | no |
| `max_slot_wal_keep_size` | `STORAGE_GB * 0.1` | no |
| `max_replication_slots` | 10 | yes |
| `max_wal_senders` | 10 | yes |
| `max_logical_replication_workers` | 4 | yes |
| `max_worker_processes` | 100 | yes |
| `wal_level` | `logical` | yes |
| `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout` | Postgres default (off) | no |
| `autovacuum_*` scale factors, thresholds, cost settings | Postgres default | no |
| `autovacuum_freeze_max_age` | Postgres default | yes |
| `track_commit_timestamp` | Postgres default | yes |

Design implications:

- Set `lock_timeout` and `statement_timeout` per role for migrations so an `ALTER TABLE` waiting on a lock does not queue every other session behind it.
- Set `idle_in_transaction_session_timeout` for application roles. Idle transactions block vacuum and hold pooler connections.
- Lower `autovacuum_vacuum_scale_factor` on large, update-heavy tables with a per-table `ALTER TABLE ... SET (autovacuum_vacuum_scale_factor = 0.02)`, which needs no instance change.
- `max_slot_wal_keep_size` caps how far a lagging mirror or subscriber can fall behind before its slot is dropped. Large bulk updates on mirrored tables can hit it.

## Extension catalog

Enable with `CREATE EXTENSION <name>;` as `snowflake_admin`. Run
`SELECT * FROM pg_available_extensions` for versions. Only extensions in the catalog can be
installed. The ones that matter for table design:

| Area | Extensions | Design use |
|---|---|---|
| Keys | `pg_uuidv7`, `pgx_ulid`, `uuid-ossp` (legacy) | Time-ordered opaque IDs on PG 16 and 17. Prefer `pg_uuidv7`. |
| Timestamps | `moddatetime`, `insert_username` | `updated_at` maintenance through a `BEFORE UPDATE` trigger. |
| Partitioning | `pg_partman`, `pg_cron` | Automatic partition creation and retention on a schedule. |
| Incremental views | `pg_ivm`, `pg_incremental` | Incrementally maintained materialized views and incremental processing pipelines. |
| Text | `citext`, `pg_trgm`, `unaccent`, `fuzzystrmatch`, `dict_int`, `dict_xsyn` | Case-insensitive constraints, `LIKE '%x%'` acceleration, fuzzy search. |
| Types | `hstore` (legacy, use `jsonb`), `ltree`, `isn`, `semver`, `seg`, `cube`, `lo` | Hierarchies (`ltree`), product codes, versions. |
| Indexes | `bloom`, `btree_gin`, `btree_gist`, `hypopg` | Mixed-type GIN/GiST indexes; hypothetical indexes for `EXPLAIN` before building. |
| Geospatial | `postgis` (+ raster, topology, SFCGAL, tiger), `h3`, `h3_postgis`, `pgrouting`, `earthdistance`, `address_standardizer` | Location data. Prefer `GEOGRAPHY` columns. |
| Vectors | `vector` (pgvector) | Embeddings with HNSW or IVFFlat indexes. |
| Security | `pgcrypto`, `pgaudit`, `sslinfo` | Hashing, auditing. |
| Observability | `pg_stat_statements`, `auto_explain`, `pgstattuple`, `pg_buffercache`, `pg_freespacemap`, `pg_visibility`, `pageinspect`, `pg_walinspect`, `pgrowlocks`, `proctab` | Bloat and lock investigation before changing a table. |
| Maintenance | `pg_repack`, `pg_squeeze`, `pg_prewarm`, `amcheck`, `pg_surgery` | Online table and index rebuilds without `VACUUM FULL`. |
| Planner | `pg_hint_plan` | Last resort for a plan the optimizer gets wrong. |
| Replication and movement | `pglogical`, `postgres_fdw`, `pg_lake`, `snowflake_cdc`, `snowflake_pooler` | Logical replication, cross-database reads, Iceberg tables, mirroring, pooling. |
| Other | `pg_cron`, `http`, `tablefunc`, `intarray`, `intagg`, `hll`, `datasketches`, `age` (graph), `orafce`, `tcn`, `refint`, `autoinc`, `ddlx`, `tsm_system_rows`, `tsm_system_time`, `xml2` | Crosstabs, approximate counts (`hll`), graph queries. |

## Replacing what is not available

| Not in the catalog | Use instead |
|---|---|
| TimescaleDB hypertables | Declarative `PARTITION BY RANGE` plus `pg_partman` (`partman.create_parent`) with `pg_cron` running `partman.run_maintenance_proc()`. |
| TimescaleDB retention policies | `pg_partman` `retention` and `retention_keep_table` settings. |
| TimescaleDB continuous aggregates | `pg_ivm` (`create_immv`) for incrementally maintained views, or a dbt model in Snowflake once the table is mirrored. |
| TimescaleDB compression | Mirror the table and let Snowflake or Iceberg hold the history. Keep only the hot window in Postgres. |
| Arbitrary custom extensions, `plpython`, `plv8` | PL/pgSQL, or move the logic to a dbt UDF in Snowflake. |
| `ALTER SYSTEM`, `shared_preload_libraries` edits | Instance parameters from Snowflake. `session_preload_libraries` is settable. |

## Logical replication

- Defaults: `wal_level = logical`, `max_wal_senders = 10`, `max_replication_slots = 10`, `max_logical_replication_workers = 4`.
- `snowflake_admin` creates publications, subscriptions, and slots. Open the networking rules both ways.
- Every mirror consumes one slot. Count mirrors plus external subscribers against the limit of 10, or raise it (restart).
- Failover-safe subscriptions require the publisher to be a primary on PG 17 or later and `failover = true` on the subscription (or `failover := true` on the slot for non-Postgres consumers). Read replicas cannot publish.
- Disable subscriptions before a major version upgrade and re-enable afterwards.
- A primary without HA that is replaced loses its subscriptions.
- `REPLICA IDENTITY DEFAULT` (the primary key) is enough for mirroring and for most subscribers. `REPLICA IDENTITY FULL` doubles the WAL per update on wide tables; use it only when a subscriber needs old values for every column.

## Workload patterns

### Update-heavy tables

- Put frequently updated columns in their own narrow table so wide rows do not churn.
- `ALTER TABLE t SET (fillfactor = 90)` to leave room for HOT updates.
- Keep updated columns out of indexes so HOT updates apply.
- On a mirrored table every update is also WAL that the mirror must ship. Batch updates and avoid no-op updates (`WHERE col IS DISTINCT FROM new_value`).

### Insert-heavy workloads

- Fewest indexes that the queries need. BRIN on the time column for append-only data.
- `COPY` or multi-row `INSERT`. `UNLOGGED` only for staging that will never be mirrored.
- Partition by time with `pg_partman` so retention is a partition drop, not a `DELETE`.
- A natural key such as `(device_id, recorded_at)` is a fine PK for event tables. Mirroring still needs some PK for update and delete support.

### Upserts

- `INSERT ... ON CONFLICT (cols) DO UPDATE` needs a matching non-partial UNIQUE index.
- Update only columns that changed with `WHERE t.col IS DISTINCT FROM EXCLUDED.col`, or the `moddatetime` trigger bumps `updated_at` on every no-op upsert and every dbt incremental run reprocesses the row.
- `DO NOTHING` is cheaper when nothing needs to change.

### Case-insensitive text

- Expression index on `lower(col)` and query with `lower(col) = lower($1)`.
- `citext` when a UNIQUE or FK constraint itself must be case-insensitive.
- Non-deterministic collations for locale-aware comparison.

## Safe schema evolution

- DDL is transactional. Test with `BEGIN; ALTER TABLE ...; ROLLBACK;`.
- `CREATE INDEX CONCURRENTLY` does not block writes and cannot run inside a transaction.
- Adding a `NOT NULL` column with a volatile default (`now()`, `gen_random_uuid()`) rewrites the table. Non-volatile defaults are metadata-only.
- Drop dependent constraints before columns.
- `CREATE OR REPLACE FUNCTION` with a different signature creates an overload; drop the old one.
- Column add, drop, and rename, table rename, and truncate are applied to a mirror immediately, outside the refresh interval, and reset the `$changes` feed for that table.
- Run DDL on port 5432 with `lock_timeout` set, never through the pooler.
