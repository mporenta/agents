# dbt-core 1.10, 1.11, 1.12 on Snowflake and Snowflake Postgres

What changed in each 1.1x release, the dbt-snowflake configs that matter for table design, and
how to model mirrored tables. Verified against the dbt docs in October 2026. The dbt v2 line
(Rust engine) is separate; 1.12 is the bridge to it.

## Contents

1. [Version map](#version-map)
2. [Profiles](#profiles)
3. [1.10](#110)
4. [1.11](#111)
5. [1.12](#112)
6. [dbt-snowflake model configs](#dbt-snowflake-model-configs)
7. [Incremental strategies](#incremental-strategies)
8. [Dynamic tables](#dynamic-tables)
9. [Iceberg](#iceberg)
10. [UDFs](#udfs)
11. [Modeling mirrored tables](#modeling-mirrored-tables)
12. [dbt-postgres against Snowflake Postgres](#dbt-postgres-against-snowflake-postgres)

## Version map

| dbt-core | GA | Headline |
|---|---|---|
| 1.10 | mid 2025 | `--sample`, `catalogs.yml`, config-key deprecations that v2 enforces |
| 1.11 | December 2025 | UDFs as dbt resources, `DBT_ENGINE_*` env vars, JSON schema warnings on by default |
| 1.12 | July 2026 | opt-in Rust parser (`--use-v2-parser`), UDF overloads, `vars.yml`, `on_error`, `run-operation --sql` |

Adapter versions track core. `dbt-snowflake` must be 1.10.6 or later if any incremental model
has collated string columns with `on_schema_change='sync_all_columns'` (Snowflake's 2026 default
string size change breaks older adapters).

## Profiles

```yaml
# profiles.yml
analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"      # <org>-<account>
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_KEY_PATH') }}"
      role: TRANSFORMER
      warehouse: TRANSFORM_WH
      database: ANALYTICS
      schema: dbt_dev
      threads: 8
```

Key-pair auth for services. Password auth is being phased out for Snowflake programmatic access.

## 1.10

- `dbt run --sample` and `dbt build --sample`: time-sliced `ref`/`source` for cheap dev builds.
- `--use-fast-test-edges` on `dbt build` for large graphs.
- Top-level `anchors:` key for YAML anchors.
- `catalogs.yml` parsing for Iceberg write integrations; models opt in with `catalog_name`.
- Source freshness `loaded_at_query` (mutually exclusive with `loaded_at_field`).
- `config.meta_get()` and `config.meta_require()`.
- dbt-snowflake: `copy_grants` on tables and views; `platform_detection_timeout_seconds`.
- Flags (opt-in): `validate_macro_args`, `require_all_warnings_handled_by_warn_error`.

Deprecations that matter when writing YAML (warnings in 1.10, errors in v2):

| Old | New |
|---|---|
| Custom keys at resource level (`owner: data-team`) | `config: { meta: { owner: data-team } }` |
| `freshness`, `meta`, `tags`, `docs`, `group`, `access` at resource level | the same keys under `config:` |
| `warn_error_options: { include: ..., exclude: ... }` | `error:` and `warn:` |
| `--models`, `-m` | `--select`, `-s` |
| Duplicate YAML keys, orphaned Jinja blocks | remove |
| `--output` for `sources.json` | `--target-path` |

## 1.11

- **UDFs** are resources in `functions/`, built by `dbt build` before the models that use them, selectable with `--select "resource_type:function"`, referenced with `{{ function('name') }}`. `--defer` resolves them from state.
- Engine environment variables take the `DBT_ENGINE_` prefix (`DBT_ENGINE_STATE`, `DBT_ENGINE_PROJECT_DIR`).
- JSON schema validation warnings on by default for Snowflake, Databricks, BigQuery, Redshift projects (`CustomKeyInConfigDeprecation`, `MissingPlusPrefixDeprecation`, `SourceOverrideDeprecation`, and so on). Fix them rather than silencing them.
- Disabling a model disables its unit tests. `dbt ls --output-keys config.materialized`.
- Flags (opt-in): `require_unique_project_resource_names`, `require_ref_searches_node_package_before_root`.
- dbt-snowflake: dynamic tables gain `cluster_by`, `immutable_where`, `copy_grants`, `refresh_warehouse`; basic table materialization on Glue-catalog Iceberg through a catalog-linked database.

## 1.12

- `--use-v2-parser` (beta): the Rust parser from dbt v2, 5 to 10 times faster on large projects. Manifest may differ at the edges; drop the flag to fall back.
- UDFs: `overloads` for multiple signatures (SQL UDFs on Snowflake and Postgres, Python on Snowflake), JavaScript UDFs on Snowflake, `packages` for Python UDFs.
- `on_error: skip_children | continue` per model.
- `vars.yml` at project root, parsed before `dbt_project.yml`. A variable lives in one file, not both.
- `dbt run-operation --sql "..."` runs ad hoc SQL through `ref`, `source`, `var`, `target`.
- `latest_version_pointer` creates a pointer view for versioned models.
- `--select "selector:name"`, `dbt seed --empty`, `.env` auto-load, `dbt compile` writes snapshots.
- New `catalogs.yml` spec (`type: horizon | glue`, `config.snowflake: {...}`) alongside the old `write_integrations` spec.
- New Semantic Layer spec: `semantic_model` nested under the model, metrics replace measures, `type_params` deprecated.
- Flags now on by default: `require_all_warnings_handled_by_warn_error`, `require_batched_execution_for_custom_microbatch_strategy`, `require_nested_cumulative_type_params`, `skip_nodes_if_on_run_start_fails`, `state_modified_compare_more_unrendered_values`, `validate_macro_args`.
- dbt-snowflake: `iceberg_version`, dynamic table `scheduler`, `snowflake_initialization_warehouse`, `transient` dynamic tables (flag `snowflake_default_transient_dynamic_tables`), `target_lag: downstream`, `on_configuration_change` applies to dynamic tables, `snowflake.quote_args` for JS UDFs.
- Removed: experimental `dbt login` and the bundled `dbt-state` plugin.

## dbt-snowflake model configs

| Config | Versions | Use |
|---|---|---|
| `transient` | all | Default `true` for tables. `false` for system-of-record tables. |
| `cluster_by` | all (dynamic tables 1.11+) | List of columns or expressions. dbt orders the build by them and sets the key. |
| `automatic_clustering` | all | Only matters on accounts with legacy manual clustering. |
| `snowflake_warehouse` | all | Per model, folder, snapshot, or test. Route heavy models to a bigger warehouse. |
| `copy_grants` | 1.10+ (dynamic tables 1.11+) | Keep grants across `CREATE OR REPLACE`. Set at project level. |
| `secure` | all | Secure views only when needed; they skip some optimizations. |
| `query_tag` | all | Cost attribution. Reset after each materialization. |
| `row_access_policy`, `table_tag` | all | Attach existing policies and tags. |
| `tmp_relation_type` | all | `view` (default), `table`, `transient`. `delete+insert` with a `unique_key` needs a table. |
| `table_format`, `external_volume`, `iceberg_version`, `catalog_name` | 1.9+, `iceberg_version` 1.12+ | See Iceberg. |
| `on_schema_change` | all | `append_new_columns` is the safe default for mirrored sources whose columns grow. |

Project-level defaults that fit mirrored data:

```yaml
# dbt_project.yml
models:
  analytics:
    +copy_grants: true
    +on_schema_change: append_new_columns
    staging:
      +materialized: view
    marts:
      +materialized: table
      +transient: false
flags:
  require_all_warnings_handled_by_warn_error: true
  validate_macro_args: true
```

## Incremental strategies

| Strategy | When |
|---|---|
| `merge` (default) | `unique_key` is the Postgres PK and is unique in the batch. Fails with a nondeterministic-merge error otherwise. |
| `delete+insert` | Key is not unique, or batches replace whole key groups. Needs `tmp_relation_type: table`. |
| `append` | Immutable events, no key. |
| `insert_overwrite` | Replaces the whole table each run (no partition overwrite on Snowflake). `overwrite_columns` limits the column list. |
| `microbatch` | Time-partitioned event tables. `event_time`, `batch_size` (`hour`, `day`, `month`, `year`), `begin`, `lookback`. Sources need `event_time` too. Batches retry independently. |

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}

select *
from {{ ref('stg_pg__orders') }}
{% if is_incremental() %}
where updated_at > (select coalesce(max(updated_at), '1900-01-01'::timestamp_tz) from {{ this }})
{% endif %}
```

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='recorded_at',
    batch_size='day',
    begin='2025-01-01',
    lookback=1
) }}
select * from {{ ref('stg_pg__readings') }}
```

## Dynamic tables

```sql
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='TRANSFORM_WH',
    refresh_warehouse='DT_REFRESH_WH',                 -- 1.11+
    snowflake_initialization_warehouse='LARGE_WH',     -- 1.12+
    target_lag='15 minutes',                           -- or 'downstream' (1.12+)
    refresh_mode='INCREMENTAL',                        -- AUTO | FULL | INCREMENTAL
    initialize='ON_CREATE',                            -- 1.11+
    immutable_where="order_date < current_date() - 30",  -- 1.11+
    cluster_by=['order_date'],                         -- 1.11+
    copy_grants=true
) }}
select ... from {{ ref('stg_pg__orders') }}
```

- dbt's `scheduler` default is `DISABLE` (Snowflake's native default is `ENABLE`). Setting `target_lag` switches it to `ENABLE`; `scheduler='DISABLE'` with `target_lag` errors.
- Changing the SQL needs `--full-refresh`. Contracts are not supported.
- `refresh_mode='INCREMENTAL'` needs deterministic SQL (no `current_timestamp()` in the projection, supported joins and aggregations), otherwise Snowflake falls back to FULL.
- `transient=true` (1.12+) drops Fail-safe for a dynamic table.

## Iceberg

```sql
{{ config(
    materialized='table',
    table_format='iceberg',
    external_volume='analytics_volume',   -- or 'SNOWFLAKE_MANAGED'
    iceberg_version=3                     -- 1.12+, immutable after creation
) }}
```

```yaml
# catalogs.yml (1.x spec)
catalogs:
  - name: horizon
    active_write_integration: snowflake_write
    write_integrations:
      - name: snowflake_write
        external_volume: analytics_volume
        table_format: iceberg
        catalog_type: built_in
        adapter_properties:
          change_tracking: true
          iceberg_version: 3
```

- Supported materializations: `table`, `incremental`, `dynamic_table`. Intermediate relations for incremental builds are ordinary temp tables.
- Base location is `<volume>/_dbt/<schema>/<model>`; `base_location_root` changes the prefix. Snowflake rejects `BASE_LOCATION` on `SNOWFLAKE_MANAGED` volumes. Isolate environments with separate volumes, not paths.
- Converting an existing incremental model to Iceberg needs `--full-refresh`.
- Glue catalog-linked databases: table materialization in 1.11, incremental only in v2. Identifiers must be lowercase and quoted (`quoting: {schema: true, identifier: true}`).
- A mirror can land directly as Iceberg (`target_table_type => 'ICEBERG'`), which makes the raw layer open without a dbt copy.

## UDFs

```
functions/
  cents_to_dollars.sql
  cents_to_dollars.yml
```

```sql
-- functions/cents_to_dollars.sql
cents / 100
```

```yaml
# functions/cents_to_dollars.yml
functions:
  - name: cents_to_dollars
    description: Converts integer cents to a NUMBER(38,2) dollar amount.
    arguments:
      - name: cents
        data_type: number
    returns:
      data_type: number(38,2)
```

```sql
select {{ function('cents_to_dollars') }}(total_cents) as total from {{ ref('stg_pg__orders') }}
```

- SQL UDFs run on Snowflake and Postgres (dbt-postgres), Python and JavaScript on Snowflake.
- 1.12 `overloads` share one DAG node; retries rerun only the failed overload.
- Use a UDF when the same logic must be callable outside dbt. Use a macro when it is only template text.

## Modeling mirrored tables

Mirror target tables are the raw layer. Point sources at the target database and stage from them.

```yaml
# models/staging/pg/_pg__sources.yml
sources:
  - name: pg_orders
    database: PG_ORDERS
    schema: PUBLIC
    tables:
      - name: orders
        config:
          loaded_at_field: updated_at
          freshness:
            warn_after: { count: 30, period: minute }
            error_after: { count: 2, period: hour }
      - name: orders_live
        identifier: orders$live        # pending changes, ~30 s lag, for near-real-time models only
      - name: orders_changes
        identifier: orders$changes     # 7-day change feed
```

```sql
-- models/staging/pg/stg_pg__orders.sql
select
    order_id,
    user_id,
    status,
    total,
    attrs,
    created_at,
    updated_at
from {{ source('pg_orders', 'orders') }}
```

Rules:

- Staging is a view over the target table. Column names arrive as Postgres wrote them, lowercase; keep `quoting` off and let Snowflake fold them.
- Marts use `incremental` + `merge` on the Postgres PK filtered by `updated_at`, which is why the OLTP table carries a trigger-maintained `updated_at`.
- Deletes: the target table reflects them, a `merge` does not remove rows from the mart. Either rebuild the mart periodically (`--full-refresh` on a schedule), use `delete+insert` keyed on the touched key set, or drive a dbt snapshot from `orders$changes` while the row is still inside the 7-day window.
- Source DDL on the mirror resets `$changes`; a `snapshot` or an `append` model that reads `$changes` must tolerate a gap after schema changes.
- Keep JSON out of the mart contract: `attrs` is VARIANT, so extract fields in staging with `attrs:field::type` and test them.
- Source freshness on `updated_at` doubles as mirror lag monitoring. Alert well under `max_slot_wal_keep_size` turnaround.

## dbt-postgres against Snowflake Postgres

dbt-postgres treats the instance as plain Postgres. Use it for transformations that must stay
in the OLTP database (reporting tables an app reads, pre-aggregations behind an API).

```yaml
orders_pg:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('PG_HOST') }}"       # <id>.snowflake.app
      port: 5432                             # direct, not the 5431 pooler
      user: dbt
      password: "{{ env_var('PG_PASSWORD') }}"
      dbname: postgres
      schema: analytics
      sslmode: require
      threads: 4
```

- Port 5432. dbt holds session state (search path, temp relations, transactions) that transaction pooling breaks.
- Grant `dbt` its own role with `CREATE` on its schema only. Never `snowflake_admin`.
- Materialize as `view` or `table`; the `incremental` strategies on dbt-postgres are `append`, `delete+insert`, `merge` (PG 15+), `microbatch`.
- Tables dbt builds here are not mirrored unless they are in a mirrored schema and have a PK. Keep dbt output in its own schema so a schema-level mirror does not ship it.
- SQL UDFs from `functions/` work on dbt-postgres in 1.11+.
