# dbt Snowflake Models: details

Examples and reference for `data-dbt` (project `aptive`, dbt-core 1.11.11, dbt-snowflake 1.11.6, `require-dbt-version: [">=1.11.0", "<1.13.0"]`). Names and values below are illustrative. Copy the shape, then check a sibling model in the same folder. The repo guides override this file.

## Contents

1. Incremental model, full example
2. schema.yml with tests, masking, and owner
3. dbt_project.yml folder config
4. Source freshness
5. Snapshot or history table
6. Dynamic tables and microbatch
7. dbt parse and manifest.json for tooling
8. Selection cheat sheet
9. Pre-PR checklist

## 1. Incremental model, full example

`models/clean/field_service/clean_aspyn_service_orders.sql` (illustrative). The model keeps the latest version of each order and a per-customer running count scoped to the customers touched in this run.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['service_order_id'],
    on_schema_change='sync_all_columns',
    tags=['Daily', 'Aspyn']
) }}

with new_rows as (
    select *
    from {{ source('aspyn', 'service_orders') }}
    {% if is_incremental() %}
    where snowflake_loaded_at > (
        select coalesce(max(snowflake_loaded_at), '1900-01-01'::timestamp_ntz)
        from {{ this }}
    )
    {% endif %}
),

latest as (
    select *
    from new_rows
    qualify row_number() over (
        partition by id order by snowflake_loaded_at desc
    ) = 1
),

affected_customers as (
    select distinct customer_id from latest
),

customer_history as (
    -- window runs only over customers touched by this batch, not the whole table
    select
        o.id,
        row_number() over (
            partition by o.customer_id order by o.created_at
        ) as customer_order_seq
    from {{ source('aspyn', 'service_orders') }} o
    join affected_customers a on a.customer_id = o.customer_id
)

select
    {{ pr_injection_id('l.id') }}   as service_order_id,
    l.id                            as aspyn_id,
    {{ pr_injection_id('l.customer_id') }} as pr_injection_customer_id,
    l.status,
    h.customer_order_seq,
    l.created_at,
    l.snowflake_loaded_at
from latest l
join customer_history h on h.id = l.id
```

Notes:

- The filter compares `snowflake_loaded_at` with itself, so late-arriving source rows are still picked up on the next run.
- `{{ this }}` on the first run does not exist. The `is_incremental()` guard keeps the subquery out of the first build.
- Cast explicitly when types differ. Do not `try_cast` inside the model.
- `on_schema_change='sync_all_columns'` needs dbt-snowflake 1.10.6 or later when collated string columns are involved. The pinned 1.11.6 meets that.
- On history tables add `transient=false`.

## 2. schema.yml with tests, masking, and owner

```yaml
version: 2

models:
  - name: clean_aspyn_service_orders
    description: >-
      One row per Aspyn service order, latest version. Source of truth for
      order status in downstream facts.
    config:
      tags: [Daily]
      meta:
        openmetadata:
          owner: [data-engineering]
    columns:
      - name: service_order_id
        description: pr_injection_id of the Aspyn order id (NUMBER).
        data_tests:
          - unique
          - not_null
      - name: aspyn_id
        description: Original Aspyn order UUID (TEXT).
        data_tests:
          - not_null
      - name: customer_email
        description: Customer email (PII, masked).
        config:
          tags: [pii]
          meta:
            masking_policy: mask_email
      - name: snowflake_loaded_at
        description: UTC load timestamp stamped by the loader.
        data_tests:
          - not_null
```

- Masking needs two pieces: `config.meta.masking_policy` on the column and the `apply_masking_policy('models')` post-hook. Most folders carry the hook in `dbt_project.yml` (for example `base`). Where the folder has none, add `post_hook: "{{ apply_masking_policy('models') }}"` under the model `config:`. Existing masked models in `models/pre/` show the shape.
- `config.meta.openmetadata.owner` at model level is only needed when the folder does not already stamp it.
- Custom keys at resource level (`owner:`, `team:`) are 1.10 deprecations. Move them under `config.meta`.
- Check whether the project uses `tests:` or `data_tests:` in neighboring files and match it. Both are accepted in 1.11.
- Test severity is inherited as `error` when omitted. State it when it matters.

## 3. dbt_project.yml folder config

```yaml
models:
  aptive:
    +query_tag: '{"cost_center": "UNASSIGNED"}'   # project default, overridden per folder
    +copy_grants: true
    clean:
      +tags: "Monthly"
      +persist_docs:
        relation: true
        columns: true
      +meta:
        openmetadata:
          owner: [data-engineering]
      field_service:
        +snowflake_warehouse: '{{ var("warehouse_override", "FIELD_OPS_DBT") }}'
        +query_tag: '{"cost_center": "FIELD_OPS"}'
    base:
      +schema: '{{ var("local_base_schema", "dbt_base") }}'
      +post-hook: "{{ apply_masking_policy('models') }}"
```

- Keys in `dbt_project.yml` use a `+` prefix. A missing prefix fires `MissingPlusPrefixDeprecation`.
- `post-hook` is hyphenated here on purpose. `post_hook` fires `CustomKeyInConfigDeprecation`.
- A subfolder setting overrides its parent by specificity. That is how a folder replaces the `UNASSIGNED` tag.
- Warehouses seen in this project: `ETL_WH` (default for loads and clean models), `FIELD_OPS_DBT`, `ENGINEER` (ad hoc).
- Adding a model to an existing folder inherits all of this. Only a new top-level folder needs its own warehouse, query_tag, tags, and owner.

## 4. Source freshness

```yaml
sources:
  - name: ai_agent
    database: raw
    schema: ai_agent
    tables:
      - name: claude_code_events
        config:
          loaded_at_field: event_timestamp
          freshness:
            warn_after: {count: 48, period: hour}
            error_after: {count: 96, period: hour}
```

- Size the thresholds from the measured widest gap, then add margin. Do not guess.
- `loaded_at_field` and `loaded_at_query` are mutually exclusive (1.10+).
- Run `dbt source freshness` against dev only when you need to prove a block. Parse-only validation needs no connection.

## 5. Snapshot or history table

- History table (`history_<source>_<entity>`): append-only CDC, every record version kept, `transient=false`, with `scd_` views for point-in-time reads. Use it for sources that already deliver change records and for anything downstream reads by time.
- Snapshot (`snapshots/`): dbt compares the current source to the previous snapshot on each run and writes SCD2 rows. Use it only when the source keeps no history and only the current state is available. A snapshot sees changes between runs only, so intermediate states are lost.
- Snapshots never appear in the refactor MCP `xref_dead_models` output. Check them with `xref_consumers` and `dbt_lineage` before changing one.
- Never drop or rebuild either one from an agent. Both hold history that cannot be recomputed.

## 6. Dynamic tables and microbatch

Neither is a common pattern in `data-dbt`. Check the repo for an existing example before using one.

- Dynamic tables (`materialized='dynamic_table'`, `target_lag`, `refresh_mode`) are available on the pinned 1.11.6 adapter. `cluster_by`, `immutable_where`, `copy_grants`, and `refresh_warehouse` on a dynamic table need dbt-snowflake 1.11. `snowflake_initialization_warehouse`, `target_lag='downstream'`, and `transient` need dbt 1.12, which is not installed (the project allows `<1.13.0`, the pin is 1.11.11). Changing a dynamic table's SQL needs `--full-refresh`, which an agent never runs.
- Microbatch (`incremental_strategy='microbatch'`, dbt-core 1.9 or later): needs `event_time`, `batch_size`, `begin`, and `event_time` on upstream sources. Batches retry independently. It replaces the `snowflake_loaded_at` filter, so do not combine both.

## 7. dbt parse and manifest.json for tooling

- `dbt parse --profiles-dir ci` builds `target/manifest.json` without running SQL. It is safe at any time.
- Tooling that needs the model graph, tags, configs, or column metadata should read `target/manifest.json` instead of parsing SQL or YAML by hand. Hand-rolled parsers miss Jinja, folder configs, and inherited tags.
- Fields used most: `nodes` (keyed by `unique_id`, with `config`, `tags`, `depends_on.nodes`, `original_file_path`), `sources`, `child_map`, `parent_map`.
- `--use-v2-parser` is a dbt 1.12 beta flag. It is not available on 1.11.11.
- Pin any helper script to the same dbt-core 1.11.11 and dbt-snowflake 1.11.6.

## 8. Selection cheat sheet

| Selector | Meaning |
|---|---|
| `--select my_model` | one model |
| `--select +my_model` | the model and all upstream |
| `--select my_model+` | the model and all downstream |
| `--select tag:Daily` | every model tagged `Daily` |
| `--select clean_aspyn_service_orders,tag:Aspyn` | intersection (comma, no space) |
| `--select path:models/clean/field_service` | one folder |
| `--select state:modified+ --state <dir>` | changed models and downstream. Needs a prior `manifest.json` in `<dir>` |
| `--exclude tag:Hourly` | remove a set |
| `--select resource_type:function` | UDFs (1.11+) |

- Use `-s` or `--select`. `--models` and `-m` are deprecated.
- On dev, start with one model, then widen. A broad selection runs real warehouse work.
- Prod selections are one tag or one model, chosen and run by a human.

## 9. Pre-PR checklist

1. The model sits in the right layer with the right prefix.
2. schema.yml has model and column descriptions, and `unique` plus `not_null` on the key.
3. Incremental models filter on `snowflake_loaded_at` and scope windows to affected ids.
4. Tags, warehouse, query_tag, and owner come from the folder or are set for a new folder.
5. PII columns carry `config.meta.masking_policy`.
6. No `try_cast` on internal ids, and no custom keys outside `config.meta`.
7. `dbt parse --profiles-dir ci` passes with no new deprecation warnings.
8. A dev build of the one changed model succeeded in `ANALYTICS.DBT_PORENTA`, one connection at a time.
9. When replacing a model, parity was proven (see the `dbt-refactor-workflow` skill).
10. The matching repo guide was read for the touched folder.
