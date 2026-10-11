---
name: dbt-snowflake-models
description: Writes and changes models in the Aptive data-dbt repo (project `aptive`, dbt-core 1.11.11, dbt-snowflake 1.11.6). Covers layer choice, naming, incremental merge models, required config, schema.yml rules, PestRoutes to Aspyn ids, and safe dbt runs. Use when adding a clean model, fact, dim, model_, report_ or history table, writing an incremental merge with snowflake_loaded_at, editing schema.yml or dbt_project.yml, choosing a layer, fixing a type mismatch on pr_injection_id, or running dbt against Snowflake, even when the user only says "add a model" or "build this in dbt".
---

# dbt Snowflake Models (data-dbt)

Rules for writing models in `data-dbt`. The repo's own guides (`ONBOARDING`, `models/models-guide.md`, `models/staging/staging-guide.md`, `models/aep_export/aep-export-guide.md`) override this skill. Open the one that matches the folder before editing. See `references/details.md` for full examples (incremental model, schema.yml, folder config, freshness, snapshots, selection, pre-PR checklist).

## When to Use This Skill

- Add a clean, fact, dimension, model_, or report_ model and pick its layer
- Write an incremental merge model filtered on `snowflake_loaded_at`
- Edit schema.yml or dbt_project.yml config
- Fix a type mismatch between PestRoutes and Aspyn ids such as `pr_injection_id`
- Run dbt against Snowflake without touching shared schemas

## Layers and names

| Folder | Purpose | Name |
|---|---|---|
| `raw` | S3 external tables | source-named |
| `history` | append-only CDC, SCD2 views | `history_<source>_<entity>`, `scd_<source>_<entity>` |
| `base`, `pre`, `ref`, `transform` | light cleaning, lookups, schema `dbt_base` | `base_`, `pre_`, `ref_` |
| `clean` | business cleaning | `clean_<source>_<entity>` |
| `intermediate` | pre-fact calculations | descriptive, no fixed prefix |
| `fact`, `dimension` | core facts and dimensions | `fact_<x>`, `dim_<x>` |
| `model` | aggregated business models | `model_<x>` |
| `report` | BI-facing tables | `report_<x>` |
| `staging` | share wrapping, PII-masked views | per staging guide |
| `aep_export` | Adobe XDM views | per AEP guide |
| `deprecated` | `+enabled: false` | unchanged |
| `semantic_models` | semantic models, metrics | per semantic guide |

Place a new model by the work it does. Cleaning one source entity goes in `clean`. Joining several cleaned entities into facts or dimensions goes in `fact` or `dimension`. Aggregations for a business question go in `model`. A table a BI tool reads directly goes in `report`. Columns in `aep_export` are a contract with audience syncs, so a schema change there needs the AEP guide first.

## Incremental merge skeleton

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['entity_id'],
    on_schema_change='sync_all_columns'
) }}

with changed as (
    select * from {{ ref('base_source_entity') }}
    {% if is_incremental() %}
    where snowflake_loaded_at > (
        select coalesce(max(snowflake_loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }}
    )
    {% endif %}
)
select * from changed
```

- The `snowflake_loaded_at` filter bounds each run to new rows.
- When a window function needs history (`row_number`, running totals), first collect the affected ids from the new rows, then scope the window to those ids. Windowing the whole table caused local spill on ETL_WH.
- `unique_key` must be unique in the batch or the merge fails.
- Set `transient=false` on history tables.

## Required config

- `tags`: schedule tag (`Daily`, `Hourly`, `Monthly`) or a source tag (`Aspyn`, `AspynHourly`). Tags decide which job runs the model.
- `snowflake_warehouse`: from the folder via `'{{ var("warehouse_override", "<WH>") }}'`. Never hard-code a warehouse in the model.
- `query_tag`: `'{"cost_center": "<NAME>"}'` on the folder. `UNASSIGNED` is a work queue, not a cost center.
- `copy_grants`: `+copy_grants: true` is set project-wide. Do not unset it.
- `meta.openmetadata.owner`: every top-level folder carries it. A new top-level folder needs it.
- Masked columns: `config.meta.masking_policy` in schema.yml. The folder `apply_masking_policy('models')` post-hook applies it.

## Testing and YAML

- Every model has a schema.yml entry with a description and column descriptions. Raw models set database, schema, and alias there. Clean models do not set a database.
- Put custom properties under `config.meta`, and `tags`, `freshness`, `group`, `access` under `config`. Resource-level custom keys are 1.10 deprecations and errors in dbt v2.
- In `dbt_project.yml` use `post-hook` (hyphen), not `post_hook`. The underscore form fires `CustomKeyInConfigDeprecation`.
- Add `unique` and `not_null` on the `unique_key`. A singular test that refs both a fast model (`Hourly`) and a slow one (`Daily`) gets pulled into the fast job and reports false failures. See the cross-cadence section of `models/models-guide.md`.
- Marketing data tests warn by default. Do not set `error` on one unless it is a consent gate.

## PestRoutes to Aspyn ids

- `pr_injection_id(field)` is `hash(field) + 100000000`. `id` is the NUMBER pr_injection id and `aspyn_id` is the UUID text.
- Aspyn to Aspyn joins use the UUID fields. Aspyn to PestRoutes joins use the NUMBER ids.
- On a type mismatch, check types with `typeof()` or `INFORMATION_SCHEMA.COLUMNS`, then cast explicitly (`id::text`). Never `try_cast` to hide an internal mismatch.

## Run safety

- Use the dev target only (`--target dev` or the default). Never `--target prod`. Never `--full-refresh`.
- Run anything that opens a Snowflake connection one at a time, in the foreground, and wait for it.
- `dbt parse --profiles-dir ci` is safe at any time and opens no warehouse work.
- Dev builds of masking models need `--vars '{pii_masking_salt: "<16+ chars>"}'`.
- Create objects only in `ANALYTICS.DBT_PORENTA`. Never drop or alter shared objects.
- Prod runs are a human job. The human runs them from a detached git worktree at the exact commit, with one explicit selection: `/home/datade/.venv/bin/dbt --profiles-dir ~/.dbt --target prod --select <one tag or model>`.
- To retire a model, use the `deprecating-models` skill. This skill does not cover it.
