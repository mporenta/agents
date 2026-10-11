---
name: snowflake-loading-patterns
description: Lands raw data into Snowflake the Aptive way, with Parquet files in a landing stage, COPY INTO into RAW.<SOURCE>_TYPED tables, append loads stamped with snowflake_loaded_at, and deduplication left to dbt. Use when writing or reviewing a stage, a FILE FORMAT, COPY INTO, a VARIANT JSON landing, a MERGE upsert, a QUALIFY dedupe, a load check, a warehouse or query_tag choice, a masking policy on a landed column, or any ad hoc Snowflake SQL. Trigger on RAW schema, landing stage, COPY INTO, COPY_HISTORY, LATERAL FLATTEN, snow sql, ETL_WH, query_tag, or "load this into Snowflake".
---

# Snowflake Loading Patterns (Aptive)

Rules and decision points for getting raw data into Snowflake. Full DDL and DML examples, load checks, cost reading, the masking policy YAML, and the `snow` CLI block live in `references/details.md`.

## When to Use This Skill

- Land Parquet files into a `RAW.<SOURCE>_TYPED` table with a stage and COPY INTO
- Choose between append, MERGE, and a VARIANT JSON landing
- Dedupe landed rows with QUALIFY in dbt
- Check a load with COPY_HISTORY or pick a warehouse and query_tag
- Apply a masking policy to a landed column
- Write ad hoc Snowflake SQL with the `snow` CLI

## Landing convention

- Target table: `RAW.<SOURCE>_TYPED.<table>`. One schema per source.
- Landing stage: `@RAW.<SCHEMA>.<SOURCE>_LANDING_STAGE`, internal or S3 backed. A storage integration is referenced by name only. Never create, alter, or drop integrations or volumes. Ask the owner.
- Files are Parquet. Column names in the files match the table, loaded with `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`.
- Load strategy is **append**. Every row carries `snowflake_loaded_at TIMESTAMP_NTZ` in UTC.
- **Deduplication and merge logic live in dbt**, not in the loader. dbt incremental models filter on `snowflake_loaded_at` and dedupe with `QUALIFY row_number() over (partition by key order by snowflake_loaded_at desc) = 1`. A retried load that duplicates rows is therefore harmless.
- Older pipelines land JSON into a single `VARIANT` column and flatten in dbt with `LATERAL FLATTEN`. Do not start new pipelines that way.

## COPY INTO skeleton

```sql
COPY INTO RAW.EXAMPLE_TYPED.TICKETS
FROM @RAW.EXAMPLE_TYPED.EXAMPLE_LANDING_STAGE/tickets/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;
```

- `ON_ERROR = ABORT_STATEMENT` so a bad file fails the load instead of silently skipping rows.
- Snowflake remembers loaded files for 64 days and skips them on rerun. `FORCE = TRUE` reloads them and creates duplicates. Use it only with a reason and expect dbt to dedupe.
- Run `VALIDATION_MODE` first when the file layout is new. It checks files without loading.
- Stamp `snowflake_loaded_at` with a column default of `CURRENT_TIMESTAMP()` converted to UTC `TIMESTAMP_NTZ`, or with a `COPY INTO` transformation. See details for both.

## Choosing a load shape

| Situation | Use |
|---|---|
| Typed, stable columns from an API or export | Parquet COPY INTO into `RAW.<SOURCE>_TYPED` (default) |
| Source schema drifts often or is deeply nested | VARIANT JSON landing, flatten in dbt with `LATERAL FLATTEN` |
| Need in-place updates of existing rows at load time | `MERGE`, but only when dbt dedupe cannot cover it. Say why in the PR |
| Latest row per key from an append table | `QUALIFY row_number()` in the dbt model |
| Reload of already loaded files | `FORCE = TRUE`, with dbt dedupe in place |

## Warehouses and query tags

- `ETL_WH`: default for loads and clean models. `FIELD_OPS_DBT`: field ops dbt folders. `ENGINEER`: ad hoc work.
- Every dbt folder sets `+snowflake_warehouse` through `var("warehouse_override", ...)` and `+query_tag: '{"cost_center": "<NAME>"}'`. The `UNASSIGNED` default is a work queue, not a real cost center. Pick the real one.
- Loads set a `query_tag` with the job name so cost reports attribute them.

## Types and tables

- `NUMBER` ids, `VARCHAR` without length, `TIMESTAMP_NTZ` in UTC for load stamps, `VARIANT` for semi-structured.
- Only `NOT NULL` is enforced on standard tables. Do not rely on PK or UNIQUE.
- Cluster only multi-TB tables with measured pruning problems. Use transient tables where fail-safe storage is waste. History tables stay non-transient.

## Ad hoc SQL rules

- Command shape: `snow sql -c prod --role dbt --warehouse ENGINEER --database ANALYTICS --schema DBT_PORENTA -q "<sql>"`.
- **One statement per call.** Wait for it to finish. Never print credentials.
- **Create objects only in `ANALYTICS.DBT_PORENTA`.** Never alter or drop shared objects: integrations, volumes, grants, stages, policies, or anything in RAW that you did not create for the task.
- Inside the `data-dbt` repo use `./.claude/tools/snowflake-query.sh` instead of `snow sql`. Run anything that opens a Snowflake connection one at a time in the foreground.
- Read-only inspection (`SHOW`, `DESCRIBE`, `SELECT` on `INFORMATION_SCHEMA`) is fine. Anything that writes to RAW needs approval.

## Masking

PII masking is applied in dbt, not in the loader. Declare it on the column in schema.yml with `config.meta.masking_policy` and rely on the folder-level `apply_masking_policy('models')` post-hook. Land raw values in RAW with its existing grants, and mask at the first dbt layer that analysts can read. Genesys transcript masking uses the SECURE UDF `analytics.util.mask_transcript_pii` with the salt baked in at deploy, never a var on prod. Dev builds of masking models need `--vars '{pii_masking_salt: "<16+ chars>"}'`.

## Cost reading

- Local spill means an undersized warehouse. Remote spill is worse.
- `failsafe_gb` far above `active_gb` means the table is rewritten too often. Move to a transient table or append.
- A saturated warehouse overstates the savings of moving a query. Check saturation before claiming a win.

See `references/details.md` for stage and COPY DDL, VARIANT landing, MERGE, load checks, and the YAML and CLI examples.
