# Snowflake loading patterns: details

Examples and checks for `SKILL.md`. Object names are placeholders. Run examples in `ANALYTICS.DBT_PORENTA` or a scratch copy, never against shared RAW objects without approval. Confirm syntax against the Snowflake documentation for your account's release when a clause here looks unfamiliar.

## Contents

1. [Stage and file format](#stage-and-file-format)
2. [Target table](#target-table)
3. [COPY INTO from Parquet](#copy-into-from-parquet)
4. [VARIANT landing and LATERAL FLATTEN](#variant-landing-and-lateral-flatten)
5. [MERGE upsert](#merge-upsert)
6. [QUALIFY dedupe](#qualify-dedupe)
7. [Loading checks](#loading-checks)
8. [Cost reading](#cost-reading)
9. [Masking policy YAML](#masking-policy-yaml)
10. [snow CLI usage](#snow-cli-usage)

## Stage and file format

The stage and the storage integration already exist for production sources. Do not create or alter them. The statements below show the shape so you can review an existing definition or build a copy in your dev schema.

```sql
CREATE FILE FORMAT IF NOT EXISTS RAW.EXAMPLE_TYPED.PARQUET_FF
  TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS RAW.EXAMPLE_TYPED.EXAMPLE_LANDING_STAGE
  URL = 's3://<bucket>/<prefix>/'
  STORAGE_INTEGRATION = <existing_integration_name>
  FILE_FORMAT = RAW.EXAMPLE_TYPED.PARQUET_FF;
```

- `STORAGE_INTEGRATION` is a reference to an integration someone else owns. Never write credentials in the stage URL or options.
- Inspect with `SHOW STAGES IN SCHEMA RAW.EXAMPLE_TYPED` and `LIST @RAW.EXAMPLE_TYPED.EXAMPLE_LANDING_STAGE/tickets/`.

## Target table

```sql
CREATE TABLE IF NOT EXISTS RAW.EXAMPLE_TYPED.TICKETS (
  id                  NUMBER NOT NULL,
  status              VARCHAR,
  created_at          TIMESTAMP_NTZ,
  payload             VARIANT,
  snowflake_loaded_at TIMESTAMP_NTZ DEFAULT SYSDATE()
);
```

- `SYSDATE()` returns the current time in UTC as `TIMESTAMP_NTZ`, so the stamp is UTC without a conversion. A column that is absent from the Parquet files takes its default during `COPY INTO`.
- Parquet column names must match table column names. Types are cast on load, so a type that cannot cast fails the file under `ABORT_STATEMENT`.

## COPY INTO from Parquet

```sql
COPY INTO RAW.EXAMPLE_TYPED.TICKETS
FROM @RAW.EXAMPLE_TYPED.EXAMPLE_LANDING_STAGE/tickets/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;
```

Options and their meaning:

| Option | Aptive default | Effect |
|---|---|---|
| `MATCH_BY_COLUMN_NAME` | `CASE_INSENSITIVE` | Maps Parquet columns to table columns by name. Unmatched file columns are dropped, unmatched table columns get NULL or their default |
| `ON_ERROR` | `ABORT_STATEMENT` | Fail the whole statement on the first bad row or file. `CONTINUE` and `SKIP_FILE` hide data loss and need a stated reason |
| `FORCE` | `FALSE` | Default skips files already loaded in the last 64 days. `TRUE` reloads them and duplicates rows. dbt dedupe absorbs this |
| `PURGE` | not set | Do not delete staged files. The S3 prefix is the source of record for replays |

Dry check before loading a new layout. `VALIDATION_MODE` checks the files and returns errors or rows without loading:

```sql
COPY INTO RAW.EXAMPLE_TYPED.TICKETS
FROM @RAW.EXAMPLE_TYPED.EXAMPLE_LANDING_STAGE/tickets/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
VALIDATION_MODE = RETURN_ERRORS;
```

`RETURN_ERRORS` returns every error in the files. `RETURN_<n>_ROWS` returns the first n rows as they would load. `VALIDATION_MODE` cannot be combined with a transformation `SELECT`.

If a column must be derived at load time, use a transformation instead of `MATCH_BY_COLUMN_NAME`. The two cannot be combined:

```sql
COPY INTO RAW.EXAMPLE_TYPED.TICKETS (id, status, snowflake_loaded_at)
FROM (
  SELECT $1:id::NUMBER, $1:status::VARCHAR, SYSDATE()
  FROM @RAW.EXAMPLE_TYPED.EXAMPLE_LANDING_STAGE/tickets/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = ABORT_STATEMENT;
```

## VARIANT landing and LATERAL FLATTEN

Older pipelines land raw JSON in one `VARIANT` column. Keep it only where it already exists.

```sql
CREATE TABLE IF NOT EXISTS RAW.EXAMPLE_JSON.EVENTS (
  payload             VARIANT,
  snowflake_loaded_at TIMESTAMP_NTZ DEFAULT SYSDATE()
);

COPY INTO RAW.EXAMPLE_JSON.EVENTS (payload)
FROM (SELECT $1 FROM @RAW.EXAMPLE_JSON.EXAMPLE_LANDING_STAGE/events/)
FILE_FORMAT = (TYPE = JSON)
ON_ERROR = ABORT_STATEMENT;
```

Flatten in dbt, one row per array element:

```sql
SELECT
  e.payload:id::NUMBER            AS event_id,
  i.value:sku::VARCHAR            AS sku,
  i.value:qty::NUMBER             AS qty,
  e.snowflake_loaded_at
FROM {{ source('example_json', 'events') }} e,
     LATERAL FLATTEN(input => e.payload:items) i
```

Add `OUTER => TRUE` to `FLATTEN` to keep events with an empty or missing array. Cast every path explicitly.

## MERGE upsert

Use only when dbt dedupe cannot cover the case, and state why in the review. Land into a staging table, then merge in one statement.

```sql
MERGE INTO ANALYTICS.DBT_PORENTA.TICKETS_CURRENT t
USING (
  SELECT *
  FROM RAW.EXAMPLE_TYPED.TICKETS
  QUALIFY row_number() OVER (PARTITION BY id ORDER BY snowflake_loaded_at DESC) = 1
) s
ON t.id = s.id
WHEN MATCHED AND s.snowflake_loaded_at > t.snowflake_loaded_at THEN UPDATE SET
  t.status = s.status,
  t.snowflake_loaded_at = s.snowflake_loaded_at
WHEN NOT MATCHED THEN INSERT (id, status, snowflake_loaded_at)
  VALUES (s.id, s.status, s.snowflake_loaded_at);
```

The source must hold one row per key. Snowflake errors or applies nondeterministic updates when several source rows match one target row, depending on the `ERROR_ON_NONDETERMINISTIC_MERGE` setting. The inner `QUALIFY` prevents that. dbt incremental models use `incremental_strategy='merge'` with `unique_key` and generate this for you.

## QUALIFY dedupe

```sql
SELECT *
FROM RAW.EXAMPLE_TYPED.TICKETS
QUALIFY row_number() OVER (
  PARTITION BY id
  ORDER BY snowflake_loaded_at DESC
) = 1
```

- Partition by the business key, order by the load stamp. Add a source `updated_at` as the first order key when the source provides one.
- In incremental models, filter new rows first with `where snowflake_loaded_at > (select coalesce(max(snowflake_loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }})`, then scope window functions to the affected ids. Windowing the whole table caused local spill on `ETL_WH`.

## Loading checks

Run after every new pipeline and after any `FORCE` reload. One statement per call.

Per-file results for one table over the last day:

```sql
SELECT file_name, status, row_count, row_parsed, error_count, first_error_message, last_load_time
FROM TABLE(RAW.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'EXAMPLE_TYPED.TICKETS',
  START_TIME => DATEADD(day, -1, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;
```

Schema-wide load log:

```sql
SELECT table_name, file_name, status, row_count, last_load_time
FROM RAW.INFORMATION_SCHEMA.LOAD_HISTORY
WHERE schema_name = 'EXAMPLE_TYPED'
ORDER BY last_load_time DESC
LIMIT 50;
```

Row count reconciliation. Compare the sum of loaded rows for a batch with the table count for the same stamp window and with the count the `etl` run logged:

```sql
SELECT count(*) AS table_rows,
       count(distinct id) AS distinct_ids,
       min(snowflake_loaded_at) AS first_load,
       max(snowflake_loaded_at) AS last_load
FROM RAW.EXAMPLE_TYPED.TICKETS
WHERE snowflake_loaded_at >= DATEADD(day, -1, SYSDATE());
```

- `table_rows` equal to the sum of `row_count` from `COPY_HISTORY` for the window means the load is complete.
- `table_rows` above `distinct_ids` is expected for append loads. It is a defect only when the gap exceeds the number of reloaded files.
- `COPY_HISTORY` in `INFORMATION_SCHEMA` covers the last 14 days. Older history is in `SNOWFLAKE.ACCOUNT_USAGE`, which lags by up to a few hours.
- An empty history for files that exist in the stage means they were never loaded or the stage path is wrong. Check with `LIST`.

## Cost reading

Numbers come from the refactor MCP cost tools or `ACCOUNT_USAGE` views.

- **Local spill** means the warehouse is undersized for the query. **Remote spill** is worse. Fix the query shape first (window scope, join order), then size up one step.
- **`failsafe_gb` far above `active_gb`** means the table is rewritten too often. Convert to transient, or append instead of full rewrite.
- **A saturated warehouse overstates savings.** When `by_warehouse[].saturated` is true, a model's cost includes queueing from neighbors. Check that before promising a drop.
- Zero usage with `as_of.snowflake_usage.present = false` means no data, not a free model.
- Pair cost with consumers. Expensive and dead is a drop candidate. Expensive and alive is an optimization candidate (schedule, incremental, warehouse size).

## Masking policy YAML

Masking is applied in dbt through column meta. The folder-level post-hook `apply_masking_policy('models')` reads it.

```yaml
version: 2
models:
  - name: clean_example_tickets
    columns:
      - name: requester_email
        description: Requester email address
        config:
          meta:
            masking_policy: pii_email_mask
      - name: id
        data_tests:
          - not_null
          - unique
```

- Use the policy names that already exist in `data-dbt`. Do not create, alter, or drop policies. Ask the owner.
- Custom keys go under `config.meta`. Top-level custom keys are a dbt 1.10 deprecation and an error in dbt v2.
- Dev builds of models that call the transcript masking UDF need `--vars '{pii_masking_salt: "<16+ chars>"}'`. Never set that var on prod.

## snow CLI usage

```bash
# ad hoc read, one statement per call
snow sql -c prod --role dbt --warehouse ENGINEER \
  --database ANALYTICS --schema DBT_PORENTA \
  -q "select current_user(), current_role(), current_warehouse()"

# inside the data-dbt repo, use the wrapper instead
./.claude/tools/snowflake-query.sh "select count(*) from raw.example_typed.tickets"
```

- One statement per call. Wait for each to finish before the next.
- Never echo connection settings, tokens, or the contents of `config.toml`.
- Create objects only in `ANALYTICS.DBT_PORENTA`. Drop only objects you created there for the task.
- The wrapper's argument shape may differ. Open the script in the repo before the first call.
