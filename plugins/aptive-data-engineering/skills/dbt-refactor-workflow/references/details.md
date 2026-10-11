# dbt Refactor Workflow: details

Field notes for the dbt refactor MCP (`plugin:dbt-refactor-mcp:refactor`). Fields named here come from the MCP's own documentation and the rebuild skills it ships. If a field is missing from a response, or a tool returns something this file does not describe, load the matching server skill with `get_skill` (names come from `list_skills`) and trust it over this file.

## Contents

1. Orientation fields
2. Per-tool notes
3. Known limitations
4. Parity rule and report template
5. Decision table

## 1. Orientation fields

`xref_summary` reports how many dbt nodes and Sigma workbooks are loaded and how stale each side is.

| Field | Meaning |
|---|---|
| `freshness.warnings` | Non-empty means the loaded data is stale or incomplete |
| `behind_commits` | The inventory is behind the repo by this many commits |
| `sigma_coverage.unpulled_listed_workbooks` | Listed workbooks not pulled yet, so their reads are missing |
| `sigma_coverage.partial_workbooks` | Workbooks pulled only in part |
| `sigma_coverage.failed_workbook_ids` | Workbooks whose pull failed |

Any non-empty value above makes every reader count an undercount. Zero is not proof.

## 2. Per-tool notes

### xref_dead_models

Lists enabled models with no alive consumer, direct or transitive. These are candidates for deprecation review, not an automatic kill list. Snapshots never appear here.

### xref_consumers

Finds the consumers of a dbt model or of any warehouse object (pass `ref=<fqn>` for the latter).

- Covers Sigma workbooks, dbt children, and non-Sigma readers: Airflow, Prefect, AEP, and direct Snowflake access.
- `transitive` shows readers reached through downstream models. A model with no direct reader can still be alive through a child.
- `external_consumers[].confidence` is `strong` or `weak`. Read `line_text` on Airflow matches because a `strong` match can come from prose. `weak` matches do not make a model alive.
- `failing_workbooks` lists workbooks that read the model and are failing. Mention them in the report because a rebuild can change their state.

### dbt_get_model

`model_ref` accepts a model name or a node's `original_file_path`. With `include_columns=true` it returns columns. On `status: ambiguous` it returns `candidates[]`. Call again with one `candidates[].node_key`. For files that are not dbt nodes, use `dbt_get_repo_file`.

### dbt_lineage

- `direction` is `upstream` or `downstream`. Depth defaults to 3.
- Results are paged. Call again with `offset=next_offset` until no further page is returned.
- Each edge has an `edge_source`. `fqn_literal` with `is_literal` marks a hard-coded table name instead of a `ref` or `source`. Treat literal edges as readers that a rename will break and that dbt will not rebuild in order.

### dbt_column_usage

Shows how downstream models and consumers use each column of a model. `output_columns` is the contract the replacement must keep. Run it for each column you plan to keep, and for a column you plan to drop to prove nobody reads it.

### dbt_sources, dbt_search, xref_external_tables

`dbt_sources` lists sources. `dbt_search` takes `q` and `scope` (`scope="code"` finds hard-coded names in code). `xref_external_tables` lists external table readers.

### cost_model

Called with `model_ref` and `days` (use 30). Reports `usd` and `by_warehouse[]` with a `saturated` flag. Also reports spill and failsafe figures.

- Zeros with `as_of.snowflake_usage.present=false` mean no usage data is loaded, not zero cost.
- Local spill means the warehouse is undersized for the model. Remote spill is worse. Fix the query or the warehouse size.
- `failsafe_gb` far above `active_gb` means the table is rewritten too often. Consider an incremental build, a transient table where fail-safe is waste, or a lower build frequency.
- A saturated warehouse (`saturated` true) overstates the model's own share of the cost because the model competed for capacity. Treat savings estimates as upper bounds.
- `cost_top_models` takes `days` and `order_by` (`"credits"`) and returns the ranked list.

### refactor_scaffold_plan

Called with `model_ref` and `target_layer`. Returns a plan for the replacement model and warnings. Stop and ask the user on any warning that the model is disabled, deprecated, an analysis, or a snapshot. The plan is advice. You write the SQL, YAML, and tests, and you keep every consumer column found in `output_columns`.

### refactor_parity_sql

Called with `old_fqn`, `new_fqn`, and `keys`. Use three-part names (`database.schema.table`) and the unique business key. It returns SQL only. Run it yourself.

Read the results in this order:

1. `duplicate_key_rows` on both sides.
2. `null_key_count` on both sides.
3. Row counts.
4. Old-only and new-only keys.
5. Changed rows.

Reading duplicates and nulls first matters because a non-unique key makes every later comparison misleading.

### refactor_sql and the catalog tools

`refactor_sql` reads the inventory database, not Snowflake. `catalog_*` tools read Sigma data stored by the background pull and never call Sigma live. Use `get_skill` for `investigate-sigma-workbook` before using them.

## 3. Known limitations

- Snapshots never appear in `xref_dead_models`. Check a snapshot with `xref_consumers` and `dbt_lineage`.
- `_personal_sql_repos`, `vroom`, and `raw_scripts` are not consumer sources. A reader there is invisible.
- An Airflow `strong` match can come from prose. Read `line_text` before counting it.
- Role and service-user reads in ACCESS_HISTORY are not usage.
- Objects in developer clone schemas are not consumers.
- Every count is only as current as `xref_summary` says it is.
- Server-side skills (`list_skills`, `get_skill`): assess-model-for-rebuild, choose-what-to-carry-over, scaffold-and-prove-parity, explain-snowflake-cost, investigate-sigma-workbook, trace-a-repo-file, trace-source-impact, nf3-third-normal-form, snowflake-postgres-table-design.

## 4. Parity rule and report template

Parity holds when all of these are true:

- Duplicate key rows and null keys are zero on both sides.
- Row counts match.
- The old-only and new-only key sets are empty.
- Every changed row is explained and accepted by the model owner.

Report template:

```
Parity report: <old_fqn> vs <new_fqn>
Key: <columns>    Date run: <YYYY-MM-DD>    Run by: <name>
Inventory state: <xref_summary freshness and coverage, or "clean">

| Check | Old | New | Result |
|---|---|---|---|
| duplicate_key_rows | | | |
| null_key_count | | | |
| row count | | | |
| old-only keys | | | |
| new-only keys | | | |
| changed rows | | | |

Changed rows explained:
- <column>: <count> rows, <cause>, <accepted by whom>

Columns kept from output_columns: <list>
Columns dropped: <list with dbt_column_usage evidence>
Verdict: <holds | does not hold>, <reason>
```

Run each generated statement separately in `ANALYTICS.DBT_PORENTA` with one connection at a time. Do not paste credentials into the report.

## 5. Decision table

| Cost | Alive | Action |
|---|---|---|
| Expensive | No | Drop candidate. Confirm no undercount condition, then hand off to the `deprecating-models` skill |
| Expensive | Yes | Optimize: schedule, incremental build, warehouse size, spill and failsafe causes |
| Cheap | No | Deprecate through the `deprecating-models` skill. Low urgency |
| Cheap | Yes | Leave alone unless a rebuild is wanted for other reasons |

An undercount condition from `xref_summary` weakens every "No" in this table. In that case say the model might still be read and ask the user how to proceed.
