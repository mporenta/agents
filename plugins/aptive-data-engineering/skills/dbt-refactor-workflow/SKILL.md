---
name: dbt-refactor-workflow
description: Assesses, traces, costs, and rebuilds data-dbt models with the dbt refactor MCP (`plugin:dbt-refactor-mcp:refactor`), a read-only inventory of the dbt repo plus Sigma, Airflow, Prefect, AEP, and Snowflake usage. Use when asked "is this model used", "is this model dead", "why is this model expensive", "what breaks if I change this source", "rebuild" or "refactor" a model, "scaffold a clean model", "prove parity", or "what can we drop", and whenever a model name like `model_x` or `report_x` needs impact, cost, or consumer facts before a change.
---

# dbt Refactor Workflow

The refactor MCP answers questions about data-dbt models from stored inventory. It never runs SQL and never writes files. You write the model files and run the generated SQL yourself, in `ANALYTICS.DBT_PORENTA` only (see the run safety rules in the `dbt-snowflake-models` skill). See `references/details.md` for per-tool field notes, known limitations, a parity report template, and the decision table.

## When to Use This Skill

- Check whether a model is used or dead before dropping it
- Find why a model is expensive in Snowflake credits
- Trace what breaks when a source or column changes
- Rebuild or scaffold a clean model from an existing one
- Prove parity between an old model and its rebuild

## Orient first

Call `xref_summary` before any other tool. Read two things.

- `freshness.warnings` and `behind_commits`. If either is non-empty, the inventory lags the repo.
- `sigma_coverage`: `unpulled_listed_workbooks`, `partial_workbooks`, `failed_workbook_ids`. If any is non-empty, workbook reads are incomplete.

Either condition means every reader count is an undercount, and zero readers is not proof of no readers. Say so in the answer and ask the user whether to continue before recommending a drop.

## Alive

A model is alive when any of these hold:

- A listed, non-archived Sigma workbook reads it directly or transitively.
- A reference-type external consumer reads it: Airflow, Prefect, AEP, or a direct Snowflake object dependency.
- An alive dbt child depends on it.

These do not make a model alive: role or service-user reads in ACCESS_HISTORY, de-listed workbooks, dev clone schemas, and `weak` Airflow matches.

## Workflows

Assess a model:

1. `xref_summary`.
2. `dbt_get_model` with `include_columns=true`. If `status` is `ambiguous`, call again with a `candidates[].node_key`.
3. `dbt_lineage` upstream and downstream. Default depth is 3. Page with `offset=next_offset`.
4. `dbt_column_usage` for each column you plan to keep. `output_columns` is the downstream contract.
5. `xref_consumers`. Read `transitive`, `external_consumers[].confidence`, and `failing_workbooks`.
6. `cost_model` with `days=30`.
7. Apply the decision table in `references/details.md`.

Trace a source:

1. `dbt_sources` to find the source and its tables.
2. `dbt_lineage` with `direction="downstream"`. Read `edge_source`. `fqn_literal` with `is_literal` means a hard-coded table name.
3. `xref_consumers` with `ref=<fqn>`. Group results by kind.
4. `dbt_search` with `q=<fqn>` and `scope="code"` to find hard-coded readers.
5. `xref_external_tables` for external table readers.
6. Report every reader kind, not only dbt models.

Explain cost:

1. `cost_top_models` with `days=30` and `order_by="credits"`.
2. `cost_model` on the top candidates.
3. Read `by_warehouse[].saturated`, spill, and failsafe fields as described in `references/details.md`.
4. Pair each candidate with `xref_consumers`.
5. Expensive and dead is a drop candidate. Expensive and alive is an optimize candidate (schedule, incremental, warehouse size).

Scaffold and prove parity:

1. Run the assess workflow first.
2. `refactor_scaffold_plan` with `model_ref` and `target_layer`. Stop on warnings (see below).
3. Write the new model, schema.yml entry, and tests. Keep every column found in `output_columns`.
4. Build the new model in `ANALYTICS.DBT_PORENTA` on the dev target.
5. `refactor_parity_sql` with `old_fqn`, `new_fqn`, and `keys`. Use three-part names and the unique business key.
6. Run the generated SQL yourself, one statement at a time, and fill the parity report.

## Stop and ask the user

- `dbt_get_model` returns `status: ambiguous` and the model cannot be chosen from context.
- `refactor_scaffold_plan` warns that the model is disabled, deprecated, an analysis, or a snapshot.
- The key passed to `refactor_parity_sql` is not unique. Parity on a non-unique key proves nothing, so find the true key first.
- `xref_summary` shows any undercount condition and the task is a drop or deprecation decision.

## Read-only route

No tool on this route runs SQL, writes the repo, or calls Sigma live. You write the files, run the SQL, and create objects in `ANALYTICS.DBT_PORENTA` only. Never alter or drop shared objects. To retire a model after the assessment, use the `deprecating-models` skill.
