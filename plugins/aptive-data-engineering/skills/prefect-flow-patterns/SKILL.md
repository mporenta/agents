---
name: prefect-flow-patterns
description: Writes and reviews Prefect 3 flows, tasks, and deployments for the Aptive self-hosted Prefect stack, including config-driven generic flows, thin wrappers around the Go `etl` binary, prefect.yaml deployment files, work pools, and prefect-snowflake and prefect-dbt integration. Use when adding or changing a flow, a deployment entry, a pipeline spec YAML, a schedule, retries, timeouts, caching, or concurrency limits, when moving an Airflow DAG to Prefect, or when registering and inspecting deployments with the `prefect` CLI. Trigger on prefect.yaml, work pool, docker-pool, run_deployment, PrefectDbtRunner, cron_shell, America/Denver, or "migrate this DAG".
---

# Prefect Flow Patterns (Aptive)

Rules and decision points for Prefect 3 on the Aptive self-hosted server (pinned `prefect-version: 3.8.7`, Python 3.12, uv). Detail lives in `references/details.md`: full prefect.yaml, a complete generic flow, the extract spec shape, retries and caching, concurrency, prefect-snowflake and prefect-dbt setup, operational commands, and a review checklist.

## When to Use This Skill

- Add or change a flow, a deployment entry, or a pipeline spec YAML
- Set a schedule, retries, timeouts, caching, or a concurrency limit
- Wrap the Go `etl` binary in a thin flow
- Move an Airflow DAG to Prefect
- Register and inspect deployments with the `prefect` CLI

## Hard rules

- **Schedules** are IANA `America/Denver`. Never `US/Mountain`, `UTC`, or a fixed offset. When only the timezone changes, the cron text stays unchanged.
- **Register with `active: false`.** Activation is a separate reviewed step. Airflow still runs the DAGs, so two schedulers or none is the main danger.
- **Timeouts and retries are flow parameters** (`retries`, `retry_delay_seconds`, `timeout_seconds`) applied to tasks inside the flow. Do not hard-code them in prefect.yaml or the decorator. Validate parameters at the top of the flow and raise `ValueError`.
- **One generic flow per job type** (`extract`, `publish`, `dbt_build`, `cron_shell`). Each deployment points at a spec YAML under `pipelines/extract/`, `pipelines/publish/`, or `pipelines/dbt/`. Do not write one `@flow` per source.
- **Flows are thin wrappers.** The Go `etl` binary (`/usr/local/bin/etl`, subcommands `plan`, `extract`, `publish`, `load`) does REST calls, Parquet flattening, S3 upload, and `COPY INTO`. Flows plan, run subprocesses, stream logs, materialize assets, and chain.
- **Secrets are never parameters and never logged.** Source credentials come from a mounted env file (`/run/secrets/<source>.env`), Snowflake from the mounted `config.toml` (`SNOWFLAKE_CONFIG_FILE`), AWS from the mounted `~/.aws` with `AWS_PROFILE`. Use `SecretStr` for any password field. Non-secret state goes in Prefect Variables.
- **Server access is CLI and REST only.** Use the `prefect` CLI or the REST API against the self-hosted server (`PREFECT_API_URL=http://server:4200/ml-prefect/api` inside worker containers). The Prefect MCP server is for documentation search only, never for reading or changing server state.
- **Anything that writes gets `concurrency_limit: {limit: 1, collision_strategy: CANCEL_NEW}`** so a slow run cannot overlap the next one.

## Decision points

| Question | Answer |
|---|---|
| Long-lived process on one host, no worker | `flow.serve()` |
| Work pool, image, or schedule managed in YAML | `prefect deploy` with prefect.yaml (Aptive default) |
| Programmatic deploy from Python | `flow.deploy(name, work_pool_name, image, job_variables)` |
| New source with an `etl` extract | New spec YAML plus a deployment entry. No new flow code |
| Python-native Snowflake work | prefect-snowflake blocks are acceptable. The Go path uses the mounted config.toml |
| Verbatim crontab line to replace | `cron_shell` flow |
| Need caching across runs | Enable result persistence first. See details |
| Fire the next job after this one | `run_deployment(..., timeout=0)` for each `then:` entry |

### Which dbt interface

| Need | Use |
|---|---|
| `dbt build` with Prefect observing, asset lineage per node, failed node fails the run | `PrefectDbtRunner` (default, what `dbt_build` uses) |
| Per-node retries or cross-run caching | `PrefectDbtOrchestrator` with `ExecutionMode.PER_NODE`. It is beta, pin `prefect-dbt>=0.7.23` |
| Retries with `PER_WAVE` | Not supported, it raises `ValueError` |

## Deployment entry skeleton

Goes under `deployments:` in `deployments/<group>.yaml`. Shared `work_pool` and `concurrency` blocks come from `definitions:` anchors.

```yaml
- name: extract-example-hourly
  entrypoint: flows.extract:extract
  parameters:
    spec_path: pipelines/extract/example.yaml
    retries: 2
    retry_delay_seconds: 60
    timeout_seconds: 3600
  enforce_parameter_schema: true
  tags: [extract, example]
  schedules:
    - cron: "0 * * * *"
      timezone: America/Denver
      active: false
      slug: extract-example-hourly
  concurrency_limit: *writer_concurrency
  work_pool: *docker_pool
```

## Generic flow skeleton

```python
from prefect import flow, task, get_run_logger

@task
def run_dataset(spec_path: str, dataset: str, timeout_seconds: int) -> int: ...

@flow(log_prints=True)
def extract(spec_path: str, retries: int = 2, retry_delay_seconds: int = 60,
            timeout_seconds: int = 3600,
            interval_start: str | None = None, interval_end: str | None = None):
    if retries < 0 or timeout_seconds <= 0:
        raise ValueError("retries must be >= 0 and timeout_seconds > 0")
    log = get_run_logger()
    plan = ...  # run `etl plan --spec <spec_path>`, parse JSON
    for dataset in plan["datasets"]:
        run_dataset.with_options(
            retries=retries, retry_delay_seconds=retry_delay_seconds,
            timeout_seconds=timeout_seconds,
        )(spec_path, dataset["name"], timeout_seconds)
```

See `references/details.md` for the complete version with validation, subprocess streaming, asset materialization, and chaining.

## Before you finish

- Run `uv sync --locked`, ruff, ty, and pytest. Registration and import checks must not execute SQL or call vendors.
- Confirm the spec header comment names the Airflow DAG replaced, the schedule, logic moved to dbt, and deviations.
- Registering with `uv run prefect deploy --prefect-file deployments/<group>.yaml` changes server state. Ask for approval first.
- Confirm API names against docs.prefect.io (use the Prefect docs search) when a signature here looks unfamiliar.
