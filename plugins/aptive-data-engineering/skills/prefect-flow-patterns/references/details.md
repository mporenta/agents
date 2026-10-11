# Prefect flow patterns: details

Full examples and operational reference for `SKILL.md`. Facts were checked against docs.prefect.io in 2026-10 (Prefect OSS 3.8.8 latest, Aptive pins 3.8.7). Confirm any signature that looks unfamiliar with the Prefect docs search before relying on it.

## Contents

1. [Full prefect.yaml](#full-prefectyaml)
2. [Generic spec-driven flow](#generic-spec-driven-flow)
3. [Extract spec shape](#extract-spec-shape)
4. [Retries and caching](#retries-and-caching)
5. [Concurrency](#concurrency)
6. [prefect-snowflake](#prefect-snowflake)
7. [PrefectDbtRunner](#prefectdbtrunner)
8. [PrefectDbtOrchestrator](#prefectdbtorchestrator)
9. [cron_shell](#cron_shell)
10. [Operational commands](#operational-commands)
11. [Review checklist](#review-checklist)

## Full prefect.yaml

One file per group at `deployments/<group>.yaml`. `build: null` and `push: null` because the image already contains the code. The `pull` step only sets the working directory. Anchors under `definitions:` keep the work pool and concurrency block identical across deployments.

```yaml
name: aptive-extract
prefect-version: 3.8.7

build: null
push: null

pull:
  - prefect.deployments.steps.set_working_directory:
      directory: /app

definitions:
  work_pool: &docker_pool
    name: docker-pool
    work_queue_name: default
    job_variables:
      image: aptive-etl:latest
      image_pull_policy: Never
      auto_remove: true
      name: "{{ ctx.flow_run.id }}"
      networks: [prefect_default]
      env:
        AWS_PROFILE: aptive
        SNOWFLAKE_CONFIG_FILE: /home/datade/.config/snowflake/config.toml
        PREFECT_API_URL: http://server:4200/ml-prefect/api
      volumes:
        - /run/secrets/example.env:/run/secrets/example.env:ro
        - /home/datade/.aws:/home/datade/.aws:ro
        - /home/datade/.config/snowflake:/home/datade/.config/snowflake:ro
  concurrency: &writer_concurrency
    limit: 1
    collision_strategy: CANCEL_NEW

deployments:
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

Notes:

- `job_variables` override the work pool base job template (`env`, `image`, `volumes`, `networks`). Image, network, and mount values above are placeholders. Copy the real ones from an existing file in `deployments/`.
- Secrets appear only as mounts, never as `env` values or `parameters`.
- `interval` schedules use `interval: <seconds>`. Cron schedules accept `cron`, `timezone`, `day_or`, `active`, `slug`.
- To pull code from git instead, replace the `pull` step with `prefect.deployments.steps.git_clone`.

## Generic spec-driven flow

Each deployment passes a spec path. The flow validates parameters, plans with `etl plan`, runs one `etl extract` subprocess per dataset as a task, materializes one asset per target table, then chains the deployments named in `then:`.

```python
import json
import subprocess
from pathlib import Path

import yaml
from prefect import flow, task, get_run_logger
from prefect.assets import materialize
from prefect.deployments import run_deployment
from prefect.context import FlowRunContext

ETL = "/usr/local/bin/etl"


def run_streaming(cmd: list[str], timeout_seconds: int) -> int:
    """Run a subprocess, forward each output line to the run log, never log env."""
    log = get_run_logger()
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, start_new_session=True,
    )
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            log.info(line.rstrip())
        return proc.wait(timeout=timeout_seconds)
    except BaseException:
        proc.terminate()
        raise


def interval_args(plan_window: str, start: str | None, end: str | None) -> list[str]:
    if plan_window == "watermark":
        return []  # watermark datasets get no interval args
    if start is None or end is None:
        # scheduled run: derive from expected_start_time when auto_scheduled,
        # using the cron timetable in America/Denver (project helper, not shown)
        start, end = derive_interval(FlowRunContext.get().flow_run)
    return ["--interval-start", start, "--interval-end", end]
```

`etl` signatures: `etl plan --spec <yaml>`, `etl extract --spec <yaml> [--dataset <name>] [--interval-start RFC3339 --interval-end RFC3339] [--dry-run] [--land-only] [--load-target snowflake|postgres] [--cursor-start <token>]`, and `etl publish --spec <yaml> --step <name> [--send]`. The interval flags go together or not at all, and neither is valid for a `window: watermark` dataset.

```python
@task
def extract_dataset(spec_path: str, name: str, window: str,
                    start: str | None, end: str | None, timeout_seconds: int) -> str:
    cmd = [ETL, "extract", "--spec", spec_path, "--dataset", name,
           *interval_args(window, start, end)]
    rc = run_streaming(cmd, timeout_seconds)
    if rc != 0:
        raise RuntimeError(f"etl extract failed for {name} with exit code {rc}")
    return name


def materialize_target(asset_key: str):
    @materialize(asset_key)
    def _mark(name: str) -> str:
        return name
    return _mark


@flow(log_prints=True)
def extract(spec_path: str,
            retries: int = 2,
            retry_delay_seconds: int = 60,
            timeout_seconds: int = 3600,
            interval_start: str | None = None,
            interval_end: str | None = None):
    # 1. validate
    if retries < 0:
        raise ValueError("retries must be >= 0")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must be >= 0")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0")
    if (interval_start is None) != (interval_end is None):
        raise ValueError("pass both interval_start and interval_end or neither")
    if not Path(spec_path).is_file():
        raise ValueError(f"spec not found: {spec_path}")

    # 2. plan: JSON on stdout, no credentials
    plan = json.loads(subprocess.run(
        [ETL, "plan", "--spec", spec_path],
        capture_output=True, text=True, check=True,
    ).stdout)
    spec = yaml.safe_load(Path(spec_path).read_text())

    # 3. one task per dataset, retries and timeout come from parameters
    runner = extract_dataset.with_options(
        retries=retries, retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=timeout_seconds,
    )
    done = []
    for ds in plan["datasets"]:
        done.append(runner(spec_path, ds["name"], spec["window"],
                           interval_start, interval_end, timeout_seconds))
        # 4. one asset per target table. Key shape is a project decision.
        # materialize_target("snowflake://<account>/raw/<schema>/<table>")(ds["name"])

    # 5. chain without waiting
    for dep in spec.get("then", []):
        run_deployment(name=dep, parameters={}, timeout=0)
```

Points to keep when adapting:

- `derive_interval` and the asset key scheme are project code, not Prefect APIs. `derive_interval` reads `flow_run.expected_start_time` and `flow_run.auto_scheduled` and applies the cron timetable in America/Denver.
- `materialize(*assets, by=...)` decorates a task. Pass the asset key (an `Asset` or a URI string) as the first argument.
- `run_deployment(name="flow/deployment", parameters=..., timeout=0)` returns after creating the run. Without `timeout=0` it waits for completion.
- `then:` entries are `flow-name/deployment-name` strings declared in the spec.

## Extract spec shape

`pipelines/extract/<source>.yaml`. The header comment records the Airflow DAG replaced, the schedule, logic moved to dbt, and deviations.

```yaml
# Replaces Airflow DAG: <dag_id>
# Schedule: <cron> America/Denver
# Moved to dbt: <dedupe, joins, renames>
# Deviations: <none or list>
job: example_hourly
source:
  base_url: https://api.example.com
  auth:
    type: header
    header_name: Authorization
    env:
      value: EXAMPLE_API_TOKEN       # name of the env var in the mounted env file
  headers: {Accept: application/json}
  concurrency: 4
  min_request_interval_ms: 200
  timeout_seconds: 60
window: interval                      # interval | watermark
output:
  bucket: <bucket>
  prefix: example/
  region: us-west-2
datasets:
  - name: tickets
    request:
      method: GET
      path: /v2/tickets
      query: {status: open}
      query_repeat: {}
      pagination:
        style: cursor
        cursor_param: cursor
        cursor_path: meta.next
        max_pages: 500
      records_path: data
      records_optional: false
    model:
      ref: models/tickets.yaml
    target:
      database: RAW
      schema: EXAMPLE_TYPED
      table: TICKETS
      load_strategy: append
      stage: "@RAW.EXAMPLE_TYPED.EXAMPLE_LANDING_STAGE"
then:
  - dbt-build/dbt-build-example
```

The `source.auth.env.value` entry names an environment variable. The value itself lives in `/run/secrets/<source>.env`.

## Retries and caching

- Retries: `@task(retries=N, retry_delay_seconds=int | [..], retry_jitter_factor=..., timeout_seconds=...)`. Aptive sets these through `task.with_options(...)` from flow parameters. `retry_jitter_factor` is honored by the task engine since 3.8.8, so on the pinned 3.8.7 it has no effect.
- Retry only idempotent steps. An `etl extract` into a timestamped S3 prefix is safe to retry. A step that appends to Snowflake is not, unless the load is guarded. Append loads rely on dbt dedupe, so a retry creates duplicate rows by design.
- Caching needs result persistence, which is off by default. Set `PREFECT_RESULTS_PERSIST_BY_DEFAULT=true`, or pass `persist_result=True` or `result_storage` on the task or flow. Without it, `cache_policy` has no effect.
- The default cache key is task inputs plus task code plus flow run id. A task called twice with identical inputs inside one flow run is cached, which surprises subprocess tasks that should run twice. Set `cache_policy=NO_CACHE` (from `prefect.cache_policies`) or `persist_result=False` on those.
- Do not cache tasks that call vendors or write to Snowflake. Cache pure planning or parsing tasks only.

## Concurrency

- Tag-based task limits: `prefect concurrency-limit create <tag> <n>`. Tasks carrying that tag wait for a slot. `tag_concurrency_slot_wait_seconds` (default 30) sets how long a task waits between checks.
- Global limits for code blocks that are not tasks:

```python
from prefect.concurrency.sync import concurrency

with concurrency("snowflake-loads", occupy=1):
    run_load()
```

- Deployment level: `concurrency_limit: {limit: 1, collision_strategy: CANCEL_NEW}` on every deployment that writes. `CANCEL_NEW` cancels the new run when one is active.

## prefect-snowflake

Use blocks for Python-native flows only. The Go `etl` path uses the mounted `config.toml`.

```bash
uv add "prefect[snowflake]"
uv run prefect block register -m prefect_snowflake
```

```python
from prefect_snowflake import SnowflakeCredentials, SnowflakeConnector

creds = SnowflakeCredentials(account="<account>", user="<user>", password="<from secret>")
creds.save("snowflake-creds", overwrite=True)

SnowflakeConnector(
    credentials=creds, database="RAW", schema="EXAMPLE_TYPED", warehouse="ETL_WH",
).save("snowflake-etl", overwrite=True)

with SnowflakeConnector.load("snowflake-etl") as conn:
    rows = conn.fetch_many("select 1", size=10)
```

`SnowflakeCredentials` also accepts private key fields in place of `password`. Methods on the connector: `fetch_many`, `execute`, `execute_many`. Saving blocks changes server state, so it needs approval like any registration.

## PrefectDbtRunner

Install: `uv add "prefect[dbt]" "prefect-dbt[snowflake]"`. The `dbt_build` flow runs `dbt build --select <select> --target <profiles_target>` from a spec.

```python
from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

@flow
def dbt_build(spec_path: str, retries: int = 0, timeout_seconds: int = 7200):
    spec = load_spec(spec_path)      # project helper, validates job, project_dir, select
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=spec["project_dir"],
            profiles_dir="/home/datade/.dbt",   # DBT_PROFILES_DIR, mounted read-only
        ),
        raise_on_failure=True,   # a failed node fails the run
    )
    runner.invoke(["build", "--select", spec["select"],
                   "--target", spec["profiles_target"]])
```

- Each dbt node becomes a Prefect task with asset lineage. Disable with `prefect: enable_assets: False` in `dbt_project.yml`.
- `PrefectDbtSettings` also takes `log_level`. `include_compiled_code` is a runner option, default false.
- `profiles.yml` templating resolves at runtime, for example `"{{ prefect.blocks.snowflake-credentials.<block>.account }}"` and `target: "{{ prefect.variables.target }}"`. At Aptive the mounted `~/.dbt/profiles.yml` is the default, templating is for Python-native setups that keep credentials in blocks.
- Dev builds never use `--target prod` from an agent. Prod runs go through the reviewed deployment.

## PrefectDbtOrchestrator

Beta. The symbols live in a private module today (`prefect_dbt.core._orchestrator`), so import paths may change. Pin `prefect-dbt>=0.7.23`.

```python
from datetime import timedelta
from prefect_dbt.core._orchestrator import PrefectDbtOrchestrator, CacheConfig, ExecutionMode

orchestrator = PrefectDbtOrchestrator(
    settings=settings,
    execution_mode=ExecutionMode.PER_NODE,
    cache=CacheConfig(expiration=timedelta(hours=24)),
)
```

`CacheConfig` fields:

| Field | Default | Meaning |
|---|---|---|
| `expiration` | `None` | Global TTL. `None` means no expiry |
| `result_storage` | `None` | Path or block slug for cached results |
| `key_storage` | `None` | Path or block slug for cache keys |
| `use_source_freshness_expiration` | `False` | Per-node TTL from upstream source freshness thresholds |
| `exclude_materializations` | `{"incremental"}` | Materializations never cached |
| `exclude_resource_types` | `{Test, Snapshot, Unit}` | Resource types never cached |

- Caching is only available in `PER_NODE` mode. Retries with `PER_WAVE` raise `ValueError`.
- Cache key: source SQL hash (not compiled SQL), node config, relation name, `full_refresh`, macro hashes, and upstream keys. Var or env changes that only alter compilation do not invalidate it.
- Test strategies are `IMMEDIATE`, `DEFERRED`, `SKIP`. `plan()` gives a dry run. `DbtCloudExecutor` exists for dbt Cloud.
- Most Aptive models are incremental, so default caching skips them. Expect little benefit.

## cron_shell

Replaces a crontab line verbatim. The flow takes `command`, `timeout_seconds`, and `artifact_key` as parameters.

- Run the command with `/bin/sh -c` in its own process group (`start_new_session=True`).
- On timeout or flow cancel, send SIGTERM to the group, not just the shell.
- Stream output to the run log.
- Publish one Markdown artifact per run with `prefect.artifacts.create_markdown_artifact`, with a per-deployment `artifact_key`. Include exit code, duration, and last output lines, no environment dump.

## Operational commands

Run from the repo root. Reads need no approval. Registration and runs change server state, so ask first.

```bash
# register deployments from one file (state change, approval required)
uv run prefect deploy --prefect-file deployments/<group>.yaml

# inspect
uv run prefect deployment ls
uv run prefect flow-run ls
uv run prefect work-pool ls

# trigger one run with parameter overrides (state change, approval required)
uv run prefect deployment run '<flow>/<deployment>' --param spec_path=pipelines/extract/example.yaml \
  --param interval_start=2026-10-10T00:00:00-06:00 --param interval_end=2026-10-11T00:00:00-06:00

# tag concurrency
uv run prefect concurrency-limit create <tag> <n>
```

Confirm the profile points at the self-hosted server first (`PREFECT_API_URL`, basic auth through `PREFECT_API_AUTH_STRING`). For REST calls, use the filter endpoints under `/api`. Never print the auth string.

## Review checklist

- Every schedule is `America/Denver` and `active: false`.
- `retries`, `retry_delay_seconds`, `timeout_seconds` are flow parameters, validated with `ValueError`.
- The deployment sets `enforce_parameter_schema: true` and `concurrency_limit` with `CANCEL_NEW` when it writes.
- No secret in `parameters`, `env`, logs, or the spec. Mounts only.
- The flow is generic and spec-driven. No per-source `@flow`.
- Cached tasks have persistence enabled and no side effects.
- The spec header names the Airflow DAG, schedule, logic moved to dbt, and deviations.
- `uv sync --locked`, ruff, ty, and pytest pass without executing SQL or calling vendors.
