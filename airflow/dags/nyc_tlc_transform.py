"""
NYC TLC transformations — dbt.

Runs the dbt project after raw is refreshed.

SCHEDULING
    No cron. Triggered by the raw assets emitted by nyc_tlc_load_raw.
    A list means AND — both yellow and green must have refreshed since this
    DAG last ran. Use (RAW_YELLOW | RAW_GREEN) for OR semantics if you
    would rather transform whenever either table updates.

    Full chain:
        nyc_tlc_ingest  --LANDING-->  nyc_tlc_load_raw
                                              |
                                          RAW_ALL_ASSETS
                                              |
                                          THIS DAG
                                              |
                                          FORMAL_ALL   (BI, MCP, exports)

WHY THE STEPS ARE SPLIT
    `dbt build` would run all of this in one command, and once the snapshot
    table exists that mostly works. It is split because SNAPSHOTS ARE NOT PART
    OF THE MODEL DAG — dbt will not order int_vendors_observed -> snapshot ->
    dim_vendor for you. That dependency is real but invisible to dbt, so
    Airflow enforces it.

    Splitting also means a failure names the layer that broke, and a retry
    resumes there instead of re-running everything.

WHY EVERY STEP RUNS EVERY TIME
    - Seeds are four small CSVs. Re-seeding guarantees the warehouse matches
      the repo; skipping it means a seed edit silently never deploys.
    - The snapshot MUST run on every refresh. It captures state that cannot
      be reconstructed — a vendor rename occurring between two runs is lost
      permanently if the snapshot did not run in between. It is the only
      object in dbt that is not reproducible from source.
    - Models are incremental, so a run processes only new periods anyway.

    The genuinely one-time work is the first build. See notes at the bottom.
"""

from __future__ import annotations

import pendulum

# use assets.py for shared asset definitions
from assets import FORMAL_ALL_ASSETS, RAW_ALL_ASSETS

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup, dag

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# dbt lives in its own virtualenv so its dependency tree cannot collide with
# Airflow's. It is deliberately NOT on PATH — the absolute path is the point.
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/dbt"

# --no-use-colors keeps ANSI escapes out of the Airflow task logs.
DBT_FLAGS = "--no-use-colors"


def dbt_task(task_id: str, command: str, **kwargs) -> BashOperator:
    """BashOperator that invokes dbt from the project directory.

    `cd` first because dbt resolves dbt_project.yml relative to the working
    directory. `set -e` so a non-zero dbt exit fails the task rather than
    being swallowed by the shell.
    """
    return BashOperator(
        task_id=task_id,
        bash_command=f"set -e; cd {DBT_DIR} && {DBT_BIN} {DBT_FLAGS} {command}",
        **kwargs,
    )


@dag(
    dag_id="nyc_tlc_transform",
    schedule=RAW_ALL_ASSETS,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["NYC TAXI AND LIMOUSINE COMMISSION", "TRANSFORM", "PUBLIC", "DATABRICKS", "NYC TRANSIT"],
    doc_md=__doc__,
)
def nyc_tlc_transform():
    # Installs dbt_utils from packages.yml. Fast and idempotent when already
    # present — cheap insurance against a stale image.
    deps = dbt_task("dbt_deps", "deps")

    # --full-refresh on seeds: they are small, and a truncate-and-reload is
    # the only way a DELETED lookup row actually disappears. Without it dbt
    # appends and a removed value lingers indefinitely.
    seed = dbt_task("dbt_seed", "seed --full-refresh")

    with TaskGroup("staging") as staging:
        # Views over raw. Nothing is materialized, so this is really
        # validating that the SQL still compiles against the current raw
        # schema — a cheap early warning when TLC changes a column.
        dbt_task("dbt_run_staging", "run --select staging")

    with TaskGroup("dimensions") as dimensions:
        # This ordering is the entire reason this DAG is not one dbt build.
        observed = dbt_task("dbt_run_int_vendors", "run --select int_vendors_observed")
        snapshot = dbt_task("dbt_snapshot", "snapshot")
        dim = dbt_task("dbt_run_dim_vendor", "run --select dim_vendor")

        observed >> snapshot >> dim

    with TaskGroup("formal") as formal:
        # Incremental merge — processes only new periods. Contracts are
        # enforced on this model, so a schema drift fails here rather than
        # reaching the formal catalog.
        dbt_task("dbt_run_nyc_trips", "run --select nyc_trips")

    # Tests run after the build rather than interleaved, so one failing test
    # doesn't block a model that would have succeeded. Tradeoff: bad data
    # reaches formal before the test catches it. Acceptable while nothing
    # customer-facing consumes it; revisit if that changes.
    #
    # --store-failures writes failing rows to a table so they can be
    # inspected rather than just counted.
    test = dbt_task("dbt_test", "test --store-failures")

    # Emits the formal assets. Only fires on success, so a failed transform
    # never tells downstream consumers there is fresh data.
    publish = EmptyOperator(task_id="publish_formal", outlets=FORMAL_ALL_ASSETS)

    deps >> seed >> staging >> dimensions >> formal >> test >> publish


nyc_tlc_transform()


# ===========================================================================
# FIRST RUN  (reference only)
# ===========================================================================
# Run once by hand from the dbt container before enabling this DAG. Doing it
# step by step surfaces contract and type mismatches one at a time instead of
# buried in a task log.
#
#   D="docker compose --env-file .env -f docker/docker-compose.yml run --rm dbt"
#   $D deps
#   $D seed --full-refresh
#   $D run --select staging
#   $D run --select int_vendors_observed
#   $D snapshot                              # creates the SCD2 table
#   $D run --select dim_vendor
#   $D run --select nyc_trips --full-refresh
#   $D test
#
# --full-refresh belongs on that first nyc_trips build only. It must NOT be
# in the DAG — it would rebuild every period on every run, defeating the
# incremental materialization entirely.
