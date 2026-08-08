"""
NYC TLC transformations — dbt.

Runs the dbt project after bronze is refreshed.

SCHEDULING
    No cron. This DAG runs on BRONZE_ASSET, emitted by nyc_tlc_load_bronze's
    publish_bronze task. Bronze only changes when TLC publishes, so a time
    schedule would mean ~29 no-op runs a month.

    The chain end to end:
        nyc_tlc_ingest  --LANDING_ASSET-->  nyc_tlc_load_bronze
                                                    |
                                              BRONZE_ASSET
                                                    |
                                                 THIS DAG
                                                    |
                                              MARTS_ASSET  (for BI, MCP, etc.)

WHY THE STEPS ARE SPLIT
    dbt can do all of this with a single `dbt build`, and once the snapshot
    table exists that would work. It is split here because SNAPSHOTS ARE NOT
    PART OF THE MODEL DAG — dbt will not order int_vendors_observed ->
    snapshot -> dim_vendor for you. The dependency is real but invisible to
    dbt, so Airflow enforces it.

    Splitting also means a failure tells you WHICH layer broke, and a retry
    resumes from there instead of rerunning everything.

WHY EVERY STEP RUNS EVERY TIME
    A reasonable instinct is "seed once, then only run models." Resist it:

    - Seeds are four tiny CSVs. Re-seeding is seconds and guarantees the
      warehouse matches the repo. Skipping it means a seed edit silently
      never reaches Databricks.
    - The snapshot MUST run on every refresh. It captures state that cannot
      be reconstructed later — a vendor rename that happens between runs is
      lost forever if the snapshot didn't run. This is the one thing in dbt
      that is not reproducible from source.
    - Models are incremental, so a run processes only new periods anyway.

    The genuinely one-time work is the FIRST build. See the notes at the
    bottom.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Must be byte-identical to the outlet in nyc_tlc_load_bronze.py.
BRONZE_ASSET = Asset("nyc-transit://bronze/trips")

# Emitted when marts are rebuilt. Downstream consumers — a BI refresh, an
# MCP server cache warm — can schedule on this.
MARTS_ASSET = Asset("nyc-transit://marts/nyc_trips")

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/dbt"

# --no-write-json keeps run artifacts out of the mounted project directory,
# which would otherwise fill with files owned by the Airflow container's uid
# and confuse local dbt runs. Drop it if you want to collect run_results.json.
DBT_FLAGS = "--no-use-colors"


def dbt_task(task_id: str, command: str, **kwargs) -> BashOperator:
    """Build a BashOperator that invokes dbt.

    `cd` first because dbt resolves dbt_project.yml relative to the working
    directory. The absolute path to the venv binary is deliberate — dbt is
    intentionally not on PATH, so nothing can accidentally pick it up in a
    context where its dependencies would conflict with Airflow's.
    """
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} {DBT_FLAGS} {command}",
        **kwargs,
    )


@dag(
    dag_id="nyc_tlc_transform",
    schedule=[BRONZE_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["nyc-transit", "dbt", "transform"],
    doc_md=__doc__,
)
def nyc_tlc_transform():

    # `dbt deps` installs packages listed in packages.yml (dbt_utils, which
    # the surrogate key macro depends on). Fast and idempotent when packages
    # are already present, so it's cheap insurance against a stale image.
    deps = dbt_task("dbt_deps", "deps")

    # Loads the four lookup CSVs. --full-refresh because seeds are small and
    # a truncate-and-reload is the only way to remove a deleted row; without
    # it, dbt appends and a removed lookup value lingers.
    seed = dbt_task("dbt_seed", "seed --full-refresh")

    with TaskGroup("staging") as staging:
        # Views over bronze. Cheap — no data is materialized, so this is
        # really just validating that the SQL compiles against the current
        # bronze schema.
        dbt_task("dbt_run_staging", "run --select staging")

    with TaskGroup("dimensions") as dimensions:
        # Order here is the whole reason this DAG isn't one `dbt build`.
        observed = dbt_task(
            "dbt_run_int_vendors", "run --select int_vendors_observed"
        )
        # Captures SCD2 history. New vendor IDs are inserted; changed names
        # expire the prior row. This is the state-capturing step.
        snapshot = dbt_task("dbt_snapshot", "snapshot")
        # Reads the snapshot and presents validity windows.
        dim = dbt_task("dbt_run_dim_vendor", "run --select dim_vendor")

        observed >> snapshot >> dim

    with TaskGroup("marts") as marts:
        # Incremental merge. Processes only new periods.
        dbt_task("dbt_run_marts", "run --select nyc_trips")

    # Tests run after everything is built rather than interleaved, so a
    # failing test doesn't block a downstream model that might have been
    # fine. The tradeoff is that bad data reaches marts before the test
    # catches it — acceptable in bronze/silver, worth revisiting if marts
    # ever feed something customer-facing.
    #
    # --store-failures writes failing rows to a table so you can inspect
    # them rather than just seeing a count.
    test = dbt_task("dbt_test", "test --store-failures")

    # Emits MARTS_ASSET. Only fires on success, so a failed transform doesn't
    # tell downstream consumers there's fresh data.
    publish = BashOperator(
        task_id="publish_marts",
        bash_command="echo 'marts refreshed'",
        outlets=[MARTS_ASSET],
    )

    deps >> seed >> staging >> dimensions >> marts >> test >> publish


nyc_tlc_transform()


# ===========================================================================
# FIRST RUN  (reference only)
# ===========================================================================
#
# Run these by hand once, from the dbt container, before enabling the DAG.
# They establish the snapshot baseline and confirm each layer independently:
#
#   docker compose run --rm dbt deps
#   docker compose run --rm dbt seed
#   docker compose run --rm dbt run --select staging
#   docker compose run --rm dbt run --select int_vendors_observed
#   docker compose run --rm dbt snapshot          # creates the SCD2 table
#   docker compose run --rm dbt run --select dim_vendor
#   docker compose run --rm dbt run --select nyc_trips --full-refresh
#   docker compose run --rm dbt test
#
# Note --full-refresh on the first nyc_trips build. An incremental model's
# first run creates the table from the full source anyway, but being explicit
# avoids ambiguity if a partial table already exists from a failed attempt.
#
# After that, the DAG above handles every subsequent refresh and no step is
# one-time. --full-refresh should NOT be in the DAG: it would rebuild every
# period on every run, which is exactly what incremental materialization
# exists to avoid.
#
# ===========================================================================
# UPGRADING TO COSMOS  (when model-level visibility is worth the setup)
# ===========================================================================
#
# astronomer-cosmos parses target/manifest.json and generates one Airflow
# task per dbt model, so the Airflow graph mirrors dbt lineage exactly. That
# gives per-model retries and makes "which model failed" visible without
# opening logs.
#
#   from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
#
#   transform = DbtTaskGroup(
#       project_config=ProjectConfig("/dbt"),
#       profile_config=ProfileConfig(
#           profile_name="nyc_transit",
#           target_name="dev",
#           profiles_yml_filepath="/dbt/profiles.yml",
#       ),
#       execution_config=ExecutionConfig(dbt_executable_path=DBT_BIN),
#   )
#
# The snapshot ordering problem does NOT go away — Cosmos reads the same
# manifest dbt does, and the snapshot is still outside the model DAG. It
# would still need explicit sequencing.
