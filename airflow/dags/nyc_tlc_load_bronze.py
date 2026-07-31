"""
NYC TLC bronze loader.

Loads Parquet from the S3 landing zone into two pre-existing Databricks
tables: yellow_trips and green_trips.

SCHEDULING
    This DAG has no time schedule. It runs when the ingest DAG announces that
    new files landed — see `schedule=[LANDING_ASSET]` below. Airflow queues it
    within seconds of the upstream asset event.

WHY COPY INTO
    COPY INTO keeps its own record of which files it has already ingested, per
    target table, inside the Delta transaction log. Re-running it on a
    directory it has already processed is a no-op. That means this DAG is
    safe to run any number of times, which in turn means retries are free and
    a duplicated asset event costs nothing.

    The alternative, INSERT INTO ... SELECT, would happily load the same file
    twice and silently double your row counts.

PREREQUISITES  (all of these will bite you if skipped)
    1. An Airflow connection `databricks_default` (host + personal access
       token), and apache-airflow-providers-databricks installed.
    2. A SQL warehouse, with its HTTP path filled in below.
    3. A Unity Catalog EXTERNAL LOCATION covering the S3 landing prefix, with
       READ FILES granted to whoever the token belongs to. Databricks cannot
       read your bucket just because Airflow can — they authenticate
       separately. See the notes at the bottom of this file.
    4. Target tables that already have the extra columns this DAG adds
       (period, _source_file, _loaded_at). See TABLE DDL at the bottom.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

# Same compatibility shim as the ingest DAG.
try:
    from airflow.sdk import Asset          # Airflow 3.x
except ImportError:                        # pragma: no cover
    from airflow.datasets import Dataset as Asset  # Airflow 2.4+

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
BUCKET = "your-lakehouse-bucket"
LANDING_PREFIX = "landing/nyc_tlc"

# MUST be byte-identical to the LANDING_ASSET in nyc_tlc_ingest.py. Airflow
# matches assets by exact string. If the loader never triggers, compare these
# two lines character by character before debugging anything else.
LANDING_ASSET = Asset(f"s3://{BUCKET}/{LANDING_PREFIX}/")

# What this DAG produces, so a dbt DAG can schedule on it later.
BRONZE_ASSET = Asset("databricks://nyc_transit/bronze/trips")

CATALOG = "nyc_transit"
SCHEMA = "bronze"

# Maps the landing-zone dataset name to its target table. Adding fhvhv later
# is a one-line change here, nothing else.
TABLES = {
    "yellow": "yellow_trips",
    "green": "green_trips",
}

DATABRICKS_CONN_ID = "databricks_default"

# From your SQL warehouse: Connection details tab. Looks like
# /sql/1.0/warehouses/abc123def456.
SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/REPLACE_ME"


def _sql_hook() -> DatabricksSqlHook:
    """One place to construct the Databricks SQL client."""
    return DatabricksSqlHook(
        databricks_conn_id=DATABRICKS_CONN_ID,
        http_path=SQL_WAREHOUSE_HTTP_PATH,
    )


@dag(
    dag_id="nyc_tlc_load_bronze",

    # THIS IS THE SCHEDULING ANSWER. Instead of a cron string, hand Airflow a
    # list of assets. The DAG becomes event-driven: it runs when every asset
    # in the list has been updated since its last run.
    #
    # Pass multiple assets and the semantics are AND — all must update. In
    # Airflow 3 you can also express OR with the | operator.
    schedule=[LANDING_ASSET],

    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["nyc-transit", "bronze", "databricks"],
    doc_md=__doc__,
)
def nyc_tlc_load_bronze():

    # -----------------------------------------------------------------------
    # TASK 1: build the list of load jobs
    # -----------------------------------------------------------------------
    @task
    def load_targets() -> list[dict]:
        """One work item per table, for dynamic task mapping.

        TABLES.items() yields (key, value) pairs from the dict. Unpacking them
        into two loop variables like this is a very common Python idiom.
        """
        return [
            {
                "dataset": dataset,
                "table": f"{CATALOG}.{SCHEMA}.{table}",
                "source": f"s3://{BUCKET}/{LANDING_PREFIX}/dataset={dataset}/",
            }
            for dataset, table in TABLES.items()
        ]
        # That's a list comprehension building dicts — same as a for-loop with
        # .append(), just denser. You'll see this shape constantly in Python.

    # -----------------------------------------------------------------------
    # TASK 2: COPY INTO
    # -----------------------------------------------------------------------
    @task(max_active_tis_per_dag=2)
    def copy_into(target: dict) -> dict:
        """Load any not-yet-ingested Parquet files into one table."""

        # Triple-quoted string, so the SQL can span lines naturally. The `f`
        # prefix still works, letting us inject the table and path.
        #
        # A note on the SELECT wrapper: COPY INTO can take a bare path, but
        # wrapping it in a SELECT lets us add lineage columns. `_metadata` is
        # a hidden column Databricks exposes on any file-based read — it
        # carries the source file's path, size, and modification time. Landing
        # that in bronze means you can always trace a row back to its file.
        #
        # `period` is NOT in the SELECT explicitly because it arrives via
        # SELECT *. Our S3 layout uses dataset=.../period=.../ directories,
        # and Databricks reads Hive-style directory names as columns
        # automatically. That's free partition metadata, but it does mean the
        # target table needs a `period` column or the load fails.
        sql = f"""
            COPY INTO {target['table']}
            FROM (
              SELECT
                *,
                _metadata.file_path              AS _source_file,
                _metadata.file_modification_time AS _source_modified_at,
                current_timestamp()              AS _loaded_at
              FROM '{target['source']}'
            )
            FILEFORMAT = PARQUET
            PATTERN = '*.parquet'
            FORMAT_OPTIONS ('mergeSchema' = 'true')
            COPY_OPTIONS  ('mergeSchema' = 'true')
        """
        # mergeSchema appears twice on purpose and they do different jobs.
        # FORMAT_OPTIONS lets Databricks reconcile schemas ACROSS the source
        # files it's reading. COPY_OPTIONS lets it add new columns to the
        # TARGET TABLE. You need both to survive TLC adding a column mid-year
        # the way they did with cbd_congestion_fee in 2025.

        log.info("COPY INTO %s from %s", target["table"], target["source"])

        # handler=fetch_all_handler tells the hook to return result rows.
        # Without it you get None back. COPY INTO returns one row summarizing
        # what it did — number of rows inserted, files skipped, and so on.
        rows = _sql_hook().run(sql, handler=fetch_all_handler)

        summary = str(rows[0]) if rows else "no result returned"
        log.info("%s -> %s", target["table"], summary)

        return {**target, "copy_result": summary}

    # -----------------------------------------------------------------------
    # TASK 3: sanity check
    # -----------------------------------------------------------------------
    @task
    def verify(loaded: list[dict]) -> list[dict]:
        """Count rows per table and per period so the load is auditable.

        Not strictly required, but a load DAG that can't tell you what it did
        is a load DAG you won't trust at 2 AM.
        """
        hook = _sql_hook()
        report = []

        for item in loaded:
            sql = f"""
                SELECT period,
                       COUNT(*)      AS row_count,
                       MAX(_loaded_at) AS last_loaded
                FROM {item['table']}
                GROUP BY period
                ORDER BY period DESC
                LIMIT 5
            """
            rows = hook.run(sql, handler=fetch_all_handler)

            for row in rows or []:
                log.info("%s | %s", item["table"], row)

            # An empty table after a load means something is quietly wrong —
            # usually a path with no matching files, which COPY INTO treats as
            # success rather than an error.
            if not rows:
                raise ValueError(
                    f"{item['table']} is empty after load. Check that "
                    f"{item['source']} contains .parquet files and that the "
                    "Unity Catalog external location grants READ FILES."
                )

            report.append({"table": item["table"], "periods": len(rows)})

        return report

    # -----------------------------------------------------------------------
    # TASK 4: announce bronze is ready
    # -----------------------------------------------------------------------
    # Same producer pattern as the ingest DAG. Your dbt DAG can later use
    # schedule=[BRONZE_ASSET] and this chain extends by one link without
    # either end knowing about the other.
    @task(outlets=[BRONZE_ASSET])
    def publish_bronze(report: list[dict]) -> None:
        log.info("Bronze refreshed: %s", report)

    # -----------------------------------------------------------------------
    # WIRING
    # -----------------------------------------------------------------------
    # Remember: this runs at parse time and builds a graph. No SQL executes.
    loaded = copy_into.expand(target=load_targets())
    publish_bronze(report=verify(loaded=loaded))


nyc_tlc_load_bronze()


# ===========================================================================
# SETUP NOTES  (not executed — reference material)
# ===========================================================================
#
# --- 1. Let Databricks read your bucket ---------------------------------
# Airflow's credentials do not carry over. Databricks needs its own path to
# S3, via Unity Catalog. Run once, as a metastore admin:
#
#   CREATE STORAGE CREDENTIAL nyc_transit_cred
#     WITH IAM ROLE 'arn:aws:iam::<account>:role/databricks-s3-access';
#
#   CREATE EXTERNAL LOCATION nyc_transit_landing
#     URL 's3://your-lakehouse-bucket/landing/'
#     WITH (STORAGE CREDENTIAL nyc_transit_cred);
#
#   GRANT READ FILES ON EXTERNAL LOCATION nyc_transit_landing TO `<your-user>`;
#
# The IAM role needs a trust policy allowing the Databricks account to assume
# it. Databricks' UI generates the exact policy JSON — use it rather than
# hand-writing one. If COPY INTO fails with a 403, this is where to look.
#
# --- 2. Target table DDL ------------------------------------------------
# Your tables need columns for the partition value and the lineage fields
# this DAG adds. If yellow_trips already exists without them:
#
#   ALTER TABLE nyc_transit.bronze.yellow_trips ADD COLUMNS (
#     period              STRING,
#     _source_file        STRING,
#     _source_modified_at TIMESTAMP,
#     _loaded_at          TIMESTAMP
#   );
#
# If you'd rather not add them, delete the three _metadata lines from the
# SELECT and add recursiveFileLookup to suppress partition inference:
#
#   FORMAT_OPTIONS ('mergeSchema' = 'true', 'recursiveFileLookup' = 'true')
#
# --- 3. Testing without waiting for an asset event ----------------------
# Asset-scheduled DAGs can still be run manually:
#
#   airflow dags test nyc_tlc_load_bronze 2026-07-29
#
# To confirm the asset wiring itself, check the Assets tab in the UI. It
# shows each asset, its producing task, and its consuming DAGs. If the loader
# isn't listed as a consumer, the two URI strings don't match.
#
# --- 4. Resetting a load during development -----------------------------
# COPY INTO remembers what it loaded. To reload from scratch:
#
#   TRUNCATE TABLE nyc_transit.bronze.yellow_trips;
#
# Truncate clears the ingested-files record along with the rows. DELETE FROM
# does not — the files stay marked as loaded and your next COPY INTO will
# find nothing to do.
