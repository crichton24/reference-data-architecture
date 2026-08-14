"""
NYC TLC raw loader — yellow and green.

Loads Parquet from the S3 landing zone into raw.nyc_tlc.yellow_trips and
raw.nyc_tlc.green_trips.

INDEPENDENCE
    Each table gets its own task instance via dynamic task mapping. They are
    siblings, not a chain: if green fails, yellow still loads and still
    verifies. Nothing is duplicated — TABLES is the single source of truth and
    Airflow expands it into one task instance per entry at runtime.

    Adding fhvhv later is one line in TABLES.

SCHEDULING
    No time schedule. Runs when nyc_tlc_ingest's publish_landing task emits
    LANDING_ASSET, which it does only when files actually landed.

    The asset URI below MUST be byte-identical to the one in nyc_tlc_ingest.py.
    Airflow matches assets by exact string — a differing trailing slash means
    this DAG silently never runs, with no error anywhere.

WHY THE SOURCE PATHS ARE DIRECTORIES
    Each source points at dataset=<name>/, not at a single month's file.
    COPY INTO records which files it has already ingested in the Delta
    transaction log, so pointing at the directory is correct: it loads what's
    new and ignores what it has seen. Naming one file would mean editing this
    DAG every month.

PREREQUISITES
    1. Airflow connection `databricks_default`, and
       apache-airflow-providers-databricks installed.
    2. A SQL warehouse — put its HTTP path in SQL_WAREHOUSE_HTTP_PATH below.
    3. A Unity Catalog external location over the S3 prefix with READ FILES
       granted. Airflow's AWS credentials do NOT carry over to Databricks.
    4. Target tables carrying the partition and lineage columns. See TABLE
       REQUIREMENTS at the bottom — this is the most common first failure.
"""

from __future__ import annotations

import logging
import os  # for environment variables specifically sql warehouse path

import pendulum
from assets import BUCKET, LANDING_ASSET, LANDING_PREFIX, RAW_ALL_ASSETS

from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Must match nyc_tlc_ingest.py exactly — both the bucket and the prefix.
## NOW INGESTED FROM assets.py
# BUCKET = "nyc-tlc-raw-data-105803061132-us-east-2-an"
# LANDING_PREFIX = "nyc-tlc"

# The trigger. Identical string to the producer's outlet, or this never fires.
## NOW INGESTED FROM assets.py
# LANDING_ASSET = Asset(f"s3://{BUCKET}/{LANDING_PREFIX}/")

# THE SINGLE SOURCE OF TRUTH. One entry per table. Everything downstream —
# task count, source paths, verification — derives from this dict, so adding
# a dataset means adding one line and nothing else.
TABLES = {
    "yellow": "raw.nyc_tlc.yellow_trips",
    "green": "raw.nyc_tlc.green_trips",
    # "fhvhv": "raw.nyc_tlc.fhvhv_trips",   # ~450 MB/month, add deliberately
}

# What this DAG produces, so a dbt DAG can schedule on it later.
# RAW_ALL_ASSETS = [
#     Asset(f"databricks://raw/nyc_tlc/{table.split('.')[-1]}")
#     for table in TABLES.values()
# ]

## NOW INGESTED FROM assets.py
# RAW_ASSETS = [
#     Asset("nyc-tlc://raw/yellow"),
#     Asset("nyc-tlc://raw/green"),
# ]


DATABRICKS_CONN_ID = "databricks_default"

# SQL warehouse > Connection details tab. Looks like /sql/1.0/warehouses/abc123.
# SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/8bf3f67b02373090"
SQL_WAREHOUSE_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]

# Set False if the target tables have only TLC's own columns. Quickest way to
# get a first load working against tables you don't want to alter yet.
INCLUDE_LINEAGE_COLUMNS = True


def _sql_hook() -> DatabricksSqlHook:
    """One place to construct the Databricks SQL client."""
    return DatabricksSqlHook(
        databricks_conn_id=DATABRICKS_CONN_ID,
        http_path=SQL_WAREHOUSE_HTTP_PATH,
    )


@dag(
    dag_id="nyc_tlc_ingest_s3_to_databricks",
    # THE SCHEDULING ANSWER: a list of assets instead of a cron string. This
    # DAG is event-driven — it runs when LANDING_ASSET is updated.
    schedule=[LANDING_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=[
        "NYC TAXI AND LIMOUSINE COMMISSION",
        "RAW",
        "BATCH",
        "PUBLIC",
        "S3_TO_DATABRICKS",
    ],  # UI filter labels
    doc_md=__doc__,
)
def nyc_tlc_load_raw():
    # -----------------------------------------------------------------------
    # TASK 1: expand the config into work items
    # -----------------------------------------------------------------------
    @task
    def load_targets() -> list[dict]:
        """One work item per table.

        TABLES.items() yields (key, value) pairs; unpacking them into two loop
        variables is standard Python idiom. The list this returns is what
        Airflow maps over — its length determines how many parallel task
        instances get created.
        """
        return [
            {
                "dataset": dataset,
                "table": table,
                "source": f"s3://{BUCKET}/{LANDING_PREFIX}/dataset={dataset}/",
            }
            for dataset, table in TABLES.items()
        ]

    # -----------------------------------------------------------------------
    # TASK 2: COPY INTO  (one independent instance per table)
    # -----------------------------------------------------------------------
    # .expand() below creates one instance of this task per work item. They run
    # in parallel and fail independently — a green failure does not touch
    # yellow. That's the independence you want, with zero duplicated config.
    #
    # max_active_tis_per_dag caps concurrency ("tis" = task instances). With
    # two tables it's academic; it matters once fhvhv joins.
    @task(max_active_tis_per_dag=2)
    def copy_into(target: dict) -> dict:
        """Load any not-yet-ingested Parquet files into one table.

        Idempotent by construction. COPY INTO keeps a per-table record of
        ingested files in the Delta log, so re-running this — after a retry, a
        duplicate asset event, or a manual trigger — loads nothing twice.
        """
        if INCLUDE_LINEAGE_COLUMNS:
            # Wrapping the path in a SELECT lets us add lineage columns.
            # _metadata is a hidden column Databricks exposes on any file-based
            # read, carrying the source file's path and modification time.
            # Landing that in raw means any row traces back to its file.
            source_expr = f"""(
              SELECT
                *,
                _metadata.file_path              AS _loaded_from,
                current_timestamp()              AS _loaded_at,
                'ktf1234'                        AS _loaded_by,  -- TODO: replace with your Databricks user ID
                _metadata.file_modification_time AS _source_modified_at
              FROM '{target["source"]}'
            )"""
        else:
            source_expr = f"'{target['source']}'"

        # mergeSchema appears twice and does different jobs each time:
        #   FORMAT_OPTIONS -- reconcile schemas ACROSS the source files
        #   COPY_OPTIONS   -- allow new columns to be added to the TARGET table
        # Both are needed to survive TLC adding a column mid-year, as they did
        # with cbd_congestion_fee in 2025.
        sql = f"""
            COPY INTO {target["table"]}
            FROM {source_expr}
            FILEFORMAT = PARQUET
            FORMAT_OPTIONS ('mergeSchema' = 'true')
            COPY_OPTIONS  ('mergeSchema' = 'true')
        """

        log.info("COPY INTO %s FROM %s", target["table"], target["source"])

        # handler=fetch_all_handler makes the hook return result rows; without
        # it you get None. COPY INTO returns one row summarizing what it did:
        # files loaded, rows inserted, files skipped.
        rows = _sql_hook().run(sql, handler=fetch_all_handler)

        summary = str(rows[0]) if rows else "no result returned"
        log.info("%s -> %s", target["table"], summary)

        return {**target, "copy_result": summary}

    # -----------------------------------------------------------------------
    # TASK 3: sanity check  (also one independent instance per table)
    # -----------------------------------------------------------------------
    # Mapped as well, so verification stays independent too. Yellow's check
    # doesn't wait on green's load, and green's failure doesn't hide yellow's
    # row counts.
    @task
    def verify(loaded: dict) -> dict:
        """Count rows per period so the load is auditable.

        COPY INTO reports success when it matches zero files, which is correct
        behavior — but it means a wrong path looks identical to a legitimate
        no-op. This turns that silent case into a loud one.
        """
        hook = _sql_hook()
        table = loaded["table"]

        period_col = "period" if INCLUDE_LINEAGE_COLUMNS else "'all'"
        sql = f"""
            SELECT {period_col} AS period, COUNT(*) AS row_count
            FROM {table}
            GROUP BY {period_col}
            ORDER BY period DESC
            LIMIT 12
        """
        rows = hook.run(sql, handler=fetch_all_handler)

        for row in rows or []:
            log.info("%s | %s", table, row)

        if not rows:
            raise ValueError(
                f"{table} is empty after load. Check that {loaded['source']} "
                "contains .parquet files and that the Unity Catalog external "
                "location grants READ FILES to this principal."
            )

        return {"table": table, "periods": len(rows)}

    # -----------------------------------------------------------------------
    # TASK 4: announce raw is ready
    # -----------------------------------------------------------------------
    # This one is NOT mapped — it's a single task consuming all the mapped
    # results, because the asset represents "raw was refreshed" rather than
    # any individual table.
    #
    # trigger_rule matters for independence. The default (all_success) would
    # withhold the asset event if green failed, stalling downstream dbt even
    # though yellow loaded fine. none_failed_min_one_success runs when at
    # least one upstream succeeded and none outright failed.
    #
    # If you'd rather emit the asset whenever ANY table succeeds — letting dbt
    # proceed on partial data while green is broken — use "one_success"
    # instead. That's a real tradeoff: faster downstream, but marts may be
    # built from an incomplete raw.
    @task(outlets=RAW_ALL_ASSETS, trigger_rule="none_failed_min_one_success")
    def publish_raw(reports: list[dict]) -> None:
        log.info("Raw refreshed: %s", reports)

    # -----------------------------------------------------------------------
    # WIRING  (runs at parse time — builds the graph, executes no SQL)
    # -----------------------------------------------------------------------
    # Two mapped stages in sequence. Each expand() fans out over the list from
    # the previous step, so yellow and green travel through copy_into and
    # verify as entirely separate task instances.
    loaded = copy_into.expand(target=load_targets())
    verified = verify.expand(loaded=loaded)
    publish_raw(reports=verified)


nyc_tlc_load_raw()


# ===========================================================================
# SETUP NOTES  (reference only — not executed)
# ===========================================================================
#
# --- TABLE REQUIREMENTS -------------------------------------------------
# With INCLUDE_LINEAGE_COLUMNS = True, each target table needs four columns
# beyond TLC's own schema:
#
#   ALTER TABLE raw.nyc_tlc.yellow_trips ADD COLUMNS (
#     period              STRING,      -- from the dataset=/period= path
#     _source_file        STRING,
#     _source_modified_at TIMESTAMP,
#     _loaded_at          TIMESTAMP
#   );
#   -- repeat for green_trips
#
# `period` arrives via SELECT * because Databricks reads Hive-style directory
# names (period=2025-11) as columns automatically. Free partition metadata,
# but the column must exist in the target or the load fails on schema
# mismatch.
#
# If you'd rather not alter the tables yet, set INCLUDE_LINEAGE_COLUMNS =
# False and add recursiveFileLookup to suppress partition inference too:
#   FORMAT_OPTIONS ('mergeSchema' = 'true', 'recursiveFileLookup' = 'true')
#
# --- LET DATABRICKS READ THE BUCKET -------------------------------------
# Airflow's credentials do not carry over. Run once as a metastore admin:
#
#   CREATE STORAGE CREDENTIAL nyc_tlc_cred
#     WITH IAM ROLE 'arn:aws:iam::105803061132:role/databricks-s3-access';
#
#   CREATE EXTERNAL LOCATION nyc_tlc_landing
#     URL 's3://nyc-tlc-raw-data-105803061132-us-east-2-an/nyc-tlc/'
#     WITH (STORAGE CREDENTIAL nyc_tlc_cred);
#
#   GRANT READ FILES ON EXTERNAL LOCATION nyc_tlc_landing TO `<your-principal>`;
#
# Generate the IAM trust policy from the Databricks UI rather than writing it
# by hand. A 403 on COPY INTO traces back here every time.
#
# --- RESETTING DURING DEVELOPMENT ---------------------------------------
#   TRUNCATE TABLE raw.nyc_tlc.yellow_trips;
#
# TRUNCATE clears the ingested-files record along with the rows. DELETE FROM
# does NOT — files stay marked as loaded and the next COPY INTO finds nothing
# to do, which looks exactly like a broken DAG.
#
# --- RUNNING ONE TABLE IN ISOLATION -------------------------------------
# Mapped instances are addressable by index, in TABLES order (yellow=0):
#
#   airflow tasks test nyc_tlc_load_raw copy_into 2026-08-01 --map-index 0
