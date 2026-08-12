"""
NYC TLC taxi zone lookup — S3 to Databricks.

Stage 2 of 2. Rebuilds raw.nyc_tlc.taxi_zone_lookup from the CSV that
nyc_taxi_zone_source_to_s3 landed.

    nyc_taxi_zone_source_to_s3      TLC CDN --> S3
    [ THIS DAG ]                    S3 --> raw.nyc_tlc.taxi_zone_lookup

SCHEDULING
    Runs on ZONE_LANDED, which the upstream DAG emits only when the file
    actually changed. On an unchanged run the upstream task skips, no asset
    event fires, and this DAG stays asleep.

WHY FULL REPLACE, NOT COPY INTO
    The CSV is the complete current truth for a small reference table (~265
    rows). COPY INTO appends, so re-landing the same file would duplicate
    every zone. CREATE OR REPLACE has the right semantics in one statement,
    and is atomic in Delta — readers see the old table or the new one, never
    an empty window mid-rebuild.

    If zone history ever matters — a zone renamed, or reassigned to a
    different borough — this becomes a MERGE into an SCD2 table. It is not
    modelled that way today because TLC has never restated zones, and the
    complexity would be speculative.

RE-RUNNING SAFELY
    This DAG is idempotent and reads whatever is currently at CURRENT_KEY.
    If it fails, retry it directly — do NOT re-run the upstream DAG. The file
    is already landed and its hash already recorded, so the upstream would
    correctly report "unchanged" and skip.
"""

from __future__ import annotations

import json
import logging

import pendulum

import os #for environment variables specifically sql warehouse path


from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.sdk import dag, task

from assets import BUCKET, LANDING_PREFIX, ZONE_LANDED_ASSET, ZONE_LOOKUP_ASSET

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
REFERENCE_PREFIX = f"{LANDING_PREFIX}/reference/taxi_zone_lookup"
CURRENT_KEY = f"{REFERENCE_PREFIX}/taxi_zone_lookup.csv"
STATE_KEY = f"{REFERENCE_PREFIX}/_state.json"

TARGET_TABLE = "raw.nyc_tlc.taxi_zone_lookup"

AWS_CONN_ID = "aws_default"
DATABRICKS_CONN_ID = "databricks_default"

#SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/REPLACE_ME"
SQL_WAREHOUSE_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]

MIN_EXPECTED_ROWS = 200


def _s3() -> S3Hook:
    return S3Hook(aws_conn_id=AWS_CONN_ID)


def _sql_hook() -> DatabricksSqlHook:
    return DatabricksSqlHook(
        databricks_conn_id=DATABRICKS_CONN_ID,
        http_path=SQL_WAREHOUSE_HTTP_PATH,
    )


@dag(
    dag_id="nyc_tlc_ingest_taxi_zone_s3_to_databricks",
    schedule=[ZONE_LANDED_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["NYC TAXI AND LIMOUSINE COMMISSION", "RAW", "BATCH","PUBLIC","S3_TO_DATABRICKS", "NYC TRANSIT"],
    doc_md=__doc__,
)
def nyc_tlc_ingest_taxi_zone_s3_to_databricks():

    # -----------------------------------------------------------------------
    # TASK 1: read provenance
    # -----------------------------------------------------------------------
    @task
    def read_state() -> dict:
        """Pick up the source hash so it can be stamped onto the table.

        Deliberately non-fatal. The state file is the upstream DAG's
        bookkeeping, not this DAG's dependency — if it is missing or
        unreadable, the CSV in S3 is still the thing being loaded and the
        load should proceed. Losing a provenance tag is not worth failing a
        load over.
        """
        try:
            hook = _s3()
            if hook.check_for_key(STATE_KEY, bucket_name=BUCKET):
                state = json.loads(hook.read_key(STATE_KEY, bucket_name=BUCKET))
                log.info("Source sha %s, %s rows", state.get("sha256", "?")[:12],
                         state.get("rows"))
                return state
        except Exception as exc:  # noqa: BLE001 — provenance is best-effort
            log.warning("Could not read state file: %s", exc)

        return {}

    # -----------------------------------------------------------------------
    # TASK 2: rebuild the table
    # -----------------------------------------------------------------------
    @task
    def replace_table(state: dict) -> dict:
        """CREATE OR REPLACE from the CSV currently in S3."""
        sha = str(state.get("sha256", "unknown"))[:16]

        # LocationID is cast explicitly rather than left to inferSchema. A
        # single blank in the column would otherwise make the key a string
        # and every downstream join would silently return nothing.
        sql = f"""
            CREATE OR REPLACE TABLE {TARGET_TABLE} AS
            SELECT
                CAST(LocationID AS BIGINT)   AS locationid,
                Borough                      AS borough,
                Zone                         AS zone,
                service_zone,
                current_timestamp()          AS _loaded_at,
                '{sha}'                      AS _source_sha256
            FROM read_files(
                's3://{BUCKET}/{CURRENT_KEY}',
                format => 'csv',
                header => true,
                inferSchema => true
            )
        """

        log.info("Replacing %s from s3://%s/%s", TARGET_TABLE, BUCKET, CURRENT_KEY)
        _sql_hook().run(sql)

        return {"table": TARGET_TABLE, "source_sha256": sha}

    # -----------------------------------------------------------------------
    # TASK 3: verify
    # -----------------------------------------------------------------------
    @task
    def verify(loaded: dict) -> dict:
        """Confirm the table is populated.

        CREATE OR REPLACE against a path with no matching rows succeeds and
        produces an empty table. Without this check that failure is
        indistinguishable from success until something downstream breaks.
        """
        rows = _sql_hook().run(
            f"SELECT COUNT(*) FROM {loaded['table']}", handler=fetch_all_handler
        )
        count = rows[0][0] if rows else 0
        log.info("%s now has %s rows", loaded["table"], count)

        if count < MIN_EXPECTED_ROWS:
            raise ValueError(
                f"{loaded['table']} has only {count} rows after load, expected "
                f"at least {MIN_EXPECTED_ROWS}. Check that "
                f"s3://{BUCKET}/{CURRENT_KEY} exists and that the Unity Catalog "
                "external location grants READ FILES to this principal."
            )

        return {**loaded, "row_count": count}

    # -----------------------------------------------------------------------
    # TASK 4: announce
    # -----------------------------------------------------------------------
    # ZONE_LOOKUP_ASSET is what dbt's stg_taxi_zone_lookup depends on. Emitting it
    # only after verification means a downstream rebuild is never triggered
    # by an empty table.
    @task(outlets=[ZONE_LOOKUP_ASSET])
    def publish(report: dict) -> None:
        log.info("Zone lookup refreshed: %s", report)

    publish(report=verify(loaded=replace_table(state=read_state())))


nyc_tlc_ingest_taxi_zone_s3_to_databricks()
