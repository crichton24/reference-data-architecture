"""
NYC TLC taxi zone lookup — reference data refresh.

Downloads TLC's zone lookup CSV, detects whether it actually changed, and
replaces raw.nyc_tlc.taxi_zone_lookup only when it did.

SCHEDULING
    Runs on LANDING_ASSET — the same event that fires when nyc_tlc_ingest
    lands new trip files. Zones are only interesting when there's trip data
    referencing them, so checking on that cadence is right: no standalone
    cron, no daily poll of a file that changes maybe once a year.

WHY CONTENT HASH RATHER THAN ETAG
    The ingest DAG uses ETags because it tracks many large files it never
    reads. This DAG downloads one small CSV regardless, so hashing the actual
    bytes is both cheaper to reason about and more reliable — a CDN can
    change an ETag on re-upload without the content differing, which would
    cause a pointless table replace.

WHY FULL REPLACE, NOT COPY INTO
    This is a small reference table (~265 rows) where the CSV is the complete
    current truth. COPY INTO appends, so a re-download would duplicate every
    zone. CREATE OR REPLACE gives correct semantics in one statement.

    If zone history ever matters — a zone being renamed or reassigned to a
    different borough — this becomes a MERGE into an SCD2 table. It isn't
    modelled that way today because TLC has never restated zones and the
    added complexity would be speculative.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging

import pendulum
import requests
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

BUCKET = "nyc-tlc-raw-data-105803061132-us-east-2-an"
LANDING_PREFIX = "nyc-tlc"

# Must be byte-identical to the outlet in nyc_tlc_ingest.py.
LANDING_ASSET = Asset(f"s3://{BUCKET}/{LANDING_PREFIX}/")

# Emitted when the zone table actually changes, so dbt can rebuild anything
# that joins to it.
ZONE_ASSET = Asset("nyc-tlc://bronze/taxi_zone_lookup")

REFERENCE_PREFIX = f"{LANDING_PREFIX}/reference/taxi_zone_lookup"
CURRENT_KEY = f"{REFERENCE_PREFIX}/taxi_zone_lookup.csv"
STATE_KEY = f"{REFERENCE_PREFIX}/_state.json"

TARGET_TABLE = "raw.nyc_tlc.taxi_zone_lookup"

AWS_CONN_ID = "aws_s3_nyc_tlc"
DATABRICKS_CONN_ID = "databricks_default"
SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/8bf3f67b02373090"

REQUEST_TIMEOUT = 60

# TLC has shipped 265 zones for years. A sudden large drop means a truncated
# download or an upstream error, not a real change — better to fail than to
# replace a good table with a bad one.
MIN_EXPECTED_ROWS = 200
EXPECTED_HEADER = ["LocationID", "Borough", "Zone", "service_zone"]


def _s3() -> S3Hook:
    return S3Hook(aws_conn_id=AWS_CONN_ID)


def _sql_hook() -> DatabricksSqlHook:
    return DatabricksSqlHook(
        databricks_conn_id=DATABRICKS_CONN_ID,
        http_path=SQL_WAREHOUSE_HTTP_PATH,
    )


@dag(
    dag_id="nyc_tlc_ingest_taxi_zone_source_to_s3_to_databricks",
    schedule=[LANDING_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["NYC TAXI AND LIMOUSINE COMMISSION", "RAW", "BATCH","PUBLIC","S3_TO_DATABRICKS"],
    doc_md=__doc__,
)
def nyc_taxi_zone_lookup():

    @task
    def fetch_and_compare() -> dict:
        """Download the CSV, validate it, and compare against the last hash.

        Returns a dict with `changed` telling the next task whether there's
        anything to do. Validation happens BEFORE the comparison so a
        corrupt download can't be recorded as the new baseline.
        """
        resp = requests.get(LOOKUP_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        content = resp.content

        # --- validate before trusting ---
        text = content.decode("utf-8-sig")  # TLC ships a BOM
        rows = list(csv.reader(io.StringIO(text)))

        if not rows:
            raise ValueError("Downloaded zone lookup is empty")

        header = [h.strip() for h in rows[0]]
        if header != EXPECTED_HEADER:
            raise ValueError(
                f"Unexpected header {header!r}; expected {EXPECTED_HEADER!r}. "
                "TLC may have changed the file format — check before loading."
            )

        data_rows = len(rows) - 1
        if data_rows < MIN_EXPECTED_ROWS:
            raise ValueError(
                f"Only {data_rows} zone rows, expected at least "
                f"{MIN_EXPECTED_ROWS}. Refusing to replace the table."
            )

        # --- compare ---
        digest = hashlib.sha256(content).hexdigest()

        hook = _s3()
        prior_digest = None
        if hook.check_for_key(STATE_KEY, bucket_name=BUCKET):
            prior = json.loads(hook.read_key(STATE_KEY, bucket_name=BUCKET))
            prior_digest = prior.get("sha256")

        if prior_digest == digest:
            log.info("Zone lookup unchanged (%s rows, sha %s)", data_rows, digest[:12])
            return {"changed": False, "rows": data_rows, "sha256": digest}

        log.info(
            "Zone lookup changed: %s -> %s (%s rows)",
            (prior_digest or "none")[:12], digest[:12], data_rows,
        )

        # Keep a dated copy alongside the current one. Costs almost nothing
        # and means a bad upstream change is recoverable.
        stamp = pendulum.now("UTC").format("YYYYMMDDHHmmss")
        hook.load_bytes(
            bytes_data=content,
            key=f"{REFERENCE_PREFIX}/history/taxi_zone_lookup_{stamp}.csv",
            bucket_name=BUCKET,
            replace=True,
        )
        hook.load_bytes(
            bytes_data=content,
            key=CURRENT_KEY,
            bucket_name=BUCKET,
            replace=True,
        )

        return {
            "changed": True,
            "rows": data_rows,
            "sha256": digest,
            "prior_sha256": prior_digest,
            "archived_as": stamp,
        }

    @task(outlets=[ZONE_ASSET])
    def replace_table(result: dict) -> dict:
        """Rebuild the Databricks table from the CSV in S3.

        Skips when nothing changed. A skipped task emits no asset event, so
        downstream dbt isn't woken up to rebuild against identical data.
        """
        if not result["changed"]:
            raise AirflowSkipException("Zone lookup unchanged — nothing to load")

        # CREATE OR REPLACE is atomic in Delta: readers see either the old
        # table or the new one, never an empty window mid-rebuild.
        #
        # header/inferSchema are read options on the CSV source. LocationID is
        # cast explicitly rather than inferred, so a file with a stray blank
        # can't silently turn the key column into a string.
        sql = f"""
            CREATE OR REPLACE TABLE {TARGET_TABLE} AS
            SELECT
                CAST(LocationID AS BIGINT)   AS locationid,
                Borough                      AS borough,
                Zone                         AS zone,
                service_zone,
                current_timestamp()          AS _loaded_at,
                '{result["sha256"][:16]}'    AS _source_sha256
            FROM read_files(
                's3://{BUCKET}/{CURRENT_KEY}',
                format => 'csv',
                header => true,
                inferSchema => true
            )
        """

        log.info("Replacing %s from s3://%s/%s", TARGET_TABLE, BUCKET, CURRENT_KEY)
        _sql_hook().run(sql)

        rows = _sql_hook().run(
            f"SELECT COUNT(*) FROM {TARGET_TABLE}", handler=fetch_all_handler
        )
        loaded = rows[0][0] if rows else 0
        log.info("%s now has %s rows", TARGET_TABLE, loaded)

        if loaded < MIN_EXPECTED_ROWS:
            raise ValueError(
                f"{TARGET_TABLE} has only {loaded} rows after load. "
                "Check the S3 object and the external location grant."
            )

        return {**result, "loaded_rows": loaded}

    @task(trigger_rule="none_failed")
    def record_state(result: dict) -> None:
        """Persist the hash only after a successful load.

        Ordering matters. Recording the hash before the load would mean a
        failed load leaves the new hash on record, and the next run would see
        'unchanged' and never retry — the table would stay stale forever with
        no error anywhere.
        """
        if not result.get("changed"):
            log.info("No change, state untouched")
            return

        _s3().load_string(
            string_data=json.dumps(
                {
                    "sha256": result["sha256"],
                    "rows": result["rows"],
                    "loaded_rows": result.get("loaded_rows"),
                    "recorded_at": pendulum.now("UTC").to_iso8601_string(),
                },
                indent=2,
            ),
            key=STATE_KEY,
            bucket_name=BUCKET,
            replace=True,
        )
        log.info("State recorded: sha %s", result["sha256"][:12])

    checked = fetch_and_compare()
    loaded = replace_table(result=checked)
    record_state(result=loaded)


nyc_taxi_zone_lookup()
