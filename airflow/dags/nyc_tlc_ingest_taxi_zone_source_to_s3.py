"""
NYC TLC taxi zone lookup — source to S3.

Stage 1 of 2. Downloads TLC's zone lookup CSV, validates it, and lands it in
S3 only when the content has actually changed.

    [ THIS DAG ]  TLC CDN --> S3
    nyc_taxi_zone_s3_to_databricks   S3 --> raw.nyc_tlc.taxi_zone_lookup

SCHEDULING
    Runs on LANDING — the asset nyc_tlc_ingest emits when new trip files
    arrive. Zones only matter when there is trip data referencing them, so
    checking on that cadence is right. A standalone cron would poll a file
    that changes maybe once a year.

WHY CONTENT HASH RATHER THAN ETAG
    The trip ingest DAG uses ETags because it tracks many large files it
    never opens. This DAG downloads one small CSV regardless, so hashing the
    bytes is both simpler to reason about and more truthful — a CDN can
    change an ETag on re-upload without the content differing, which would
    trigger a pointless table replace downstream.

WHY VALIDATE BEFORE COMPARING
    Ordering matters. If a truncated download were hashed and recorded first,
    the bad hash becomes the new baseline and the next run reports
    "unchanged" forever. Validation gates everything.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging

import pendulum
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException
from assets import BUCKET, LANDING_ASSET, LANDING_PREFIX, ZONE_LANDED_ASSET

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

REFERENCE_PREFIX = f"{LANDING_PREFIX}/reference/taxi_zone_lookup"
CURRENT_KEY = f"{REFERENCE_PREFIX}/taxi_zone_lookup.csv"
STATE_KEY = f"{REFERENCE_PREFIX}/_state.json"

AWS_CONN_ID = "aws_default"
REQUEST_TIMEOUT = 60

# TLC has shipped 265 zones for years. A sharp drop means a truncated
# download or an upstream error, not a real change — better to fail loudly
# than to overwrite a good file with a bad one.
MIN_EXPECTED_ROWS = 200
EXPECTED_HEADER = ["LocationID", "Borough", "Zone", "service_zone"]


def _s3() -> S3Hook:
    return S3Hook(aws_conn_id=AWS_CONN_ID)


@dag(
    dag_id="nyc_tlc_ingest_taxi_zone_source_to_s3",
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
        "SOURCE_TO_S3",
        "NYC TRANSIT",
    ],
    doc_md=__doc__,
)
def nyc_tlc_ingest_taxi_zone_source_to_s3():
    # -----------------------------------------------------------------------
    # TASK 1: download and validate
    # -----------------------------------------------------------------------
    @task
    def download() -> dict:
        """Fetch the CSV and prove it is well-formed before anything else.

        Returns the content as a hex-encoded string so it can travel through
        XCom. The file is ~12 KB, which is comfortably inside what XCom is
        meant to carry — do not copy this pattern for the trip parquet files.
        """
        resp = requests.get(LOOKUP_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        content = resp.content

        # TLC ships a UTF-8 BOM. utf-8-sig strips it; plain utf-8 would leave
        # a stray character on the first header name and fail the check below.
        text = content.decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text)))

        if not rows:
            raise ValueError("Downloaded zone lookup is empty")

        header = [h.strip() for h in rows[0]]
        if header != EXPECTED_HEADER:
            raise ValueError(
                f"Unexpected header {header!r}; expected {EXPECTED_HEADER!r}. "
                "TLC may have changed the file format — inspect before loading."
            )

        data_rows = len(rows) - 1
        if data_rows < MIN_EXPECTED_ROWS:
            raise ValueError(
                f"Only {data_rows} zone rows, expected at least "
                f"{MIN_EXPECTED_ROWS}. Refusing to land a suspect file."
            )

        digest = hashlib.sha256(content).hexdigest()
        log.info("Downloaded %s rows, sha %s", data_rows, digest[:12])

        return {
            "content_hex": content.hex(),
            "rows": data_rows,
            "sha256": digest,
        }

    # -----------------------------------------------------------------------
    # TASK 2: compare against what we last landed
    # -----------------------------------------------------------------------
    @task
    def compare(downloaded: dict) -> dict:
        """Decide whether this is new content.

        Separated from the download so a comparison problem is visibly
        distinct from a network problem — the whole reason for splitting
        these DAGs applies within them too.
        """
        hook = _s3()
        prior_digest = None

        if hook.check_for_key(STATE_KEY, bucket_name=BUCKET):
            prior = json.loads(hook.read_key(STATE_KEY, bucket_name=BUCKET))
            prior_digest = prior.get("sha256")

        changed = prior_digest != downloaded["sha256"]

        if changed:
            log.info(
                "Zone lookup changed: %s -> %s",
                (prior_digest or "none")[:12],
                downloaded["sha256"][:12],
            )
        else:
            log.info("Zone lookup unchanged (sha %s)", downloaded["sha256"][:12])

        return {**downloaded, "changed": changed, "prior_sha256": prior_digest}

    # -----------------------------------------------------------------------
    # TASK 3: land it
    # -----------------------------------------------------------------------
    # outlets fires ZONE_LANDED, which is what wakes the S3-to-Databricks DAG.
    # The skip is load-bearing: a SKIPPED task emits no asset event, so an
    # unchanged file leaves the downstream DAG asleep.
    @task(outlets=[ZONE_LANDED_ASSET])
    def land(result: dict) -> dict:
        """Write the current file plus a dated archive copy."""
        if not result["changed"]:
            raise AirflowSkipException("Zone lookup unchanged — nothing to land")

        content = bytes.fromhex(result["content_hex"])
        hook = _s3()

        # A dated copy costs almost nothing and makes a bad upstream change
        # recoverable without re-downloading from TLC (who do not publish
        # history).
        stamp = pendulum.now("UTC").format("YYYYMMDDHHmmss")
        archive_key = f"{REFERENCE_PREFIX}/history/taxi_zone_lookup_{stamp}.csv"

        hook.load_bytes(bytes_data=content, key=archive_key, bucket_name=BUCKET, replace=True)
        hook.load_bytes(bytes_data=content, key=CURRENT_KEY, bucket_name=BUCKET, replace=True)

        log.info("Landed s3://%s/%s (%s rows)", BUCKET, CURRENT_KEY, result["rows"])

        return {
            "sha256": result["sha256"],
            "rows": result["rows"],
            "prior_sha256": result["prior_sha256"],
            "archive_key": archive_key,
            "landed_at": pendulum.now("UTC").to_iso8601_string(),
        }

    # -----------------------------------------------------------------------
    # TASK 4: record state
    # -----------------------------------------------------------------------
    @task
    def record_state(landed: dict) -> None:
        """Persist the hash, only after the file is safely in S3.

        This DAG's contract ends at S3. If the downstream Databricks load
        fails, the fix is to retry THAT dag — not to re-download a file that
        is already correctly landed. Recording state here is what makes those
        two failure modes independently recoverable, which is the point of
        the split.
        """
        _s3().load_string(
            string_data=json.dumps(landed, indent=2, sort_keys=True),
            key=STATE_KEY,
            bucket_name=BUCKET,
            replace=True,
        )
        log.info("State recorded: sha %s", landed["sha256"][:12])

    record_state(landed=land(result=compare(downloaded=download())))


nyc_tlc_ingest_taxi_zone_source_to_s3()
