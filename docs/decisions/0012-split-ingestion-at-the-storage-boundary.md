# 0012. Split ingestion at the storage boundary

Date: 2026-08-12
Status: Accepted

## Context

Ingestion originally ran as one DAG per source: download from the origin,
land in S3, and load into Databricks. A failure anywhere produced one red
task in one DAG, and diagnosis meant determining which of three unrelated
systems was at fault.

The failure modes are genuinely different. A CDN 403 is not an S3 permission
problem is not a Unity Catalog grant problem, and the correct recovery differs
in each case.

## Decision

Split every ingestion path at the point where data lands in S3:

    nyc_tlc_ingest_source_to_s3            -> nyc_tlc_ingest_s3_to_databricks
    nyc_tlc_ingest_taxi_zone_source_to_s3  -> nyc_tlc_ingest_taxi_zone_s3_to_databricks

The upstream DAG's contract ends at "the object is in S3." The downstream DAG
reads S3 and owns everything from there. They are chained by an asset.

## Alternatives considered

**One DAG per source.** Simpler to read, fewer objects. Rejected — it conflates
failure domains, and a Databricks outage forces re-downloading files that
already landed correctly.

**One DAG with task groups.** Retains the diagnostic boundary within a single
graph but not the recovery boundary: a cleared task group still re-runs
upstream work.

## Consequences

Ingest state is recorded by the upstream DAG, after the S3 write succeeds.
The consequence is important and non-obvious: **if the Databricks load fails,
retry the downstream DAG — do not re-run the upstream one.** The file is
already landed and its hash recorded, so the upstream would correctly report
"unchanged" and skip. This is documented in both DAG docstrings.

Each DAG is independently testable with `airflow dags test`, which was the
motivating benefit.

More DAG objects to keep named consistently. The `*_source_to_s3` /
`*_s3_to_databricks` convention makes the boundary legible at a glance.
