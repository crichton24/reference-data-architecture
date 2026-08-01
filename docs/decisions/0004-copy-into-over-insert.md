# 0004. Load raw with COPY INTO

Date: 2026-07-31
Status: Accepted

## Context

Parquet files land in S3 and must be loaded into existing Delta tables in
Databricks. The loader will be retried on failure and may be triggered more
than once for the same landing event, so it has to be safe to re-run.

TLC changes the schema between years — `cbd_congestion_fee` was added to
several datasets in 2025 — and has signalled further standardization.

## Decision

Load with `COPY INTO`, wrapping the source path in a `SELECT` so `_metadata`
lineage columns are captured alongside the data. Enable `mergeSchema` in both
`FORMAT_OPTIONS` and `COPY_OPTIONS`.

## Alternatives considered

**`INSERT INTO ... SELECT`.** No record of what it has already read. A retry
silently doubles rows. Idempotency would have to be rebuilt by hand in Airflow,
duplicating state that Delta already maintains correctly.

**Auto Loader (`cloudFiles`).** The right answer at high file volume, and where
this would go if arrival frequency increased. Overkill for roughly two files a
month, and on Databricks Free Edition serverless it can't use time-based
triggers anyway, so it would run in the same micro-batch mode COPY INTO already
provides — with more moving parts.

**`MERGE INTO`.** Solves a problem this layer doesn't have. Raw is
append-only by design; deduplication and correction belong in silver.

## Consequences

Idempotency comes free, so retries and duplicate triggers are safe.

`COPY INTO` tracks loaded files in the Delta log, which means `DELETE FROM`
does not reset a load — only `TRUNCATE TABLE` does. This surprises people
during development and is documented in the loader DAG.

Target tables must carry the partition column (`period`) and the three lineage
columns, or loads fail on schema mismatch.
