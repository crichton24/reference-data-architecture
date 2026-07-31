# Architecture

## Overview

Two ingestion paths converge on a Databricks lakehouse governed by Unity
Catalog. Batch data arrives monthly and irregularly; streaming data arrives
continuously. Both land as files in S3 before entering the lakehouse, so the
bronze layer has exactly one entry pattern regardless of source cadence.

## Layers

| Layer | Contents | Materialization | Owner |
|---|---|---|---|
| Landing | Source files, untouched, partitioned by dataset and period | S3 objects | Airflow |
| Bronze | Raw rows plus lineage columns, append-only | Delta tables | Airflow (COPY INTO) |
| Staging | Renamed, typed, one row per source row | Views | dbt |
| Marts | Dimensional models | Tables | dbt |
| Serving | Agent-facing tools over marts | UC functions | MCP |

Bronze is deliberately dumb. No deduplication, no filtering, no type coercion
beyond what Parquet already carries. Everything that could be wrong is a
downstream concern, which means a bad transformation is always fixable by
rebuilding from bronze rather than re-downloading from the source.

## Orchestration

DAGs chain through Airflow assets rather than sensors or explicit triggers
(ADR 0005):

```
nyc_tlc_ingest ──emits──► LANDING_ASSET ──triggers──► nyc_tlc_load_bronze
                                                              │
                                                          emits BRONZE_ASSET
                                                              │
                                                              ▼
                                                         dbt_transform
```

`nyc_tlc_ingest` runs daily on a cron schedule; everything downstream is
event-driven. Since TLC publishes roughly monthly, the downstream chain sits
idle most days rather than running to do nothing.

## Setup prerequisites

These are separate systems that authenticate independently. Granting one
access does not grant the others.

**AWS.** An S3 bucket in the same region as the Databricks workspace. An IAM
user for Airflow scoped to `GetObject`, `PutObject`, `ListBucket` on that
bucket only. A separate IAM role for Databricks to assume.

**Airflow.** Connections `aws_default` and `databricks_default`. Both are
supplied through environment variables in `docker-compose.yml` rather than the
UI, so local setup is reproducible from `.env`.

**Databricks.** A SQL warehouse. A Unity Catalog storage credential wrapping
the IAM role, an external location over `s3://<bucket>/landing/`, and a
`READ FILES` grant. Run `databricks/ddl/` in numeric order.

## Known gaps

- Streaming path not built. Kafka consumer and MTA GTFS-realtime decoding.
- No data quality gate between bronze and staging beyond dbt tests.
- Terraform describes intent but has not been applied — see `infra/README.md`.
- Unity Catalog grants are ad hoc rather than defined as code.
