# 0005. Chain DAGs with assets, not sensors

Date: 2026-07-31
Status: Accepted

## Context

The bronze loader must run after the ingest DAG lands files. Ingest runs daily
but produces files roughly once a month — on most days there is nothing to
load.

## Decision

Ingest declares an outlet asset on a final task that *skips itself when nothing
landed*. The loader schedules on that asset rather than on a cron expression.

## Alternatives considered

**`ExternalTaskSensor`.** Requires the two DAGs' schedules to align, and holds
a worker slot while waiting. Worse, it would fire on every successful ingest
run — including the 29 days a month that ingest correctly does nothing.

**`TriggerDagRunOperator`.** Works, but inverts the dependency: the producer
must name every consumer. Adding a second downstream consumer means editing the
upstream DAG, which is exactly the coupling this avoids.

**Merging both into one DAG.** Simplest, and defensible. Rejected because the
two have genuinely different failure modes and retry semantics — a Databricks
outage shouldn't force re-downloading files that already landed successfully.

## Consequences

The chain extends without either end knowing about the other. A dbt DAG can
schedule on the loader's `BRONZE_ASSET` with no change to anything upstream.

Asset identity is an exact string match. A trailing-slash mismatch between
producer and consumer silently breaks the chain with no error — the consumer
just never runs. The URIs are defined as constants and must be kept in sync.

Requires Airflow 2.4+ (Datasets) or 3.x (Assets). The DAGs use a compatibility
import to work on either.
