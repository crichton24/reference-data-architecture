# 0014. Let the source define schemas in `raw`

Date: 2026-08-12
Status: Accepted

## Context

Bronze tables were initially created from hand-written DDL transcribing TLC's
published schema. Loading then failed repeatedly on type mismatches —
`TIMESTAMP` against `TIMESTAMP_NTZ`, `INT` against `BIGINT` — one column at a
time, each requiring an `ALTER TABLE` and another attempt.

Hand-written DDL for an external source is a standing bet that the types were
transcribed correctly, against a publisher who changes them between years.

## Decision

Create `raw` tables from the source files:

    CREATE TABLE raw.nyc_tlc.yellow_trips AS
    SELECT *, <lineage columns>
    FROM parquet.`s3://.../dataset=yellow/period=<a month>/`
    WHERE 1=0;

`COPY INTO` with `mergeSchema` then absorbs additive changes. Type discipline
happens in dbt transform models, where a cast is version-controlled, reviewed,
and testable.

## Alternatives considered

**Hand-written DDL.** Rejected on evidence. It produced a sequence of failures
whose only value was discovering what the source actually contained.

**Schema inference at load with no pre-created table.** Loses the ability to
add lineage columns and to grant on the table before first load.

## Consequences

`raw` schemas are whatever the source sent. That is the intent — `raw` is a
faithful record, not an opinion.

A `WHERE 1=0` create is required once per table, from a representative source
file. Choose a recent period so recently added columns are present.

Two related discoveries are documented in the README rather than here, because
they are environment quirks rather than decisions: Databricks promotes bare
`current_timestamp()` and `user()` select expressions into column DEFAULT
clauses, requiring `delta.feature.allowColumnDefaults`; and `COPY INTO` with a
`PATTERN` of `*.parquet` does not descend into partition directories.
