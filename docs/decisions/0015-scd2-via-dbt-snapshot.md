# 0015. SCD2 vendor history via a dbt snapshot

Date: 2026-08-12
Status: Accepted

## Context

Vendor identifiers appear in trip data; their names come from TLC's published
data dictionary. TLC has added vendors mid-year and renamed existing ones. The
requirement is to detect new vendor IDs automatically and preserve name
changes as history rather than overwriting them.

## Decision

A dbt snapshot, `dim_vendor_snapshot`, with `strategy='check'` and
`check_cols=['vendor_name']`, reading `int_vendors_observed` — a model joining
observed vendor IDs to a seeded name lookup with a `LEFT JOIN`.

`dim_vendor` reads the snapshot and presents validity windows in dimensional
conventions.

## Alternatives considered

**`strategy='timestamp'`.** Requires a trustworthy `updated_at` on the source.
The source is a derived lookup with no such column.

**`check_cols='all'`.** Would include `trip_count` and `last_seen_at`, which
change on every run — producing a new SCD version daily and a history that
records nothing.

**An incremental model with hand-written merge logic.** Reimplements what
snapshots already do correctly, including the edge cases.

**`INNER JOIN` to the seed.** Would drop trips whose vendor is not yet in the
lookup. Unmapped IDs instead surface as `Unknown vendor (n)` and are flagged
by `is_unmapped`, because silently losing rows is worse than an ugly label.

## Consequences

`invalidate_hard_deletes` is false. A vendor absent from recent trips has not
ceased to exist, and historical trips still reference it.

Snapshots must run on every refresh. A snapshot is the only object in the
project that is **not reproducible from source** — a rename occurring between
two runs is lost permanently if the snapshot did not run in between.

The snapshot is not part of dbt's model DAG, so dbt will not order
`int_vendors_observed` -> `snapshot` -> `dim_vendor`. Airflow enforces that
ordering explicitly, which is the reason `nyc_tlc_transform` is a sequence of
steps rather than a single `dbt build`.

`is_unmapped = true` is an action item: update the seed from TLC's current
data dictionary.
