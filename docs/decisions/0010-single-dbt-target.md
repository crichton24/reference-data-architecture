# 0010. A single dbt target

Date: 2026-08-12
Status: Accepted

## Context

A dev/uat/prod target framework was scaffolded, distinguishing environments by
schema prefix. Once the catalog structure changed to encode the medallion
layer (ADR 0009), the target name had nothing left to vary: models override
both catalog and schema explicitly, so a `dev` run and a `prod` run would
write to identical tables.

Databricks Free Edition provides one workspace.

## Decision

One target, `prod`. The profile's catalog and schema serve only as a fallback
for nodes that do not override them, and point at `transform` so an
unconfigured node lands somewhere harmless rather than in `formal`.

## Alternatives considered

**Catalog prefixes per environment** (`dev_formal`, `dev_transform`). The
complete answer, and the path if this ever needs environments. Rejected for
now: it triples the catalog count, requires a parallel copy of `raw` or a
cross-environment source reference, and Free Edition makes provisioning it
impractical.

**Keeping both targets unchanged.** Rejected as actively dangerous — a `dev`
run would silently write to production tables.

## Consequences

There is no isolated environment for testing model changes. Changes are
validated by `dbt parse` and `dbt compile` in CI, and by building single models
with `--select` before a full run.

The fallback catalog is `transform`, deliberately. A node that loses its explicit
configuration produces an unexpected table in a transform catalog rather than an
unreviewed object beside contracted data.

Reinstating environments means adding a target and a `generate_database_name`
macro. Model code would not change.
