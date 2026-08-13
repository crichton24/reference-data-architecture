# 0013. Enforce dbt contracts in `formal` only

Date: 2026-08-12
Status: Accepted

## Context

Datasets in `formal` are versioned and carry a published contract with
consumers. A versioning standard that relies on engineers remembering to
declare changes is a description of intent, not a control.

dbt contracts declare every column and data type in model YAML and fail the
build when the model produces something different.

## Decision

Set `contract: enforced: true` on all `formal` models. Do not set it in `raw`
or `transform`.

Governance and release-control metadata is applied as Unity Catalog tags in
the same model configuration.

## Alternatives considered

**Contracts everywhere.** Rejected. `raw` and `transform` exist to absorb
upstream change without breaking — TLC has added columns mid-year, and both
layers must accept that silently. Contract obligations there would defeat
their purpose and produce failures for events the design already anticipates.

**Contracts nowhere, tests only.** Tests run after the model is built, so bad
structure reaches `formal` before anything objects. A contract refuses to build
it.

## Consequences

Every column and type in `formal` must be declared. This is real up-front work
and the reason for limiting scope to two models.

`on_schema_change` must not be `append_new_columns` on a contracted model —
silently adding columns is exactly what the contract exists to prevent. Use
`fail`.

`not_null` constraints become Delta constraints enforced at write time, unlike
dbt tests which run afterward. Applied only where the model guarantees the
value; where the data is merely expected to be non-null, a test is safer.

On dbt-databricks 1.10, model-level `databricks_tags` replace project-level
tags rather than merging. The full tag set is repeated per model until 1.12
makes merging additive.
