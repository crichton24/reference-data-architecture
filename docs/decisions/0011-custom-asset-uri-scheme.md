# 0011. A custom asset URI scheme, defined centrally

Date: 2026-08-12
Status: Accepted

## Context

DAGs are chained through Airflow assets (ADR 0005). Airflow matches assets by
exact string comparison. A producer emitting one URI and a consumer listening
for another is not an error — the consumer simply never runs, silently, with
nothing in any log to explain it.

Two distinct problems appeared. Asset URIs drifted between files during
refactoring, on three separate occasions. And the Databricks provider
registers a validator for the `databricks://` scheme that rejects URIs not
matching `databricks://<host>/<catalog>/<schema>/<table>`, raising at DAG
parse time.

## Decision

Use a project-specific scheme, `nyc-transit://`, and define every asset once
in `airflow/dags/assets.py`. All DAGs import from there.

## Alternatives considered

**`databricks://` with a fully qualified host.** Satisfies the validator and
would enable Databricks lineage integration. Rejected because it bakes an
environment-specific hostname into asset identity, and remains exposed to
further tightening of provider validation.

**Per-DAG asset definitions.** The original approach. Rejected on evidence —
it drifted repeatedly, and the failure is silent.

## Consequences

Assets are identifiers only. Airflow never dereferences them, and no provider
validates them.

Producer and consumer cannot drift, because there is one definition.

A single bad line in `assets.py` fails every DAG that imports it, rather than
one. This is a real cost, accepted because the DagBag integrity test catches
import errors in CI before deployment.

A CI check asserts that no relation in `formal` is named with a version or
preview suffix; a similar check on asset consistency is worth adding.
