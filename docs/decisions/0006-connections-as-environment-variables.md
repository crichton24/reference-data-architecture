# 0006. Connections as environment variables

Date: 2026-08-12
Status: Accepted

## Context

Airflow resolves a connection ID through a chain: environment variable, then
secrets backend, then metadata database. The local stack needs `aws_default`
and `databricks_default` available to every DAG, reproducibly, on any machine
that clones this repo.

## Decision

Define connections as `AIRFLOW_CONN_*` environment variables in
`docker/docker-compose.yml`, interpolated from `.env`. Nothing is entered in
the Airflow UI.

## Alternatives considered

**UI-defined connections.** Discoverable — Admin > Connections shows them, with
a test button. Rejected because setup becomes a list of manual steps that
people get wrong, and CI has no human to click them. Credentials would also
live in the metadata database, so database backups would contain secrets.

**A secrets backend (AWS Secrets Manager, Databricks scopes).** The right
production answer, and the intended destination. Not adopted yet because it
adds an external dependency to a local development stack.

## Consequences

Admin > Connections appears empty. This surprises people and reads as
"unconfigured." Documented in the README, with the inspection command:

    airflow connections get aws_default

Note `airflow connections list` reads only the database and will not show
these — `get` resolves the full chain.

The discoverability cost is not unique to environment variables. A secrets
backend is equally invisible in the UI, so solving discoverability by moving
to the UI now would mean giving it up again later. Documentation is the
solution that survives the migration.
