# 0002. One repo per project, organized by tool

Date: 2026-07-31
Status: Accepted

## Context

The platform spans Airflow DAGs, dbt models, Databricks DDL and job
definitions, and Terraform. These could be split by tool (an "airflow-dags"
repo serving every project) or by project (this repo, containing every tool for
NYC transit).

Development is currently a single person. Airflow and dbt run in separate
Docker containers.

## Decision

One repository per project, with a top-level directory per tool.

## Alternatives considered

**One repo per tool.** The deciding case is the shape of a routine change.
Adding the `fhvhv` dataset touches the ingest DAG, the loader's table map, a
dbt source and staging model, and a table DDL. In one repo that is a single PR
with a single review and an atomic revert. Split by tool it becomes three PRs
with an undocumented ordering dependency and no way to roll back together.

The main argument for splitting by tool is decoupling team release cadences.
There is one team. That cost doesn't exist here, and the coupling is real —
these components are not independently useful.

**A single repo for all projects.** Rejected for the opposite reason: separate
projects genuinely are independent, and a shared repo would couple their CI,
their history, and their access control for no benefit.

## Consequences

Each tool needs to be told it doesn't own the repo root: dbt via its project
subdirectory setting, Databricks via job paths inside a Git folder, Airflow via
git-sync or a volume mount scoped to `airflow/dags/`.

CI must use path filters, or every push runs every check.

Genuinely shared code — a dbt package, a custom Airflow operator used by more
than one project — gets extracted to its own versioned repo *when a second
consumer actually appears*, not in anticipation.
