# 0008. Run dbt from an isolated virtualenv

Date: 2026-08-12
Status: Accepted

## Context

Airflow must invoke dbt. dbt-core and Airflow pin incompatible versions of
several shared dependencies, and installing dbt alongside Airflow in the same
environment is a well-known way to break the Airflow installation.

A related incident already occurred here: installing provider packages without
Airflow's constraints file caused pip to uninstall `apache-airflow` and leave
the image with libraries present but no `airflow` entrypoint. The container
started cleanly and failed only on first use.

## Decision

Install dbt into `/home/airflow/dbt-venv` in the Airflow image. DAGs invoke it
by absolute path through `BashOperator`. dbt is deliberately not on `PATH`.

Airflow providers continue to be installed against the version-matched
constraints file.

## Alternatives considered

**dbt in Airflow's environment.** Rejected — the failure mode above, on every
dbt or provider upgrade.

**Astronomer Cosmos.** Generates one Airflow task per dbt model, giving
per-model retries and lineage in the Airflow UI. Attractive, and the likely
future state. Deferred because it requires a manifest kept in sync with the
project, adds parsing cost, and does not solve the snapshot ordering problem
(ADR 0015) that motivated splitting the DAG in the first place. With six
models the added machinery outweighs the visibility gained.

**DockerOperator against the dbt container.** Clean separation, but requires
mounting the Docker socket into Airflow, which is awkward under WSL2 and
widens the trust boundary.

**Cosmos `ExecutionMode.VIRTUALENV`.** Builds and destroys a virtualenv per
task. With a task per model that overhead compounds, and teams have reported
disk exhaustion on managed Airflow.

## Consequences

The dbt version is pinned in the Airflow image and must be upgraded in step
with the standalone dbt container, or local and orchestrated runs diverge.

Failures surface as task-level, not model-level. A dbt run failure names the
layer, and the model must be identified from the log.

The `/dbt` project directory is mounted into the Airflow container as well as
the dbt container. Both read the same files, so a project change takes effect
in both without a rebuild.
