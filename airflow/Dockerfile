FROM apache/airflow:3.3.0-python3.11

ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.11

USER airflow

# --- Airflow providers, pinned to Airflow's own constraint set -------------
# Never install these unconstrained: pip will re-resolve the base image's
# dependency tree and can uninstall apache-airflow itself, leaving the
# container running but with no `airflow` entrypoint.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# --- dbt, deliberately in its own virtualenv ------------------------------
# dbt-core and Airflow disagree on several shared dependencies. Installing
# dbt into the same environment is the single most common way to break an
# Airflow image. A venv gives dbt its own dependency tree with zero overlap;
# the DAG invokes it by absolute path, so nothing needs to be on PATH.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
      dbt-core==1.10.* \
      dbt-databricks==1.10.*

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*
USER airflow
