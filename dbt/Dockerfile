FROM python:3.11-slim

# git is needed by `dbt deps` to clone packages from the dbt hub.
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
      dbt-core==1.10.* \
      dbt-databricks==1.10.*

WORKDIR /dbt

# Deliberately NO `COPY . /dbt` and no build-time `dbt deps`.
#
# The compose file bind-mounts the project directory over /dbt at runtime,
# which would hide anything copied in here — including dbt_packages, leading
# to a confusing "package not found" for dbt_utils. Run `dbt deps` at runtime
# instead (it's a few seconds and only needed when packages.yml changes).

# Profiles live in the project directory rather than ~/.dbt so everything is
# under one mount.
ENV DBT_PROFILES_DIR=/dbt

ENTRYPOINT ["dbt"]
CMD ["--help"]
