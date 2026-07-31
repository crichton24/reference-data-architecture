FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-core==1.8.* dbt-databricks==1.8.*

WORKDIR /dbt
COPY . /dbt
RUN dbt deps || true

ENTRYPOINT ["dbt"]
CMD ["--help"]
