# NYC Transit Lakehouse

An end-to-end data platform built on public NYC transportation data. Batch and
streaming ingestion, a governed lakehouse, dimensional models, and an agent-facing
semantic layer.

Built as a working reference implementation rather than a tutorial — the design
decisions are documented in [`docs/decisions/`](docs/decisions/) and are the most
useful thing in this repo.

## Architecture

```
NYC TLC trip records ──► Airflow ──► S3 landing ──┐
   (monthly parquet)      (batch)                 │
                                                  ├──► Databricks ──► dbt ──► marts ──► MCP
MTA GTFS-realtime ──────► Kafka ──► S3 landing ───┘   (bronze)      (silver/gold)      (agents)
   (30s protobuf)        (stream)
```

All governed by Unity Catalog. See [`docs/architecture.md`](docs/architecture.md).

## Data sources

| Source | Mode | Cadence | Auth |
|---|---|---|---|
| NYC TLC trip records | Batch | Monthly, ~2 month lag | None |
| MTA GTFS-realtime | Stream | ~30 seconds | None (subway feeds) |
| NYC Taxi Zone lookup | Batch | Rare | None |

## Repo layout

| Path | What lives here |
|---|---|
| `airflow/` | DAGs, plugins, and the Airflow image |
| `dbt/` | Models, tests, macros, and the dbt image |
| `databricks/` | Table DDL, Unity Catalog objects, Asset Bundle definitions |
| `infra/` | Terraform for S3, IAM, and UC storage credentials |
| `docker/` | Local development stack |
| `docs/` | Architecture notes and decision records |
| `ci/` | Config used only by CI (dummy dbt profile, lint rules) |

## Quickstart

```bash
cp .env.example .env          # then fill in your values
make up                       # start Airflow + Kafka + dbt locally
make test                     # DAG integrity + dbt parse + lint
```

Full setup, including the AWS and Databricks prerequisites, is in
[`docs/architecture.md`](docs/architecture.md).

## Contributing
Review git commit standards here:  `docs/standards/commit-conventions.md`

## Status

| Component | State |
|---|---|
| TLC batch ingestion | Working |
| Bronze load (COPY INTO) | Working |
| dbt staging models | In progress |
| MTA streaming ingestion | Not started |
| MCP server | Not started |

## License

MIT
