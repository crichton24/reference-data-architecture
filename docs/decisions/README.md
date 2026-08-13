# Architecture Decision Records

Short documents capturing decisions that were not obvious, along with the
options rejected and why.

The value is in the rejected options. Anyone reading the code can see what was
built; only these explain what was considered and discarded, which is the part
that is expensive to reconstruct later.

Format is [Michael Nygard's](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions.html),
kept deliberately short. One page each. If it needs more, it is probably two
decisions.

| # | Decision | Status |
|---|---|---|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-monorepo-per-project.md) | One repo per project, organized by tool | Accepted |
| [0003](0003-poll-tlc-rather-than-schedule.md) | Poll for TLC files rather than scheduling monthly | Accepted |
| [0004](0004-copy-into-over-insert.md) | Load raw with COPY INTO | Accepted |
| [0005](0005-asset-driven-scheduling.md) | Chain DAGs with assets, not sensors | Accepted |
| [0006](0006-connections-as-environment-variables.md) | Connections as environment variables | Accepted |
| [0007](0007-credential-patterns-by-environment.md) | Credential patterns by environment | Accepted |
| [0008](0008-dbt-in-an-isolated-virtualenv.md) | Run dbt from an isolated virtualenv | Accepted |
| [0009](0009-medallion-catalogs-with-literal-schemas.md) | Medallion catalogs with literal schema names | Accepted |
| [0010](0010-single-dbt-target.md) | A single dbt target | Accepted |
| [0011](0011-custom-asset-uri-scheme.md) | A custom asset URI scheme, defined centrally | Accepted |
| [0012](0012-split-ingestion-at-the-storage-boundary.md) | Split ingestion at the storage boundary | Accepted |
| [0013](0013-contracts-enforced-in-formal-only.md) | Enforce dbt contracts in `formal` only | Accepted |
| [0014](0014-source-derived-schemas-in-raw.md) | Let the source define schemas in `raw` | Accepted |
| [0015](0015-scd2-via-dbt-snapshot.md) | SCD2 vendor history via a dbt snapshot | Accepted |

## Adding one

Copy `template.md`, take the next number, never edit an accepted record in
place. Decisions get superseded, not rewritten — a record that changes silently
loses the thing that made it worth keeping.
