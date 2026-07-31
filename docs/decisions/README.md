# Architecture Decision Records

Short documents capturing decisions that were not obvious, along with the
options rejected and why.

The value is in the rejected options. Anyone reading the code can see what was
built; only these explain what was considered and discarded, which is the part
that's expensive to reconstruct later.

Format is [Michael Nygard's](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions.html),
kept deliberately short. One page each. If it needs more, it's probably two
decisions.

| # | Decision | Status |
|---|---|---|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-monorepo-per-project.md) | One repo per project, organized by tool | Accepted |
| [0003](0003-poll-tlc-rather-than-schedule.md) | Poll for TLC files rather than scheduling monthly | Accepted |
| [0004](0004-copy-into-over-insert.md) | Load bronze with COPY INTO | Accepted |
| [0005](0005-asset-driven-scheduling.md) | Chain DAGs with assets, not sensors | Accepted |

## Adding one

Copy `template.md`, take the next number, never edit an accepted record in
place. Decisions get superseded, not rewritten — a record that changes silently
loses the thing that made it worth keeping.
