# 0001. Record architecture decisions

Date: 2026-07-31
Status: Accepted

## Context

This project spans four tools and a dozen non-obvious choices. Six months from
now the code will still be readable but the reasoning behind it won't be, and
the reasoning is what determines whether a future change is safe.

This repo also serves as a professional portfolio. Working pipelines are
table stakes; documented tradeoff reasoning is the differentiator.

## Decision

Record significant architectural decisions as numbered ADRs in
`docs/decisions/`, using Nygard's format. "Significant" means: it constrains
future work, or a competent engineer would reasonably have chosen otherwise.

## Alternatives considered

**Comments in code.** Good for local "why," useless for decisions spanning
files or tools. The COPY INTO choice touches a DAG, a table definition, and a
dbt source — no single file owns it.

**A wiki.** Drifts from the code because it isn't reviewed alongside it. ADRs
in the repo change in the same PR as the thing they describe.

**Nothing.** The default, and the reason most data platforms accumulate
decisions nobody can explain or safely reverse.

## Consequences

Small ongoing cost per decision. Records must be superseded rather than
edited, which means the folder grows monotonically — that's intentional.
