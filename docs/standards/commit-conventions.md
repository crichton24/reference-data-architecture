# Commit Conventions

Standards for commit messages and commit scope in this repository.

## The principle

**A commit message explains why, not what.**

The diff already shows what changed — Git can produce that mechanically. What
Git cannot reconstruct is the reasoning: the constraint you were working
around, the option you rejected, the failure mode you were preventing. That
reasoning is what someone needs six months from now when they're deciding
whether a line is safe to remove.

Compare:

```
Update Dockerfile
```

against:

```
Pin provider installs to Airflow constraints file

Installing providers without --constraint lets pip re-resolve the base
image's dependency tree. It uninstalls apache-airflow and reinstalls a
partial version: libraries present, console script missing. Containers
start cleanly and `airflow` is simply not found, which makes this look
like a PATH problem rather than a build problem.
```

The second one stops the next person from "simplifying away" that URL.

## Format

```
<type>(<scope>): <subject>
                              <- blank line, required
<body: why this change, what it fixes, what was rejected>

<footer: issue refs, breaking changes, ADR links>
```

The blank line after the subject is structural, not cosmetic. Git treats
line one as the subject everywhere — `git log --oneline`, GitHub's commit
list, merge summaries. Omit the blank line and none of that tooling has
anything short to display.

### Subject line

| Rule | Reason |
|---|---|
| Under 50 characters | GitHub truncates around 72; short subjects stay readable in `--oneline` |
| Imperative mood | Matches Git's own generated messages ("Merge branch…", "Revert…") |
| No trailing period | It's a title, not a sentence |
| Lowercase after the colon | Consistency; the type prefix already provides visual structure |

The test for imperative mood: the subject should complete the sentence
*"If applied, this commit will ___."*

- `Add asset-driven trigger` — correct
- `Added asset-driven trigger` — past tense, wrong
- `Adds asset-driven trigger` — third person, wrong

### Body

Wrap at 72 characters. Explain:

- **Why** the change was needed
- **What** the previous behavior was, if it wasn't obvious
- **What alternatives** were rejected and why
- **What** a reader might otherwise be tempted to "fix"

Skip the body only when the subject genuinely says everything — typo fixes,
formatting, dependency bumps with no behavioral consequence.

## Types

| Type | Use for |
|---|---|
| `feat` | New capability |
| `fix` | Corrects broken behavior |
| `docs` | Documentation only |
| `refactor` | Restructuring with no behavior change |
| `test` | Adding or fixing tests |
| `chore` | Tooling, dependencies, CI, build config |
| `perf` | Performance improvement |
| `revert` | Reverts a previous commit (reference its hash in the body) |

## Scopes

This is a monorepo, so scope carries real weight — it makes
`git log --oneline | grep dbt` a useful filter rather than a shot in the dark.

| Scope | Covers |
|---|---|
| `airflow` | DAGs, plugins, Airflow image |
| `dbt` | Models, macros, tests, dbt image |
| `databricks` | DDL, notebooks, asset bundles |
| `infra` | Terraform, AWS resources |
| `docker` | Local development stack |
| `ci` | GitHub Actions, linting, pre-commit |
| `docs` | Architecture notes, ADRs |

Scope is optional but strongly preferred. Omit it only when a change
genuinely spans everything (a repo-wide rename, a license change).

## Examples

```
feat(airflow): trigger bronze loader on landing asset

Replaces the planned ExternalTaskSensor approach. The ingest DAG runs
daily but only lands files roughly monthly, so a sensor would fire on
every successful run and the loader would do nothing 29 days out of 30.

The publish task skips itself when no files landed, and a skipped task
emits no asset event — that's what keeps the downstream chain dormant.

See docs/decisions/0005-asset-driven-scheduling.md
```

```
fix(docker): pin provider installs to Airflow constraints

Unconstrained pip re-resolves the base image's dependency tree and
uninstalls apache-airflow, leaving libraries present but the console
script gone. Build succeeds, containers start, and `airflow` is not
found — the failure surfaces nowhere near its cause.

Also dropped version pins from requirements.txt, which conflicted with
the constraints file.
```

```
feat(dbt): add stg_yellow_trips

Coalesces cbd_congestion_fee to 0 — the column doesn't exist before
2025 and mergeSchema backfills it as NULL, which would silently drop
rows from any downstream sum.

No test on total_amount being positive: TLC data legitimately contains
negative totals for voided and refunded trips. Filtering belongs in
marts, not staging.
```

```
docs(adr): record COPY INTO decision
```

```
chore(ci): filter workflow jobs by changed path

Every push was running every check. dbt-only PRs no longer trigger
Terraform validation.
```

## Commit scope

One logical change per commit.

**The test:** could you revert this commit cleanly without breaking something
unrelated? If not, it's doing too much.

The Dockerfile fix and the requirements.txt change belong in one commit —
neither works without the other. A new dbt model and a CI tweak do not,
even if you happened to write them in the same sitting.

`git add -p` walks the working tree hunk by hunk, which is how you split a
messy set of changes into clean commits after the fact.

### Never commit a broken state

A commit that doesn't build poisons `git bisect` for everyone, including
future you. If a change genuinely requires several steps, either squash them
before merging or make each step independently valid.

### Never let these reach main

`wip` · `fix` · `updates` · `stuff` · `asdf` · `final fix for real this time`

These are the commits that turn `git log` from documentation into noise.
They're fine on a local branch; squash them before the PR merges.

## Footers

```
Refs: #42
Closes: #42
Reverts: a1b2c3d
BREAKING CHANGE: bronze tables now require a `period` column
```

`Closes: #42` auto-closes the issue on merge. `BREAKING CHANGE:` is
recognized by changelog tooling if this repo ever adopts it.

## Setup

### Commit template

Create `.gitmessage` in the repo root:

```
# <type>(<scope>): <subject>            <- 50 chars, imperative, no period
#
# Why this change? What was the previous behavior?
# What alternatives were rejected?
# Wrap at 72 characters.
#
# Refs:
#
# Types:  feat fix docs refactor test chore perf revert
# Scopes: airflow dbt databricks infra docker ci docs
```

Register it:

```bash
git config commit.template .gitmessage
```

Lines starting with `#` are stripped, so the guidance never lands in the
message. It pre-fills your editor on every `git commit`, which makes the
good format the path of least resistance.

### Editor, not -m

`git commit -m` encourages one-line messages because writing a body inline is
awkward. Bare `git commit` opens your editor with the template loaded.

```bash
git config core.editor "code --wait"   # or vim, nano, whatever
```

Keep `-m` for genuinely trivial commits.

## Why this matters here

This repository is a portfolio artifact. The commit history is public,
readable, and part of what a technical reviewer may skim.

A history of well-scoped commits with explanatory bodies signals engineering
discipline in a way that's difficult to fake retroactively. It's also the
cheapest documentation you will ever write — you're already typing something
into that field.

Where a commit implements a documented decision, link the ADR in the footer.
The two forms of documentation reinforce each other: the ADR carries the full
reasoning, the commit points at it from the exact line of code it produced.
