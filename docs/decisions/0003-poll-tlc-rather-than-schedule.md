# 0003. Poll for TLC files rather than scheduling monthly

Date: 2026-07-31
Status: Accepted

## Context

NYC TLC publishes trip records monthly with a nominal two-month lag, but the
actual release date drifts by days to weeks. There is no API, no webhook, and
no RSS feed. The only contract is a predictable URL.

TLC also occasionally restates a month after publishing it.

## Decision

Run the ingest DAG daily. Each run issues HTTP HEAD requests across a rolling
eight-month window of expected files, and downloads only what is both published
and not already ingested at its current ETag. A 404 is a normal outcome, not a
failure.

Ingest state lives in a manifest object in S3, keyed by dataset and period,
recording the ETag of every file loaded.

## Alternatives considered

**A monthly schedule with a two-month offset.** Assumes a release date that
TLC does not actually honor. Fails roughly as often as it succeeds, and each
failure needs manual intervention.

**A monthly schedule plus a retrying sensor.** Works, but a sensor polling for
up to three weeks occupies a worker slot for that whole period, and the failure
mode when the file never appears is a timeout rather than a clear signal.

**Tracking ingest state by filename alone.** Cheaper, but blind to
restatements. The ETag comparison catches a silently corrected month, which
would otherwise leave stale data in bronze indefinitely.

## Consequences

Roughly 16 HEAD requests per day against a CDN. Negligible.

The DAG usually does nothing, which is why downstream scheduling had to become
event-driven — see [0005](0005-asset-driven-scheduling.md).

The rolling window bounds how far back a restatement can be detected. Eight
months is a guess; widen it if a correction is ever missed.
