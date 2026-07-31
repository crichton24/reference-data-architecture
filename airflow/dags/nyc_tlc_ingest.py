"""
NYC TLC trip-record ingestion  (annotated edition)

WHAT THIS IS
    An Airflow DAG. A "DAG" (Directed Acyclic Graph) is Airflow's word for a
    workflow: a set of tasks plus the order they run in. "Acyclic" just means
    no loops — work flows forward and never circles back.

    Put this file in $AIRFLOW_HOME/dags/. Airflow finds it on its own. You
    never run it with `python nyc_tlc_ingest.py`.

THE MOST IMPORTANT THING TO UNDERSTAND
    Airflow reads this file in two completely different ways.

    1. PARSE TIME — every ~30 seconds, the scheduler imports this file to see
       what tasks exist. Everything at the outer indentation level runs:
       imports, constants, the decorators, and the final nyc_tlc_ingest()
       call at the bottom. The BODIES of the @task functions do NOT run.

    2. RUN TIME — when a task is actually due, a worker process runs that one
       function body.

    So: cheap stuff at the top, real work inside @task functions. If you put
    a slow API call at the outer level, you'd make it hundreds of times a day
    for no reason.

WHAT IT DOES
    TLC publishes monthly Parquet files roughly two months late, but the
    exact day drifts. So instead of guessing a release date, this DAG runs
    daily and asks "is it there yet?" for a rolling window of months. Files
    that have appeared and haven't been downloaded yet get pulled to S3.

WORKER IMAGE NEEDS
    apache-airflow-providers-amazon, requests, pyarrow
"""

# ---------------------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------------------
# Python convention (PEP 8) is three groups, separated by blank lines:
#   1. __future__ imports   2. standard library   3. third-party packages
# It's cosmetic, but every Python codebase you'll read follows it.

# This one is special. It changes how Python reads the type hints further
# down, so you can write modern syntax like `dict | None` even on older
# Python versions. Harmless to always include.
from __future__ import annotations

# Standard library — ships with Python, nothing to install.
import hashlib      # cryptographic hashes; used here to fingerprint a schema
import json         # convert between Python dicts and JSON text
import logging      # structured logging (better than print)
import tempfile     # scratch directories that clean themselves up
from pathlib import Path  # object-oriented file paths

# Third-party — installed via pip.
import pendulum     # datetime library Airflow uses; nicer than stdlib datetime
import requests     # the standard way to make HTTP calls in Python
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Airflow renamed Dataset -> Asset in version 3. This try/except keeps the DAG
# working on either. The pattern is worth knowing: attempt the modern import,
# fall back to the older one, alias both to the same local name.
try:
    from airflow.sdk import Asset          # Airflow 3.x
except ImportError:                        # pragma: no cover
    from airflow.datasets import Dataset as Asset  # Airflow 2.4+

# A logger named after this module. Anything you log here shows up in the
# Airflow UI under the task's Logs tab. Use this instead of print() — print
# output is easy to lose, log output is timestamped and searchable.
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# ALL_CAPS is Python's convention for "this is a constant, don't reassign it."
# Python won't actually stop you — it's a signal to human readers.
#
# Keeping config up here means changing behavior never requires touching
# logic below. That's a habit worth building early.

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Note the parentheses instead of square brackets: this is a TUPLE, not a
# list. Tuples can't be modified after creation. For a fixed configuration
# value that's exactly what you want — it can't be accidentally appended to
# somewhere deep in the code.
#
# Start narrow. fhvhv is ~400-500 MB/month; yellow is ~50 MB.
DATASETS = ("yellow", "green")

LAG_MONTHS = 2       # TLC's nominal publication delay
LOOKBACK_MONTHS = 8  # how many months back we re-check on every run

BUCKET = "your-lakehouse-bucket"
LANDING_PREFIX = "landing/nyc_tlc"

# An f-string. The `f` prefix lets you drop variables directly into a string
# inside curly braces, instead of gluing pieces together with +.
MANIFEST_KEY = f"{LANDING_PREFIX}/_manifest.json"

# The name of an Airflow Connection you set up in the UI (Admin > Connections).
# Credentials live there, encrypted — never hardcoded in a DAG file.
AWS_CONN_ID = "aws_default"

REQUEST_TIMEOUT = 60  # seconds; always set one, or a hung server hangs your task

# An Asset is just a named thing that DAGs can produce and consume. The string
# is only an identifier — Airflow never opens this URI. What matters is that
# the producer and every consumer spell it IDENTICALLY, character for
# character. A trailing slash mismatch is the classic reason a consumer DAG
# never fires.
LANDING_ASSET = Asset(f"s3://{BUCKET}/{LANDING_PREFIX}/")


def _s3() -> S3Hook:
    """Build an S3 client.

    Two things to notice:

    The leading underscore in `_s3` is a convention meaning "internal helper,
    not part of this module's public interface." Python doesn't enforce it.

    The `-> S3Hook` is a TYPE HINT: it documents that this function returns an
    S3Hook object. Python ignores type hints at runtime, but your editor uses
    them for autocomplete and error checking, which is genuinely useful.

    This is a function rather than a module-level variable on purpose. A
    module-level S3Hook() would be constructed at PARSE time, every 30
    seconds, forever. Calling it inside tasks builds one only when needed.
    """
    return S3Hook(aws_conn_id=AWS_CONN_ID)


# ---------------------------------------------------------------------------
# THE DAG
# ---------------------------------------------------------------------------
# @dag is a DECORATOR. A decorator is a function that wraps another function
# to add behavior. The @ line means "take the function defined below, hand it
# to dag(), and use whatever comes back."
#
# Practically: it turns an ordinary Python function into an Airflow workflow.
@dag(
    dag_id="nyc_tlc_ingest",      # unique name shown in the UI

    # Cron syntax: minute hour day-of-month month day-of-week.
    # "0 7 * * *" = 7:00 AM every day.
    schedule="0 7 * * *",

    # Airflow will not schedule any run before this moment.
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),

    # catchup=False means: on first deploy, don't retroactively run every day
    # since start_date. Leave this False unless you specifically want a
    # backfill — catchup=True on a daily DAG with an old start date will
    # instantly queue hundreds of runs.
    catchup=False,

    # Never let two runs of this DAG overlap. Without it, a slow run could
    # collide with the next one and both would fight over the manifest file.
    max_active_runs=1,

    # Applied to every task unless overridden: retry 3 times, 10 min apart.
    # Network calls fail transiently; retries turn a 3 AM page into a non-event.
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=10)},

    tags=["nyc-transit", "bronze", "batch"],  # UI filter labels
    doc_md=__doc__,  # __doc__ is the triple-quoted string at the top of this
                     # file; this renders it into the UI as documentation
)
def nyc_tlc_ingest():
    # Everything indented under here is INSIDE the DAG definition. Tasks are
    # defined as nested functions. This nesting looks unusual if you're new to
    # Python, but it's just a function containing other functions.

    # -----------------------------------------------------------------------
    # TASK 1: read what we've already ingested
    # -----------------------------------------------------------------------
    # @task turns a plain function into an Airflow task. Its return value is
    # automatically stored in "XCom" (cross-communication) — Airflow's small
    # key-value store — and passed to whatever task consumes it.
    #
    # XCom is meant for small metadata, not data payloads. Passing a dict of
    # filenames: fine. Passing a 50 MB dataframe: don't.
    @task
    def load_manifest() -> dict:
        """Prior ingest state, keyed by '<dataset>/<period>'."""
        hook = _s3()

        # `if not X:` reads as "if X is falsy." In Python, False, None, 0,
        # empty string, and empty list/dict are all falsy.
        if not hook.check_for_key(MANIFEST_KEY, bucket_name=BUCKET):
            return {}  # empty dict = first ever run, nothing ingested yet

        # json.loads turns JSON TEXT into a Python dict.
        # (loads = "load string". json.load, no s, reads from a file object.)
        return json.loads(hook.read_key(MANIFEST_KEY, bucket_name=BUCKET))

    # -----------------------------------------------------------------------
    # TASK 2: figure out which files we'd LIKE to have
    # -----------------------------------------------------------------------
    @task
    def candidate_files(data_interval_end=None) -> list[dict]:
        """Every (dataset, month) pair inside the rolling lookback window.

        `data_interval_end` is magic: Airflow inspects the parameter names of
        your task function and injects matching context values automatically.
        Here it's the end of the period this run covers. The `=None` default
        keeps the function callable in a plain test without Airflow.

        Return type `list[dict]` means "a list, where every element is a dict."
        """
        # Method chaining: each call returns a new pendulum datetime, so they
        # stack left to right. Read it as: take this run's timestamp, snap to
        # the first of that month, then step back LAG_MONTHS months.
        #
        # pendulum objects are IMMUTABLE — .subtract() returns a new object
        # rather than modifying the original. That prevents a whole category
        # of date bugs.
        anchor = (
            pendulum.instance(data_interval_end)
            .start_of("month")
            .subtract(months=LAG_MONTHS)
        )

        out = []  # accumulator: start empty, append as we go

        # range(8) produces 0,1,2,...,7 — eight values starting at zero.
        for offset in range(LOOKBACK_MONTHS):
            month = anchor.subtract(months=offset)
            period = month.format("YYYY-MM")  # e.g. "2026-03"

            # A nested loop: for each month, check each dataset. Eight months
            # times two datasets = 16 candidates per run.
            for dataset in DATASETS:
                filename = f"{dataset}_tripdata_{period}.parquet"

                # Building a dict literal. Curly braces with key: value pairs.
                # Dicts are Python's workhorse for passing structured data
                # around when you don't want to define a formal class.
                out.append(
                    {
                        "key": f"{dataset}/{period}",  # our unique identifier
                        "dataset": dataset,
                        "period": period,
                        "filename": filename,
                        "url": f"{BASE_URL}/{filename}",
                    }
                )
        return out

    # -----------------------------------------------------------------------
    # TASK 3: ask the server which of those actually exist
    # -----------------------------------------------------------------------
    @task
    def probe(candidate: dict, manifest: dict) -> dict | None:
        """HEAD the file. Return it only if published and new or restated.

        `dict | None` is a UNION type hint: this returns either a dict or the
        value None. None is Python's "nothing here" — like NULL in SQL.
        """
        # An HTTP HEAD request asks only for the headers, not the body. It's
        # how you check "does this file exist and how big is it" without
        # downloading 50 MB to find out.
        resp = requests.head(
            candidate["url"], timeout=REQUEST_TIMEOUT, allow_redirects=True
        )

        # 404 means TLC hasn't published this month yet. That's expected and
        # normal, not a failure — so we return None rather than raising.
        if resp.status_code == 404:
            # Note the %s and the comma, rather than an f-string. The logging
            # module only builds the final message if this log level is
            # actually enabled. Minor here, meaningful in a hot loop.
            log.info("Not published yet: %s", candidate["filename"])
            return None

        # Any OTHER bad status (500, 403...) is a real problem. raise_for_status
        # throws an exception, which fails the task, which triggers a retry.
        resp.raise_for_status()

        # An ETag is a fingerprint of the file's contents that S3 and most web
        # servers provide. If the ETag is unchanged, the file is unchanged.
        # This is what makes the DAG idempotent — safe to run repeatedly.
        #
        # `or ""` handles a missing header: if .get() returns None, use an
        # empty string instead so .strip() doesn't crash.
        # .strip('"') removes the literal quote marks servers wrap ETags in.
        etag = (resp.headers.get("ETag") or "").strip('"')

        # .get() looks up a key and returns None if it's absent, instead of
        # raising KeyError the way manifest[key] would.
        prior = manifest.get(candidate["key"])

        # Already have this exact file. Nothing to do.
        if prior and prior.get("etag") == etag:
            return None

        # We have a record, but the fingerprint changed — TLC restated the
        # month. Worth a warning so it's visible, then we re-download.
        if prior:
            log.warning(
                "Restated file detected for %s (etag %s -> %s)",
                candidate["key"], prior.get("etag"), etag,
            )

        # `**candidate` is DICTIONARY UNPACKING: it splats all of candidate's
        # key-value pairs into this new dict, then we add two more. It's the
        # concise way to say "a copy of that, plus these."
        return {
            **candidate,
            "etag": etag,
            "content_length": int(resp.headers.get("Content-Length", 0)),
        }

    # -----------------------------------------------------------------------
    # TASK 4: drop the Nones
    # -----------------------------------------------------------------------
    @task
    def to_fetch(probed: list[dict | None]) -> list[dict]:
        """Filter out the candidates that weren't available or were unchanged."""
        # A LIST COMPREHENSION. Read right to left: "for each p in probed, if p
        # is truthy, keep p." Equivalent to a for-loop with an if and an
        # append, but it's the idiomatic Python form and you'll see it
        # constantly.
        targets = [p for p in probed if p]
        log.info("%d file(s) to ingest this run", len(targets))
        return targets

    # -----------------------------------------------------------------------
    # TASK 5: actually download
    # -----------------------------------------------------------------------
    # max_active_tis_per_dag caps how many copies of THIS task run at once
    # ("tis" = task instances). Without it, sixteen simultaneous downloads
    # could saturate your connection or trip rate limiting.
    @task(max_active_tis_per_dag=3)
    def fetch(target: dict) -> dict:
        """Stream to a temp file, verify, fingerprint the schema, upload."""
        # Import inside the function, not at the top. pyarrow is a heavy
        # import, and remember the top of this file is re-executed every 30
        # seconds at parse time. Deferring it keeps parsing fast.
        import pyarrow.parquet as pq

        dest_key = (
            f"{LANDING_PREFIX}/dataset={target['dataset']}"
            f"/period={target['period']}/{target['filename']}"
        )
        # Two adjacent string literals with nothing between them are
        # automatically joined by Python. That's how the line above splits
        # across two lines without a + or a backslash.
        #
        # The dataset=/period= naming isn't decoration — it's Hive-style
        # partitioning. Spark reads those directory names as columns.

        # A CONTEXT MANAGER. `with` guarantees cleanup happens even if the
        # code inside raises an exception. Here, the temp directory and
        # everything in it is deleted on the way out, no matter what.
        with tempfile.TemporaryDirectory() as tmp:
            # Path objects overload the / operator to join paths. Reads
            # naturally and works on Windows and Linux alike.
            local = Path(tmp) / target["filename"]

            # stream=True means "don't load the whole response into memory."
            # Critical here: some of these files are hundreds of megabytes.
            with requests.get(
                target["url"], stream=True, timeout=REQUEST_TIMEOUT
            ) as r:
                r.raise_for_status()
                with local.open("wb") as fh:  # "wb" = write, binary mode
                    # Pull the file down 8 MB at a time and write each chunk
                    # straight to disk. Memory use stays flat regardless of
                    # file size.
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        fh.write(chunk)

            # Did we get the whole thing? A truncated download otherwise looks
            # like success and silently corrupts your bronze layer.
            size = local.stat().st_size
            if target["content_length"] and size != target["content_length"]:
                raise ValueError(
                    f"Size mismatch for {target['filename']}: "
                    f"expected {target['content_length']}, got {size}"
                )

            # Read only the Parquet file's schema (its column names), not its
            # data. Parquet stores schema in a footer, so this is nearly free
            # even on a huge file.
            #
            # sorted() so column ORDER changes don't look like schema changes.
            columns = sorted(pq.read_schema(local).names)

            # Reduce that column list to a short fingerprint so we can compare
            # months cheaply. .encode() converts text to bytes (hashing needs
            # bytes). [:16] is SLICING — keep the first 16 characters, plenty
            # for detecting a change.
            schema_hash = hashlib.sha256(",".join(columns).encode()).hexdigest()[:16]

            # Upload completes before the object becomes visible in S3, so a
            # reader never sees a partially written file.
            _s3().load_file(
                filename=str(local), key=dest_key, bucket_name=BUCKET, replace=True
            )
        # Dedent past the `with` — temp directory is now gone automatically.

        log.info("Landed s3://%s/%s (%.1f MB)", BUCKET, dest_key, size / 1024**2)

        return {
            **target,
            "s3_key": dest_key,
            "bytes": size,
            "columns": columns,
            "schema_hash": schema_hash,
            "ingested_at": pendulum.now("UTC").to_iso8601_string(),
        }

    # -----------------------------------------------------------------------
    # TASK 6: record what happened, flag schema surprises
    # -----------------------------------------------------------------------
    # trigger_rule matters here. The default is "all_success", which treats a
    # SKIPPED upstream task as a reason to skip. When TLC has published
    # nothing, `fetch` expands over an empty list and Airflow marks it
    # skipped — which would cascade and skip this task too. "none_failed"
    # says "run as long as nothing actually failed."
    @task(trigger_rule="none_failed")
    def update_manifest(manifest: dict, results: list[dict]) -> dict:
        """Persist ingest state and surface schema drift within a dataset."""
        # A dict whose values are SETS. A set holds unique items only, which
        # is exactly what we want for "which schema shapes have we seen."
        #
        # The type hint dict[str, set[str]] documents the shape: string keys
        # mapping to sets of strings.
        by_dataset: dict[str, set[str]] = {}

        # .values() iterates the dict's values, ignoring keys.
        for entry in manifest.values():
            # setdefault is a useful idiom: "give me the value at this key, and
            # if the key doesn't exist, create it with this default first."
            # Saves an if-statement on every loop.
            by_dataset.setdefault(entry["dataset"], set()).add(
                entry.get("schema_hash", "")
            )

        for r in results:
            known = by_dataset.get(r["dataset"], set())

            # `if known and ...` — the first clause matters. On the very first
            # run there's no history, so every schema is "new" and warning
            # would be noise. Only flag a change once we have a baseline.
            if known and r["schema_hash"] not in known:
                log.warning(
                    "Schema drift in %s at %s. Columns: %s",
                    r["dataset"], r["period"], r["columns"],
                )
            by_dataset.setdefault(r["dataset"], set()).add(r["schema_hash"])

            # Assign into the dict. Creates the key if absent, overwrites if
            # present — which is what we want for a restated file.
            manifest[r["key"]] = r

        _s3().load_string(
            # json.dumps is the reverse of json.loads: dict -> JSON text.
            # indent=2 and sort_keys=True make it readable and diffable if you
            # ever open it by hand.
            string_data=json.dumps(manifest, indent=2, sort_keys=True),
            key=MANIFEST_KEY,
            bucket_name=BUCKET,
            replace=True,
        )
        return manifest

    # -----------------------------------------------------------------------
    # WIRING THE TASKS TOGETHER
    # -----------------------------------------------------------------------
    # This is where it gets conceptually slippery, so slow down here.
    #
    # These lines run at PARSE time, and they do NOT execute your task logic.
    # Calling load_manifest() does not read S3. It returns a placeholder that
    # represents "the future output of that task." Passing that placeholder
    # into another task is what tells Airflow "this one depends on that one."
    #
    # You're describing a shape, not performing work.

    # -----------------------------------------------------------------------
    # TASK 7: announce that new data landed
    # -----------------------------------------------------------------------
    # `outlets` is the producer half of asset scheduling. When this task
    # SUCCEEDS, Airflow records an asset event, and any DAG scheduled on
    # LANDING_ASSET is queued immediately.
    #
    # The skip is the whole point. Most days TLC publishes nothing, and we
    # don't want to wake the downstream loader for zero files. A SKIPPED task
    # emits no asset event — only a successful one does.
    @task(outlets=[LANDING_ASSET], trigger_rule="none_failed")
    def publish_landing(results: list[dict]) -> None:
        """Emit an asset event, but only if we actually landed something."""
        if not results:
            # Raising this specific exception marks the task Skipped rather
            # than Failed. It's Airflow's way of saying "nothing to do here,"
            # which is a normal outcome, not an error.
            raise AirflowSkipException("No new files this run")

        log.info(
            "Landed %d file(s): %s",
            len(results),
            ", ".join(r["key"] for r in results),
        )

    manifest = load_manifest()

    # DYNAMIC TASK MAPPING — Airflow's version of a parallel for-loop.
    #
    # .expand() says: take this list and run one copy of the task per element.
    # We don't know the list length until runtime, and that's fine — Airflow
    # creates the task instances once candidate_files() actually returns.
    #
    # .partial() supplies arguments that are the SAME for every copy. Every
    # probe needs the manifest, but each gets a different candidate. So:
    # manifest goes in .partial(), candidate goes in .expand().
    probed = probe.partial(manifest=manifest).expand(candidate=candidate_files())

    # to_fetch(probed) collapses 16 results (mostly None) down to the few real
    # ones, then we expand again over just those. Two mapped stages in a row.
    landed = fetch.expand(target=to_fetch(probed))

    # update_manifest consumes `landed`, so Airflow knows it runs after fetch.
    recorded = update_manifest(manifest=manifest, results=landed)

    # publish_landing consumes `landed` too, but we also want it to run AFTER
    # the manifest is safely written — otherwise the loader could start while
    # our bookkeeping is still in flight. The >> operator sets an explicit
    # ordering dependency with no data passed between the tasks.
    recorded >> publish_landing(results=landed)


# ---------------------------------------------------------------------------
# INSTANTIATE
# ---------------------------------------------------------------------------
# Easy to miss and it costs people real time. The @dag decorator defines a
# FACTORY; this call actually produces the DAG object Airflow registers.
#
# Leave this line out and Airflow parses the file with no errors and shows you
# nothing in the UI. If a DAG mysteriously doesn't appear, check this first.
nyc_tlc_ingest()
