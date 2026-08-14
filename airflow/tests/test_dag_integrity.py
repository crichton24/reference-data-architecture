"""DAG integrity tests.

These run in CI on every PR touching airflow/. They import every DAG the same
way the scheduler does, which catches the failure modes that are otherwise
invisible until deploy: syntax errors, missing dependencies, cycles, and the
classic omitted instantiation call at the bottom of a TaskFlow DAG.

No scheduler, no database, no credentials required.
"""

from pathlib import Path

import pytest
from airflow.models import DagBag

DAG_DIR = Path(__file__).parent.parent / "dags"


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAG_DIR), include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    """Every file in dags/ imports cleanly."""
    assert not dagbag.import_errors, "DAG import failures:\n" + "\n".join(
        f"  {f}: {e}" for f, e in dagbag.import_errors.items()
    )


def test_dags_were_found(dagbag: DagBag) -> None:
    """Guards against a DAG file that parses but never instantiates.

    Without this, deleting the trailing `my_dag()` call passes every other
    check and simply produces an empty UI.
    """
    assert len(dagbag.dags) > 0, "No DAGs registered — check for a missing instantiation call"


def test_every_dag_is_tagged(dagbag: DagBag) -> None:
    """House rule: tags are how anyone finds anything once there are 40 DAGs."""
    untagged = [dag_id for dag_id, dag in dagbag.dags.items() if not dag.tags]
    assert not untagged, f"DAGs missing tags: {untagged}"


def test_every_dag_has_retries(dagbag: DagBag) -> None:
    """House rule: anything touching a network needs retries."""
    no_retries = [
        dag_id for dag_id, dag in dagbag.dags.items() if dag.default_args.get("retries", 0) < 1
    ]
    assert not no_retries, f"DAGs without retries: {no_retries}"


def test_every_dag_has_docs(dagbag: DagBag) -> None:
    """House rule: a DAG with no description is undocumented infrastructure."""
    undocumented = [
        dag_id for dag_id, dag in dagbag.dags.items() if not (dag.doc_md or dag.description)
    ]
    assert not undocumented, f"DAGs without documentation: {undocumented}"
