"""Test environment setup.

pytest imports this before collecting tests, so anything set here is in
place before DagBag imports the DAG files.

Two DAGs read DATABRICKS_HTTP_PATH at module level rather than with .get().
That is deliberate — a missing warehouse path should fail loudly at parse
time instead of sending a placeholder to Databricks and getting a confusing
400. The cost is that importing them requires the variable to exist, so
tests supply a fake one. It is never used to connect.
"""

import os

os.environ.setdefault("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/test")
os.environ.setdefault("DATABRICKS_HOST", "https://test.invalid")
os.environ.setdefault("DATABRICKS_TOKEN", "not-a-real-token")
