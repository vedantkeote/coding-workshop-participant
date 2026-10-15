"""Both notebooks must run end-to-end against the lake the pipeline produces.

Guards the contract between the lake and its readers: rename a Gold column (insights) or a
lineage column / manifest field (lake explorer) and this fails here, not in front of a
stakeholder. Builds a fresh lake via main(), then executes every notebook cell with nbclient.
"""

import logging
import os
import uuid
from pathlib import Path

import nbformat
import pytest
from nbclient import NotebookClient

import job

pytestmark = pytest.mark.integration

NOTEBOOKS = Path(__file__).resolve().parents[1] / "notebooks"


@pytest.fixture(scope="module")
def lake_bucket():
    participant = os.environ.get("PARTICIPANT_ID")
    if not participant:
        pytest.skip("PARTICIPANT_ID not set (needed to find the seeded landing bucket)")
    bucket = f"coding-workshop-nb-{uuid.uuid4().hex[:8]}"
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level  # main() reconfigures logging
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("IS_LOCAL", "true")
        mp.setenv("DATALAKE_BUCKET", bucket)
        try:
            assert job.main([]) == 0
        finally:
            root.handlers[:] = saved_handlers
            root.setLevel(saved_level)
            job._LOG_CONTEXT.clear()
        yield bucket
        lake = job.Lake(job.Settings.from_env())
        for top in ("bronze/", "silver/", "gold/", "quarantine/", "_runs/"):
            lake.delete_prefix(bucket, top)
        lake.s3.delete_bucket(Bucket=bucket)


@pytest.mark.parametrize(("notebook", "charts"), [
    ("insights.ipynb", 8),        # 7 questions + anomalies
    ("lake_explorer.ipynb", 6),   # inventory, lineage, raw files, Silver events, quarantine, runs
])
def test_notebook_executes_without_errors(lake_bucket, monkeypatch, notebook, charts):
    monkeypatch.setenv("IS_LOCAL", "true")
    monkeypatch.setenv("DATALAKE_BUCKET", lake_bucket)  # the kernel inherits this environment
    nb = nbformat.read(NOTEBOOKS / notebook, as_version=4)
    NotebookClient(nb, timeout=300, kernel_name="python3", resources={"metadata": {"path": str(NOTEBOOKS)}}).execute()

    outputs = [o for c in nb.cells if c.cell_type == "code" for o in c.outputs]
    assert not [o for o in outputs if o.get("output_type") == "error"]
    assert sum("image/png" in o.get("data", {}) for o in outputs) == charts
    freshness = next(o["data"]["text/markdown"] for o in outputs if "text/markdown" in o.get("data", {}))
    assert lake_bucket in freshness  # it read the lake we pointed it at
