"""Tests for lineage (dataset graph, declared Gold inputs) and run manifests."""

import dataclasses
import json
from datetime import date

import pandas as pd
import pytest

import job

SETTINGS = job.Settings.from_env({"PARTICIPANT_ID": "x"})


def test_every_lineage_input_is_a_known_dataset_or_external_source():
    lineage = job.dataset_lineage(SETTINGS)
    for target, inputs in lineage.items():
        for source in inputs:
            assert source in lineage or source.startswith(("postgres://", "s3://")), f"{target} <- {source}"


def test_lineage_graph_is_acyclic():
    lineage = job.dataset_lineage(SETTINGS)
    state: dict[str, str] = {}

    def visit(node):
        assert state.get(node) != "visiting", f"cycle through {node}"
        if state.get(node) == "done":
            return
        state[node] = "visiting"
        for parent in lineage.get(node, []):
            visit(parent)
        state[node] = "done"

    for node in lineage:
        visit(node)


def test_every_gold_table_traces_back_to_a_source_system():
    lineage = job.dataset_lineage(SETTINGS)

    def roots(node):
        if node not in lineage:
            return {node}
        return set().union(*(roots(p) for p in lineage[node]))

    for name in job.GOLD_TABLES:
        assert any(r.startswith(("postgres://", "s3://")) for r in roots(f"gold/{name}"))
    assert any(r.startswith("s3://") for r in roots("gold/employee_learning_summary"))  # events lineage


def test_gold_registry_order_respects_dependencies():
    built: set[str] = set()
    for name, (inputs, _) in job.GOLD_TABLES.items():
        for dep in inputs:
            assert dep.startswith("silver/") or dep.removeprefix("gold/") in built, f"{name} uses {dep} too early"
        built.add(name)
    assert set(job.GOLD_TABLES) == set(job.GOLD_GRAIN)


def test_builder_using_an_undeclared_input_fails(monkeypatch):
    # A builder that reaches for an input it didn't declare must crash, not silently work.
    tables = dict(job.GOLD_TABLES)
    tables["skill_distribution"] = (("gold/employee_skill_gaps",), lambda i, as_of: i["silver/teams"])
    monkeypatch.setattr(job, "GOLD_TABLES", tables)
    from test_gold import silver_fixture
    with pytest.raises(KeyError, match="silver/teams"):
        job.compute_gold(silver_fixture(), date(2026, 10, 15))


def test_mermaid_has_a_node_per_dataset_and_an_edge_per_dependency():
    lineage = job.dataset_lineage(SETTINGS)
    text = job.lineage_mermaid(SETTINGS)
    assert text.startswith("flowchart LR")
    assert text.count(" --> ") == sum(len(v) for v in lineage.values())
    assert '"gold/attrition_risk"' in text


def test_manifest_contents_and_no_secrets():
    settings = dataclasses.replace(SETTINGS, postgres_pass="s3cret-value")
    ctx = job.RunContext(settings)
    with ctx.step("bronze") as r:
        r.metrics["teams_rows"] = 100
    manifest = job.build_manifest(ctx, ["bronze"], 0)
    assert manifest["status"] == "succeeded" and manifest["steps"][0]["metrics"] == {"teams_rows": 100}
    assert len(manifest["code"]["sha256"]) == 64 and manifest["code"]["packages"]["pandas"] == pd.__version__
    assert "s3cret-value" not in json.dumps(manifest, default=str)


# --- Integration ----------------------------------------------------------------


@pytest.fixture
def lake_env(local_settings, make_bucket):
    settings = dataclasses.replace(local_settings, lake_bucket=make_bucket("lake"))
    return settings, job.Lake(settings)


def read_json(lake, bucket, key):
    return json.loads(lake.get_bytes(bucket, key))


@pytest.mark.integration
def test_latest_pointer_moves_only_on_complete_successful_runs(lake_env):
    settings, lake = lake_env

    def run(steps, fail=False):
        ctx = job.RunContext(settings)
        try:
            with ctx.step(steps[0]):
                if fail:
                    raise RuntimeError("boom")
        except RuntimeError:
            pass
        key = job.write_manifest(lake, ctx, job.build_manifest(ctx, steps, 1 if fail else 0))
        return ctx.run_id, key

    good_id, good_key = run(list(job.STEPS))
    assert read_json(lake, settings.lake_bucket, "_runs/latest.json")["run_id"] == good_id
    failed_id, failed_key = run(list(job.STEPS), fail=True)
    partial_id, _ = run(["silver"])
    assert read_json(lake, settings.lake_bucket, "_runs/latest.json")["run_id"] == good_id  # unchanged
    failed = read_json(lake, settings.lake_bucket, failed_key)
    assert failed["status"] == "failed" and failed["exit_code"] == 1 and "boom" in failed["steps"][0]["error"]
    assert good_key == f"_runs/run_date={settings.run_date.isoformat()}/{good_id}.json"


@pytest.mark.integration
def test_manifest_write_failure_is_logged_not_raised(local_settings, caplog):
    settings = dataclasses.replace(local_settings, lake_bucket="coding-workshop-does-not-exist-000")
    ctx = job.RunContext(settings)
    assert job.write_manifest(job.Lake(settings), ctx, job.build_manifest(ctx, ["gold"], 0)) is None
