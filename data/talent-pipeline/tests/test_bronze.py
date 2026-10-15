"""Tests for the Bronze layer: incremental copy planning, Postgres snapshots, raw file copies."""

import dataclasses
import logging
from datetime import date

import pandas as pd
import pytest

import job

# --- Unit: copy planning (pure function, no I/O) -------------------------------

GOOD = "learning_activity/year=2024/month=09/day=03/events-000.jsonl"
BRONZE_GOOD = "bronze/learning_activity/year=2024/month=09/day=03/events-000.jsonl"


def obj(key, etag="a", size=10):
    return {"Key": key, "ETag": etag, "Size": size}


@pytest.mark.parametrize("key, ok", [
    (GOOD, True),
    ("learning_activity/year=2024/month=9/day=03/events-000.jsonl", False),   # month not zero-padded
    ("learning_activity/year=2024/month=09/events-000.jsonl", False),         # missing day=
    ("learning_activity/year=2024/month=09/day=03/events-000.json", False),   # wrong extension
    ("learning_activity/year=2024/month=09/day=03/sub/events.jsonl", False),  # too deep
    ("other/year=2024/month=09/day=03/events-000.jsonl", False),              # wrong root
])
def test_landing_key_contract(key, ok):
    assert bool(job.LANDING_LEARNING_KEY.match(key)) is ok


def test_plan_copies_new_files_to_mirrored_bronze_path():
    plan = job.plan_learning_copy([obj(GOOD)], [])
    assert plan.to_copy == [(GOOD, BRONZE_GOOD)] and plan.unchanged == 0


def test_plan_skips_identical_files():
    plan = job.plan_learning_copy([obj(GOOD)], [obj(BRONZE_GOOD)])
    assert plan.to_copy == [] and plan.unchanged == 1


@pytest.mark.parametrize("changed", [{"etag": "b"}, {"size": 11}])
def test_plan_flags_changed_source_as_conflict_not_overwrite(changed):
    plan = job.plan_learning_copy([obj(GOOD, **changed)], [obj(BRONZE_GOOD)])
    assert plan.conflicts == [GOOD] and plan.to_copy == []


def test_plan_rejects_bad_keys():
    bad = "learning_activity/stray.jsonl"
    plan = job.plan_learning_copy([obj(bad), obj(GOOD)], [])
    assert plan.bad_keys == [bad] and len(plan.to_copy) == 1


# --- Integration: real Postgres + LocalStack, scratch buckets ------------------


@pytest.fixture
def env(local_settings, make_bucket):
    """Settings pointing at a scratch landing bucket (3 files) and an empty scratch lake."""
    settings = dataclasses.replace(local_settings, landing_bucket=make_bucket("landing"),
                                   lake_bucket=make_bucket("lake"), run_date=date(2026, 10, 15))
    lake = job.Lake(settings)
    for day in ("01", "02", "03"):
        body = f'{{"event_id": "EV{day}", "employee_id": "1"}}\n{{"event_id": "EV{day}b", "employee_id": "2"}}\n'
        lake.s3.put_object(Bucket=settings.landing_bucket,
                           Key=f"learning_activity/year=2024/month=09/day={day}/events-000.jsonl", Body=body.encode())
    return settings, job.SourceDatabase(settings), lake


def run_bronze(settings, db, lake):
    ctx = job.RunContext(settings)
    job.ingest_bronze(ctx, db, lake)
    return ctx, ctx.steps[0].metrics


@pytest.mark.integration
def test_bronze_postgres_matches_source_with_lineage(env):
    settings, db, lake = env
    ctx, metrics = run_bronze(settings, db, lake)
    source = db.snapshot(job.SOURCE_TABLES)
    for table, src in source.items():
        assert metrics[f"{table}_rows"] == len(src)
        out = lake.read_parquet(settings.lake_bucket, f"{job.BRONZE_POSTGRES}/{table}")
        assert len(out) == len(src)
        # Source columns preserved, plus exactly the lineage columns + partition.
        assert set(out.columns) == set(src.columns) | {"_run_id", "_ingested_at", "_source", "ingest_date"}
        assert (out["_run_id"] == ctx.run_id).all()
        assert out["_source"].iloc[0].endswith(f"/hr.{table}")


@pytest.mark.integration
def test_bronze_keeps_source_types(env):
    settings, db, lake = env
    run_bronze(settings, db, lake)
    employees = lake.read_parquet(settings.lake_bucket, f"{job.BRONZE_POSTGRES}/employees")
    projects = lake.read_parquet(settings.lake_bucket, f"{job.BRONZE_POSTGRES}/projects")
    # INTEGER stays an integer (psycopg2 returns Python ints -> int64; values are exact).
    assert pd.api.types.is_integer_dtype(employees["employee_id"])
    assert type(employees["hire_date"].iloc[0]).__name__ == "date"  # DATE, not text
    assert type(projects["outcome_score"].iloc[0]).__name__ == "Decimal"  # NUMERIC(5,2) exact


@pytest.mark.integration
def test_bronze_files_are_byte_identical_with_lineage_metadata(env):
    settings, db, lake = env
    ctx, metrics = run_bronze(settings, db, lake)
    assert metrics["learning_files_copied"] == 3 and metrics["learning_files_verified"] == 3
    src = lake.s3.get_object(Bucket=settings.landing_bucket, Key=GOOD.replace("day=03", "day=01"))["Body"].read()
    dst = lake.s3.get_object(Bucket=settings.lake_bucket, Key=BRONZE_GOOD.replace("day=03", "day=01"))["Body"].read()
    assert src == dst
    meta = lake.head_metadata(settings.lake_bucket, BRONZE_GOOD)
    assert meta["run-id"] == ctx.run_id and meta["source"].startswith(f"s3://{settings.landing_bucket}/")


@pytest.mark.integration
def test_bronze_rerun_is_idempotent_and_incremental(env):
    settings, db, lake = env
    run_bronze(settings, db, lake)
    _, second = run_bronze(settings, db, lake)
    assert second["learning_files_copied"] == 0 and second["learning_files_unchanged"] == 3
    out = lake.read_parquet(settings.lake_bucket, f"{job.BRONZE_POSTGRES}/teams")
    assert len(out) == 100  # replaced, not appended
    assert len(lake.list_objects(settings.lake_bucket, f"{job.BRONZE_POSTGRES}/teams/")) == 1


@pytest.mark.integration
def test_bronze_new_run_date_adds_snapshot_and_keeps_history(env):
    settings, db, lake = env
    run_bronze(settings, db, lake)
    run_bronze(dataclasses.replace(settings, run_date=date(2026, 10, 16)), db, lake)
    keys = [o["Key"] for o in lake.list_objects(settings.lake_bucket, f"{job.BRONZE_POSTGRES}/teams/")]
    assert [k.split("/")[3] for k in sorted(keys)] == ["ingest_date=2026-10-15", "ingest_date=2026-10-16"]


@pytest.mark.integration
def test_bronze_changed_landing_file_is_not_overwritten(env):
    settings, db, lake = env
    run_bronze(settings, db, lake)
    original = lake.s3.get_object(Bucket=settings.lake_bucket, Key=BRONZE_GOOD)["Body"].read()
    lake.s3.put_object(Bucket=settings.landing_bucket, Key=GOOD, Body=b'{"tampered": true}\n')
    _, metrics = run_bronze(settings, db, lake)
    assert metrics["learning_files_conflicting"] == 1 and metrics["learning_files_copied"] == 0
    assert lake.s3.get_object(Bucket=settings.lake_bucket, Key=BRONZE_GOOD)["Body"].read() == original


@pytest.mark.integration
def test_bronze_skips_files_outside_partition_layout(env):
    settings, db, lake = env
    lake.s3.put_object(Bucket=settings.landing_bucket, Key="learning_activity/stray.jsonl", Body=b"{}\n")
    _, metrics = run_bronze(settings, db, lake)
    assert metrics["learning_files_bad_key"] == 1 and metrics["learning_files_copied"] == 3


@pytest.mark.integration
def test_bronze_warns_about_empty_landing_files_but_still_ingests_them(env, caplog):
    settings, db, lake = env
    empty_key = "learning_activity/year=2024/month=09/day=04/events-000.jsonl"
    lake.s3.put_object(Bucket=settings.landing_bucket, Key=empty_key, Body=b"")
    with caplog.at_level(logging.WARNING, logger="talent_pipeline"):
        ctx, metrics = run_bronze(settings, db, lake)
    assert metrics["learning_files_empty"] == 1
    assert metrics["learning_files_copied"] == 4 and metrics["learning_files_verified"] == 4  # still ingested
    copy = lake.list_objects(settings.lake_bucket, "bronze/" + empty_key)
    assert [o["Size"] for o in copy] == [0]                                                   # faithful 0-byte copy
    warnings = [r for r in caplog.records if r.getMessage().startswith("landing file is empty")]
    assert [r.fields["key"] for r in warnings] == [empty_key]
    assert ctx.steps[0].status == "succeeded"                                                 # warning, not failure


@pytest.mark.integration
def test_empty_file_with_bad_key_is_reported_once_as_bad_key(env):
    settings, db, lake = env
    lake.s3.put_object(Bucket=settings.landing_bucket, Key="learning_activity/stray.jsonl", Body=b"")
    _, metrics = run_bronze(settings, db, lake)
    assert metrics["learning_files_bad_key"] == 1 and metrics["learning_files_empty"] == 0
