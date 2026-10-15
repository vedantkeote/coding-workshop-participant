"""Tests for job.py sections 1-5: configuration, logging, run context, retry, I/O."""

import json
import logging
import uuid
from datetime import date

import pandas as pd
import psycopg2
import pytest

import job

# --- 1. Configuration --------------------------------------------------------


def test_local_settings_use_infra_defaults():
    s = job.Settings.from_env({"PARTICIPANT_ID": "abc123"})
    assert s.is_local is True
    assert (s.postgres_host, s.postgres_port, s.postgres_name, s.postgres_user) == ("localhost", 5432, "postgres", "postgres")
    assert s.landing_bucket == "coding-workshop-landing-abc123"
    assert s.lake_bucket == "coding-workshop-datalake-abc123"
    assert s.aws_endpoint_url == job.LOCAL_AWS_ENDPOINT


def test_empty_values_count_as_unset():
    # backend/README.md: locally POSTGRES_NAME/USER/PASS are injected as empty strings.
    s = job.Settings.from_env({"PARTICIPANT_ID": "x", "POSTGRES_NAME": "", "POSTGRES_USER": "  "})
    assert s.postgres_name == "postgres" and s.postgres_user == "postgres"


def test_cloud_mode_requires_explicit_settings():
    with pytest.raises(job.ConfigError) as err:
        job.Settings.from_env({"IS_LOCAL": "false"})
    for name in ("POSTGRES_HOST", "POSTGRES_PASS", "LANDING_BUCKET", "DATALAKE_BUCKET", "AWS_REGION"):
        assert name in str(err.value)


def test_cloud_mode_has_no_local_endpoint():
    env = {"IS_LOCAL": "false", "POSTGRES_HOST": "db", "POSTGRES_NAME": "n", "POSTGRES_USER": "u",
           "POSTGRES_PASS": "p", "PARTICIPANT_ID": "x", "AWS_REGION": "ap-south-1"}
    s = job.Settings.from_env(env)
    assert s.aws_endpoint_url is None and s.aws_region == "ap-south-1"


def test_run_date_override_and_validation():
    assert job.Settings.from_env({"PARTICIPANT_ID": "x", "RUN_DATE": "2026-01-31"}).run_date == date(2026, 1, 31)
    with pytest.raises(job.ConfigError, match="RUN_DATE"):
        job.Settings.from_env({"PARTICIPANT_ID": "x", "RUN_DATE": "31/01/2026"})


def test_bad_port_is_a_config_error():
    with pytest.raises(job.ConfigError, match="POSTGRES_PORT"):
        job.Settings.from_env({"PARTICIPANT_ID": "x", "POSTGRES_PORT": "abc"})


def test_password_never_in_repr():
    s = job.Settings.from_env({"PARTICIPANT_ID": "x", "POSTGRES_PASS": "s3cret"})
    assert "s3cret" not in repr(s)


# --- 2. Logging --------------------------------------------------------------


def test_json_formatter_includes_context_and_fields(monkeypatch):
    monkeypatch.setitem(job._LOG_CONTEXT, "run_id", "r-1")
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "hello", None, None)
    record.fields = {"rows": 3}
    line = json.loads(job.JsonFormatter().format(record))
    assert line["msg"] == "hello" and line["level"] == "INFO"
    assert line["run_id"] == "r-1" and line["rows"] == 3


# --- 3. Run context ----------------------------------------------------------


@pytest.fixture
def ctx():
    return job.RunContext(job.Settings.from_env({"PARTICIPANT_ID": "x"}))


def test_run_id_is_time_sortable_and_unique():
    ids = {job.new_run_id() for _ in range(50)}
    assert len(ids) == 50
    assert all(len(i) == len("20261015T091500Z-1a2b3c4d") for i in ids)


def test_step_records_success_and_metrics(ctx):
    with ctx.step("demo") as result:
        result.metrics["rows_written"] = 7
    step = ctx.steps[0]
    assert step.status == "succeeded" and step.metrics == {"rows_written": 7}
    assert step.duration_s is not None and step.finished_at is not None
    assert ctx.summary()["status"] == "succeeded"


def test_step_records_failure_and_reraises(ctx):
    with pytest.raises(ValueError):
        with ctx.step("boom"):
            raise ValueError("bad data")
    assert ctx.steps[0].status == "failed"
    assert ctx.steps[0].error == "ValueError: bad data"
    assert ctx.summary()["status"] == "failed"


# --- 4. Retry ----------------------------------------------------------------


def test_retry_succeeds_after_transient_failures():
    calls, sleeps = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("blip")
        return "ok"

    assert job.with_retry(flaky, what="t", retry_on=(ConnectionError,), sleep=sleeps.append) == "ok"
    assert len(calls) == 3 and len(sleeps) == 2
    assert 0 <= sleeps[0] <= 0.5 and 0 <= sleeps[1] <= 1.0  # backoff caps: 0.5 * 2**(n-1)


def test_retry_gives_up_after_max_attempts():
    sleeps = []
    with pytest.raises(ConnectionError):
        job.with_retry(lambda: (_ for _ in ()).throw(ConnectionError("down")),
                       what="t", retry_on=(ConnectionError,), attempts=4, sleep=sleeps.append)
    assert len(sleeps) == 3  # 4 attempts -> 3 waits


def test_retry_does_not_retry_permanent_errors():
    calls = []

    def bad_password():
        calls.append(1)
        raise PermissionError("auth failed")

    with pytest.raises(PermissionError):
        job.with_retry(bad_password, what="t", retry_on=(ConnectionError,), sleep=lambda _: None)
    assert len(calls) == 1


def test_backoff_never_exceeds_max_delay():
    sleeps = []
    with pytest.raises(ConnectionError):
        job.with_retry(lambda: (_ for _ in ()).throw(ConnectionError()), what="t", retry_on=(ConnectionError,),
                       attempts=10, max_delay=2.0, sleep=sleeps.append)
    assert max(sleeps) <= 2.0


def test_postgres_connect_retries_then_raises(monkeypatch):
    attempts = []

    def refuse(**kwargs):
        attempts.append(kwargs)
        raise psycopg2.OperationalError("connection refused")

    monkeypatch.setattr(job.psycopg2, "connect", refuse)
    monkeypatch.setattr(job.time, "sleep", lambda _: None)
    db = job.SourceDatabase(job.Settings.from_env({"PARTICIPANT_ID": "x"}))
    with pytest.raises(psycopg2.OperationalError):
        db.connect()
    assert len(attempts) == 5
    assert attempts[0]["sslmode"] == "prefer"


def test_cli_rejects_unknown_steps():
    with pytest.raises(SystemExit):
        job.parse_args(["--steps", "bronze,platinum"])
    assert job.parse_args(["--steps", "silver, gold"]).steps == ["silver", "gold"]


# --- 5. I/O against real local services --------------------------------------


@pytest.mark.integration
def test_postgres_source_is_readable(local_settings):
    db = job.SourceDatabase(local_settings)
    db.ping()
    teams = db.read_table("teams")
    assert list(teams.columns) == ["team_id", "name", "department"] and len(teams) == 100


@pytest.mark.integration
def test_parquet_rewrite_is_idempotent(local_settings, make_bucket):
    lake, bucket = job.Lake(local_settings), make_bucket()
    df = pd.DataFrame({"year": [2024, 2024, 2025], "v": [1, 2, 3]})
    for _ in range(2):  # same write twice must not duplicate rows
        assert lake.write_parquet(df, bucket, "t/", partition_cols=["year"]) == 3
    assert len(lake.read_parquet(bucket, "t")) == 3

    lake.write_parquet(pd.DataFrame({"year": [2025], "v": [9]}), bucket, "t", partition_cols=["year"])
    out = lake.read_parquet(bucket, "t").sort_values("v")
    assert out["v"].tolist() == [1, 2, 9]  # only year=2025 replaced; 2024 untouched
    assert len(lake.list_objects(bucket, "t/")) == 2  # one file per partition, no folder markers


@pytest.mark.integration
def test_ensure_bucket_without_create_fails_fast(local_settings):
    lake = job.Lake(local_settings)
    with pytest.raises(job.ClientError):
        lake.ensure_bucket(f"coding-workshop-missing-{uuid.uuid4().hex[:8]}", create=False)
