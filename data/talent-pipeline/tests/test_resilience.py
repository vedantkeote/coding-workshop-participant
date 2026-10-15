"""Guide section 7 (Error Handling & Pipeline Resilience) and performance test 4
(External Dependency Test): transient failures recover, permanent ones fail fast and
clearly, schema drift is quarantined + alerted without corrupting downstream layers."""

import dataclasses
import logging
from datetime import date

import pandas as pd
import psycopg2
import pytest
from botocore.awsrequest import AWSResponse

import job

# --- Unit: file-level schema drift --------------------------------------------------


def records(rows):
    return pd.DataFrame(rows).assign(_raw="{}", _source_line=range(1, len(rows) + 1))


def test_file_missing_a_required_field_entirely_is_quarantined_whole(caplog):
    ok = {"_source_file": "a", "event_id": "EV1", "employee_id": "1", "event_date": "x", "event_type": "t",
          "course_name": "c"}
    partial = {**ok, "_source_file": "b", "employee_id": None}           # one null: row-level problem
    drifted = {**ok, "_source_file": "c", "employee_id": None, "emp": "1"}  # never present: file-level drift
    df = records([ok, partial, {**partial, "employee_id": "2"}, drifted, drifted])
    with caplog.at_level(logging.ERROR, logger="talent_pipeline"):
        kept, rejected = job.quarantine_drifted_files(df, job.LEARNING_REQUIRED_SOURCE_FIELDS)
    assert set(kept["_source_file"]) == {"a", "b"} and len(rejected) == 2
    assert set(rejected["reject_reason"]) == {"schema_mismatch_file: missing field(s) employee_id"}
    alert = [r for r in caplog.records if r.getMessage() == "schema mismatch: file quarantined"][0]
    assert alert.fields["alert"] is True and alert.fields["file"] == "c"


def test_healthy_files_pass_through_untouched():
    ok = {"_source_file": "a", "event_id": "EV1", "employee_id": "1", "event_date": "x", "event_type": "t",
          "course_name": "c"}
    kept, rejected = job.quarantine_drifted_files(records([ok]), job.LEARNING_REQUIRED_SOURCE_FIELDS)
    assert len(kept) == 1 and rejected.empty


# --- Postgres: transient failure recovers ----------------------------------------


@pytest.mark.integration
def test_postgres_recovers_from_transient_connection_failures(local_settings, monkeypatch):
    real_connect, calls = psycopg2.connect, []

    def flaky_connect(**kwargs):
        calls.append(1)
        if len(calls) <= 2:
            raise psycopg2.OperationalError("server closed the connection unexpectedly")
        return real_connect(**kwargs)

    monkeypatch.setattr(job.psycopg2, "connect", flaky_connect)
    monkeypatch.setattr(job.time, "sleep", lambda _: None)
    job.SourceDatabase(local_settings).ping()  # succeeds on the 3rd attempt
    assert len(calls) == 3


# --- S3: transient 503s recover; permanent errors fail fast -----------------------


class _EmptyBody:
    def stream(self, **_):
        return iter([b""])


def inject_s3_status(lake, operation, status, times):
    """Make the next `times` calls of `operation` return HTTP `status` before reaching LocalStack."""
    seen = {"n": 0}

    def handler(request, **_):
        seen["n"] += 1
        if seen["n"] <= times:
            return AWSResponse(request.url, status, {}, _EmptyBody())
        return None  # let the real request through

    lake.s3.meta.events.register(f"before-send.s3.{operation}", handler)
    return seen


@pytest.mark.integration
def test_s3_recovers_from_transient_503s(local_settings, make_bucket):
    lake = job.Lake(local_settings)
    bucket = make_bucket()
    seen = inject_s3_status(lake, "HeadBucket", 503, times=2)
    lake.ensure_bucket(bucket, create=False)  # AWS standard retry mode absorbs the 503s
    assert seen["n"] == 3


@pytest.mark.integration
def test_s3_gives_up_after_max_attempts(local_settings, make_bucket):
    lake = job.Lake(local_settings)
    bucket = make_bucket()
    seen = inject_s3_status(lake, "HeadBucket", 503, times=100)
    with pytest.raises(job.ClientError):
        lake.ensure_bucket(bucket, create=False)
    assert seen["n"] == 5  # total_max_attempts in BotoConfig (first try + 4 retries)


@pytest.mark.integration
def test_s3_access_denied_fails_fast_with_iam_hint(local_settings, make_bucket):
    lake = job.Lake(local_settings)
    bucket = make_bucket()
    seen = inject_s3_status(lake, "HeadBucket", 403, times=100)
    with pytest.raises(PermissionError, match="verify the IAM role"):
        lake.ensure_bucket(bucket, create=False)
    assert seen["n"] == 1  # 403 is permanent: no retries


# --- Table schema drift: quarantine + alert, downstream untouched ------------------


@pytest.fixture
def lake_with_silver(local_settings, make_bucket):
    settings = dataclasses.replace(local_settings, landing_bucket=make_bucket("landing"),
                                   lake_bucket=make_bucket("lake"), run_date=date(2026, 10, 15))
    lake = job.Lake(settings)
    job.ingest_bronze(job.RunContext(settings), job.SourceDatabase(settings), lake)
    job.build_silver(job.RunContext(settings), lake)
    return settings, lake


@pytest.mark.integration
def test_table_schema_drift_quarantines_snapshot_alerts_and_leaves_silver_untouched(lake_with_silver, caplog):
    settings, lake = lake_with_silver
    silver_before = {o["Key"]: o["ETag"] for o in lake.list_objects(settings.lake_bucket, "silver/")}

    # The source drops a column: rewrite today's Bronze talent_review without attrition_risk.
    prefix = f"bronze/postgres/talent_review/ingest_date={settings.run_date.isoformat()}"
    drifted = lake.read_parquet(settings.lake_bucket, prefix).drop(columns=["attrition_risk"])
    lake.write_parquet(drifted, settings.lake_bucket, prefix)

    ctx = job.RunContext(settings)
    with caplog.at_level(logging.ERROR, logger="talent_pipeline"), pytest.raises(job.SchemaError):
        job.build_silver(ctx, lake)

    q = lake.read_parquet(settings.lake_bucket, "quarantine/talent_review")
    assert len(q) == 2000 and set(q["reject_reason"]) == {"schema_mismatch: missing column(s) attrition_risk"}
    alerts = [r for r in caplog.records if getattr(r, "fields", {}).get("alert")]
    assert alerts and alerts[0].fields["entity"] == "talent_review"
    # Nothing in Silver was rewritten - not even the tables processed before talent_review.
    assert {o["Key"]: o["ETag"] for o in lake.list_objects(settings.lake_bucket, "silver/")} == silver_before
    assert ctx.steps[0].status == "failed"
