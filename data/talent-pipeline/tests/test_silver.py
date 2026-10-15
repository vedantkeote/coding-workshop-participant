"""Tests for the Silver layer: type coercion, rules, contracts, JSONL parsing, drift, quarantine."""

import dataclasses
import json
from datetime import date

import pandas as pd
import pytest

import job

# --- Unit: coercion ------------------------------------------------------------


def test_coerce_int_rejects_text_and_fractions():
    out = job.coerce(pd.Series(["352", " 7 ", None, "abc", "3.5", ""], dtype=object), "int")
    assert out.tolist() == [352, 7, pd.NA, pd.NA, pd.NA, pd.NA] and str(out.dtype) == "Int64"


def test_coerce_dates_are_strict():
    out = job.coerce(pd.Series([date(2024, 2, 29), "2024-04-06", "2026-13-45", "yesterday", "N/A"]), "date")
    assert out.tolist()[:2] == [date(2024, 2, 29), date(2024, 4, 6)]
    assert out.isna().tolist() == [False, False, True, True, True]
    assert out.dtype == job.DATE


def test_coerce_timestamp_requires_iso_seconds():
    out = job.coerce(pd.Series(["2026-03-20T18:43:00", "2026-03-20", "2026-03-20 18:43:00"]), "timestamp")
    assert out.notna().tolist() == [True, False, False]


def test_coerce_str_trims_and_blanks_to_na():
    assert job.coerce(pd.Series([" a ", "", "  ", None]), "str").tolist() == ["a", pd.NA, pd.NA, pd.NA]


def test_coerce_unknown_kind():
    with pytest.raises(ValueError):
        job.coerce(pd.Series([1]), "uuid")


# --- Unit: validate() ----------------------------------------------------------

CONTRACT = job.Contract(
    "demo", {"id": "int", "grade": "str", "score": "int", "note": "str"},
    required=("id", "grade", "score"), key=("id",),
    rules=(job.one_of("grade", ["A", "B"]), job.between("score", 0, 100)),
)


def frame(rows):
    return pd.DataFrame(rows, columns=["id", "grade", "score", "note", "_lineage"])


def test_validate_passes_clean_rows_with_types_and_lineage():
    out = job.validate(frame([[1, "A", "90", None, "src"]]), CONTRACT)
    assert len(out.valid) == 1 and out.rejected.empty
    assert list(out.valid.columns) == ["id", "grade", "score", "note", "_lineage"]
    assert str(out.valid["score"].dtype) == "Int64"


def test_validate_reports_every_failed_check():
    out = job.validate(frame([[None, "Z", "abc", None, "src"]]), CONTRACT)
    assert out.valid.empty
    reasons = set(out.rejected["reject_reason"].iloc[0].split(";"))
    assert reasons == {"missing_id", "not_allowed_grade", "bad_type_score"}


def test_validate_optional_column_may_be_null_but_not_malformed():
    out = job.validate(frame([[1, "A", "5", None, ""], [2, "B", "500", "x", ""]]), CONTRACT)
    assert out.valid["id"].tolist() == [1]
    assert out.rejected["reject_reason"].tolist() == ["out_of_range_score"]


def test_validate_quarantines_later_duplicates_only():
    out = job.validate(frame([[1, "A", "1", "first", ""], [1, "A", "1", "second", ""]]), CONTRACT)
    assert out.valid["note"].tolist() == ["first"]
    assert out.rejected["reject_reason"].tolist() == ["duplicate_id"]


def test_validate_duplicate_of_invalid_row_is_not_a_duplicate():
    # The first copy is invalid, so the second (valid) copy is the survivor.
    out = job.validate(frame([[1, "Z", "1", None, ""], [1, "A", "1", None, ""]]), CONTRACT)
    assert out.valid["grade"].tolist() == ["A"]
    assert out.rejected["reject_reason"].tolist() == ["not_allowed_grade"]


def test_validate_missing_column_is_a_schema_error():
    with pytest.raises(job.SchemaError, match="score"):
        job.validate(pd.DataFrame({"id": [1], "grade": ["A"], "note": [None]}), CONTRACT)


def test_validate_drops_unexpected_columns(caplog):
    df = frame([[1, "A", "1", None, ""]]).assign(surprise=1)
    out = job.validate(df, CONTRACT)
    assert "surprise" not in out.valid.columns


def test_validate_extra_rules_eg_foreign_keys():
    rule = job.references("id", "parents", pd.Series([1, 2], dtype="Int64"))
    out = job.validate(frame([[1, "A", "1", None, ""], [3, "A", "1", None, ""]]), CONTRACT, [rule])
    assert out.valid["id"].tolist() == [1] and out.rejected["reject_reason"].tolist() == ["unknown_parents"]


# --- Unit: source contracts on edge cases --------------------------------------

RUN = date(2026, 10, 15)


def employees(**overrides):
    row = {"employee_id": 1, "first_name": "Ada", "last_name": "L", "email": "ada@acme.example",
           "team_id": 1, "job_level": "IC1", "hire_date": date(2020, 1, 1)}
    return pd.DataFrame([{**row, **overrides}])


@pytest.mark.parametrize("override, reason", [
    ({"email": "not-an-email"}, "bad_format_email"),
    ({"job_level": "Wizard"}, "not_allowed_job_level"),
    ({"hire_date": date(2027, 1, 1)}, "future_hire_date"),
    ({"hire_date": "2020-02-30"}, "bad_type_hire_date"),
])
def test_employee_contract_rejects(override, reason):
    out = job.validate(employees(**override), job.source_contracts(RUN)["employees"])
    assert out.rejected["reject_reason"].tolist() == [reason]


def test_hire_date_on_run_date_is_allowed():
    out = job.validate(employees(hire_date=RUN), job.source_contracts(RUN)["employees"])
    assert len(out.valid) == 1


def test_review_contract_enforces_period_rating_and_one_review_per_period():
    reviews = pd.DataFrame([
        {"review_id": 1, "employee_id": 1, "review_period": "2025-H1", "rating": 4, "manager_comment": None,
         "submitted_date": date(2025, 8, 1)},
        {"review_id": 2, "employee_id": 1, "review_period": "2025-H1", "rating": 3, "manager_comment": "x",
         "submitted_date": date(2025, 8, 2)},
        {"review_id": 3, "employee_id": 2, "review_period": "2025-Q1", "rating": 6, "manager_comment": None,
         "submitted_date": date(2025, 8, 2)},
    ])
    out = job.validate(reviews, job.source_contracts(RUN)["performance_reviews"])
    assert out.valid["review_id"].tolist() == [1]
    assert out.rejected["reject_reason"].tolist() == [
        "duplicate_employee_id_review_period", "bad_format_review_period;out_of_range_rating"]


# --- Unit: learning events -----------------------------------------------------

KEY = "bronze/learning_activity/year=2026/month=03/day=20/events-000.jsonl"


def event(**overrides):
    e = {"event_id": "EV0000000001", "employee_id": "1", "event_date": "2026-03-20T18:43:00",
         "event_type": "quiz_passed", "course_name": "SQL", "text": "Passed.", "score": 80}
    e.update(overrides)
    return json.dumps(e)


def run_events(lines, employee_ids=(1, 2)):
    records, malformed = job.parse_jsonl([(KEY, "\n".join(lines).encode())])
    records, drift = job.apply_field_aliases(records, job.LEARNING_FIELD_ALIASES)
    records = records.rename(columns={"event_date": "event_ts"})
    rule = job.references("employee_id", "employees", pd.Series(employee_ids, dtype="Int64"))
    return job.validate(records, job.LEARNING_EVENTS, [rule]), malformed, drift


def test_parse_jsonl_splits_malformed_lines_and_keeps_lineage():
    records, malformed = job.parse_jsonl([(KEY, f'{event()}\n\n{{"event_id": "EV00\n[1, 2]'.encode())])
    assert len(records) == 1 and records["_source_line"].tolist() == [1]
    assert records["_partition_date"].iloc[0] == pd.Timestamp("2026-03-20")
    assert malformed["reject_reason"].tolist() == ["malformed_json", "not_a_json_object"]
    assert malformed["_source_line"].tolist() == [3, 4]  # blank line 2 skipped, numbering preserved


def test_field_alias_maps_notes_to_text():
    out, _, drift = run_events([event(text=None) .replace('"text": null', '"notes": "old name"')])
    assert drift == 1 and out.valid["text"].tolist() == ["old name"]


@pytest.mark.parametrize("override, reason", [
    ({"employee_id": None}, "missing_employee_id"),
    ({"employee_id": "abc"}, "bad_type_employee_id"),
    ({"employee_id": "999"}, "unknown_employees"),
    ({"event_date": "yesterday"}, "bad_type_event_ts"),
    ({"event_date": ""}, "missing_event_ts"),
    ({"event_date": "2026-03-21T09:00:00"}, "event_outside_partition"),
    ({"event_type": "teleported"}, "not_allowed_event_type"),
    ({"score": None}, "missing_score"),
    ({"score": 101}, "out_of_range_score"),
    ({"event_type": "enrollment"}, "unexpected_score"),
    ({"event_id": "X1"}, "bad_format_event_id"),
])
def test_learning_event_rules(override, reason):
    out, _, _ = run_events([event(**override)])
    assert out.rejected["reject_reason"].tolist() == [reason]


def test_learning_event_without_score_is_fine_for_unscored_types():
    out, _, _ = run_events([event(event_type="enrollment", score=None)])
    assert len(out.valid) == 1


def test_learning_events_dedupe_keeps_first_line():
    out, _, _ = run_events([event(text="first"), event(text="second")])
    assert out.valid["text"].tolist() == ["first"] and out.valid["_source_line"].tolist() == [1]


# --- Unit: dimension -----------------------------------------------------------


def test_dim_employee_joins_team_and_computes_tenure():
    emp = pd.DataFrame({"employee_id": [1], "first_name": ["Ada"], "last_name": ["L"], "email": ["a@b.c"],
                        "team_id": [7], "job_level": ["IC1"],
                        "hire_date": job.coerce(pd.Series(["2016-10-15"]), "date")})
    teams = pd.DataFrame({"team_id": [7], "name": ["Team X"], "department": ["Sales"]})
    dim = job.build_dim_employee(emp, teams, date(2026, 10, 15))
    row = dim.iloc[0]
    assert (row.full_name, row.team_name, row.department, row.tenure_years) == ("Ada L", "Team X", "Sales", 10.0)


# --- Integration: full Bronze -> Silver on scratch buckets ---------------------

BAD_AND_GOOD_LINES = [
    event(event_id="EV0000000001"),
    event(event_id="EV0000000001"),                 # duplicate
    event(event_id="EV0000000002", employee_id=None),
    event(event_id="EV0000000003", event_date="N/A"),
    '{"event_id": "EV0000000004", "employ',           # truncated
    event(event_id="EV0000000005", event_type="enrollment", score=None),
]


@pytest.fixture
def env(local_settings, make_bucket):
    settings = dataclasses.replace(local_settings, landing_bucket=make_bucket("landing"),
                                   lake_bucket=make_bucket("lake"), run_date=RUN)
    lake = job.Lake(settings)
    lake.s3.put_object(Bucket=settings.landing_bucket, Key=KEY.removeprefix("bronze/"),
                       Body="\n".join(BAD_AND_GOOD_LINES).encode())
    db = job.SourceDatabase(settings)
    job.ingest_bronze(job.RunContext(settings), db, lake)
    return settings, lake


def run_silver(settings, lake):
    ctx = job.RunContext(settings)
    job.build_silver(ctx, lake)
    return ctx.steps[0].metrics


@pytest.mark.integration
def test_silver_reconciles_and_routes_bad_rows(env):
    settings, lake = env
    m = run_silver(settings, lake)
    assert (m["learning_events_read"], m["learning_events_valid"], m["learning_events_quarantined"]) == (6, 2, 4)
    for table in job.SOURCE_TABLES:
        assert m[f"{table}_read"] == m[f"{table}_valid"] + m[f"{table}_quarantined"]
    q = lake.read_parquet(settings.lake_bucket, "quarantine/learning_events")
    assert sorted(q["reject_reason"]) == ["bad_type_event_ts", "duplicate_event_id", "malformed_json",
                                          "missing_employee_id"]
    assert q["raw_record"].str.len().gt(0).all() and set(q["_source_line"]) == {2, 3, 4, 5}


@pytest.mark.integration
def test_silver_output_types_and_partitions(env):
    settings, lake = env
    run_silver(settings, lake)
    events = lake.read_parquet(settings.lake_bucket, "silver/learning_events")
    assert events["event_date"].dtype == job.DATE and str(events["employee_id"].dtype) == "Int64"
    keys = [o["Key"] for o in lake.list_objects(settings.lake_bucket, "silver/learning_events/")]
    assert keys == ["silver/learning_events/event_year=2026/event_month=3/part-0.parquet"]
    dim = lake.read_parquet(settings.lake_bucket, "silver/dim_employee")
    assert len(dim) == 2000 and dim["department"].notna().all()


@pytest.mark.integration
def test_silver_rerun_is_idempotent(env):
    settings, lake = env
    first = run_silver(settings, lake)
    second = run_silver(settings, lake)
    assert first == second
    assert len(lake.read_parquet(settings.lake_bucket, "silver/learning_events")) == 2
    assert len(lake.read_parquet(settings.lake_bucket, "quarantine/learning_events")) == 4
    assert len(lake.list_objects(settings.lake_bucket, "silver/employees/")) == 1


@pytest.mark.integration
def test_silver_without_bronze_fails_clearly(local_settings, make_bucket):
    settings = dataclasses.replace(local_settings, lake_bucket=make_bucket("lake"))
    ctx = job.RunContext(settings)
    with pytest.raises(RuntimeError, match="run the bronze step"):
        job.build_silver(ctx, job.Lake(settings))
    assert ctx.steps[0].status == "failed"


# --- Unit: records dated before the employee's hire date ----------------------------


def test_period_end_is_last_day_of_half_year():
    assert job.period_end(pd.Series(["2024-H2", "2025-H1"], dtype="string")).tolist() == [
        date(2024, 12, 31), date(2025, 6, 30)]


@pytest.mark.parametrize("hired, period, flagged", [
    ("2024-12-31", "2024-H2", False),  # hired on the period's last day: worked (a day of) it
    ("2025-01-01", "2024-H2", True),   # hired the day after the period ended
    ("2024-09-01", "2024-H2", False),  # hired mid-period: partial period, not flagged
    ("2025-01-01", "2025-H1", False),  # hired on the first day of the period
    ("2018-01-01", "2024-H2", False),  # long-standing employee
])
def test_review_before_hire_boundaries(hired, period, flagged):
    employees = pd.DataFrame({"employee_id": pd.array([1], dtype="Int64"), "hire_date": job.coerce(pd.Series([hired]), "date")})
    reviews = pd.DataFrame({"employee_id": pd.array([1], dtype="Int64"), "review_period": pd.array([period], dtype="string")})
    rule = job.BEFORE_HIRE_DATES["performance_reviews"]
    assert job.flag_before_hire(reviews["employee_id"], rule(reviews), employees).tolist() == [flagged]


def test_flag_before_hire_handles_empty_input():
    employees = pd.DataFrame({"employee_id": pd.array([1], dtype="Int64"), "hire_date": job.coerce(pd.Series(["2020-01-01"]), "date")})
    empty = pd.Series([], dtype="Int64")
    assert job.flag_before_hire(empty, job.coerce(pd.Series([], dtype="string"), "date"), employees).tolist() == []


def test_insert_before_lineage_keeps_lineage_columns_last():
    df = pd.DataFrame({"a": [1], "_run_id": ["r"]})
    assert list(job.insert_before_lineage(df, "flag", pd.Series([True])).columns) == ["a", "flag", "_run_id"]
