"""End-to-end: `main()` exactly as the cloud runs it, on the real source data, into a
scratch lake. Covers the guide's end-to-end smoke test, idempotency validation and
aggregation validation against independently computed expected values.

Needs: local Postgres seeded (schema hr), the seeded LocalStack landing bucket, and the
reference dataset in sampleData/ (scripts/seed_sources.sh sets all of these up).
"""

import glob
import json
import logging
import os
import uuid
from pathlib import Path

import pandas as pd
import pytest

import job

pytestmark = pytest.mark.integration

RUN_DATE = "2026-10-15"
DATASET = Path(os.environ.get("SOURCE_DATA_DIR", Path(__file__).resolve().parents[3]
                              / "sampleData/data-engineering-performance-talent-management"))


def gold_snapshot(lake, bucket):
    """All Gold tables minus per-run lineage columns, for run-to-run comparison."""
    return {t: lake.read_parquet(bucket, f"gold/{t}").drop(columns=["_run_id"]) for t in job.GOLD_GRAIN}


@pytest.fixture(scope="module")
def pipeline():
    """Run the full pipeline twice via main() into a fresh scratch lake bucket."""
    participant = os.environ.get("PARTICIPANT_ID")
    if not participant:
        pytest.skip("PARTICIPANT_ID not set (needed to find the seeded landing bucket)")
    bucket = f"coding-workshop-e2e-{uuid.uuid4().hex[:8]}"
    env = {"IS_LOCAL": "true", "PARTICIPANT_ID": participant, "DATALAKE_BUCKET": bucket, "RUN_DATE": RUN_DATE}
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level  # main() reconfigures logging
    with pytest.MonkeyPatch.context() as mp:
        for key, value in env.items():
            mp.setenv(key, value)
        settings = job.Settings.from_env()
        lake = job.Lake(settings)
        exit_codes, snapshots = [], []
        try:
            for _ in range(2):
                exit_codes.append(job.main([]))
                snapshots.append(gold_snapshot(lake, bucket))
        finally:
            root.handlers[:] = saved_handlers
            root.setLevel(saved_level)
            job._LOG_CONTEXT.clear()
        yield settings, lake, exit_codes, snapshots
    for top in ("bronze/", "silver/", "gold/", "quarantine/", "_runs/"):
        lake.delete_prefix(bucket, top)
    lake.s3.delete_bucket(Bucket=bucket)


def test_smoke_full_pipeline_succeeds_and_publishes(pipeline):
    settings, lake, exit_codes, _ = pipeline
    assert exit_codes == [0, 0]
    latest = json.loads(lake.get_bytes(settings.lake_bucket, "_runs/latest.json"))
    manifest = json.loads(lake.get_bytes(settings.lake_bucket, latest["manifest"]))
    assert manifest["status"] == "succeeded" and [s["name"] for s in manifest["steps"]] == [
        "preflight", "bronze", "silver", "gold"]
    gold = {s["name"]: s for s in manifest["steps"]}["gold"]["metrics"]
    assert gold["promotion_readiness_rows"] == 2000 and gold["skill_distribution_rows"] == 400


def test_idempotency_second_run_produces_identical_gold(pipeline):
    _, _, _, (first, second) = pipeline
    for table in job.GOLD_GRAIN:
        pd.testing.assert_frame_equal(first[table], second[table], obj=table)


def test_second_run_is_incremental_and_duplicate_free(pipeline):
    settings, lake, _, _ = pipeline
    runs = sorted(o["Key"] for o in lake.list_objects(settings.lake_bucket, f"_runs/run_date={RUN_DATE}/"))
    second = json.loads(lake.get_bytes(settings.lake_bucket, runs[-1]))
    bronze = {s["name"]: s for s in second["steps"]}["bronze"]["metrics"]
    assert bronze["learning_files_copied"] == 0 and bronze["learning_files_unchanged"] == 783
    assert bronze["learning_files_empty"] == 1  # year=2025/month=09/day=03 is 0 bytes in the source
    assert len(lake.read_parquet(settings.lake_bucket, "silver/learning_events")) == 29574
    assert len(lake.list_objects(settings.lake_bucket, "quarantine/learning_events/")) == 1


@pytest.fixture(scope="module")
def raw():
    """The reference dataset read with plain pandas/json - no pipeline code involved."""
    if not DATASET.exists():
        pytest.skip(f"reference dataset not found at {DATASET}")
    tables = {name: pd.read_csv(DATASET / f"{name}.csv") for name in (
        "employees", "teams", "performance_reviews", "talent_review", "competency_assessments", "projects")}
    seen, events = set(), []
    for path in sorted(glob.glob(str(DATASET / "learning_activity/*/*/*/*.jsonl"))):
        for line in open(path):
            try:
                e = json.loads(line)
                pd.Timestamp(e["event_date"])
                valid_date = len(str(e["event_date"])) == 19
            except Exception:
                continue
            if valid_date and e["employee_id"] is not None and e["event_id"] not in seen:
                seen.add(e["event_id"])
                events.append(e)
    tables["events"] = pd.DataFrame(events).astype({"employee_id": int})
    return tables


def test_gold_matches_independent_computation(pipeline, raw):
    _, _, _, (gold, _) = pipeline
    reviews = raw["performance_reviews"].pivot(index="employee_id", columns="review_period", values="rating")
    change = reviews["2025-H1"] - reviews["2024-H2"]
    talent = raw["talent_review"].set_index("employee_id").join(reviews)
    ready = (talent.potential_rating == "High") & (talent.promotion_readiness == "Ready Now")
    tiers = gold["promotion_readiness"]["promotion_tier"].value_counts()
    attrition = gold["attrition_risk"]
    gaps = raw["competency_assessments"].required_level - raw["competency_assessments"].proficiency_level

    # Q1-Q3, Q5: counts that follow directly from the agreed definitions
    assert int(change.sum()) == int(gold["employee_performance_history"]["rating_change"].sum())
    assert int((gaps >= 2).sum()) == int(gold["employee_skill_gaps"]["is_critical_gap"].sum()) == 325
    assert int(ready.sum()) == tiers["ready_strong"] + tiers["ready"] == 357
    assert int((ready & (talent["2025-H1"] >= 4)).sum()) == tiers["ready_strong"] == 166
    assert int((change <= -2).sum()) == int((attrition["performance_risk"] == "High").sum())
    assert int(((change <= -2) & (talent.attrition_risk == "Low")).sum()) == int(attrition["hidden_risk"].sum())

    # Q4: every employee's completed-course count
    completed = raw["events"][raw["events"].event_type == "course_completed"].groupby("employee_id").size()
    got = gold["employee_learning_summary"].set_index("employee_id")["courses_completed"]
    assert completed.reindex(got.index, fill_value=0).astype(int).tolist() == got.astype(int).tolist()
    assert len(raw["events"]) == int(gold["employee_learning_summary"]["total_events"].sum())

    # Q6: every team's average latest rating
    latest = raw["employees"].merge(raw["performance_reviews"].query("review_period == '2025-H1'"), on="employee_id")
    expected = latest.groupby("team_id")["rating"].mean().round(3)
    team = gold["team_performance_outcomes"].set_index("team_id")["avg_latest_rating"].astype(float)
    pd.testing.assert_series_equal(expected, team.reindex(expected.index), check_names=False)

    # Q7: every competency x department average
    skills = (raw["competency_assessments"].merge(raw["employees"], on="employee_id")
              .merge(raw["teams"], on="team_id").groupby(["competency_id", "department"])["proficiency_level"]
              .mean().round(3))
    dist = gold["skill_distribution"].set_index(["competency_id", "department"])["avg_proficiency"].astype(float)
    pd.testing.assert_series_equal(skills, dist.reindex(skills.index), check_names=False)


def test_records_before_hire_match_independent_count(pipeline, raw):
    """Option 1 (flag, keep): every flagged record is counted from the raw files, and none were dropped."""
    _, _, _, (gold, _) = pipeline
    hire = raw["employees"].set_index("employee_id")["hire_date"].pipe(pd.to_datetime)
    reviews = raw["performance_reviews"]
    period_end = pd.to_datetime(reviews["review_period"].map({"2024-H2": "2024-12-31", "2025-H1": "2025-06-30"}))
    assessments = raw["competency_assessments"]
    events = raw["events"]

    expected_reviews = int((period_end < reviews["employee_id"].map(hire)).sum())
    expected_assessments = int((pd.to_datetime(assessments["assessed_date"]) < assessments["employee_id"].map(hire)).sum())
    expected_events = int((pd.to_datetime(events["event_date"]).dt.normalize() < events["employee_id"].map(hire)).sum())

    assert expected_reviews == int(gold["employee_performance_history"]["recorded_before_hire"].sum()) == 666
    assert expected_assessments == int(gold["employee_skill_gaps"]["recorded_before_hire"].sum()) == 234
    assert expected_events == int(gold["employee_learning_summary"]["events_before_hire"].sum())
    assert len(gold["employee_performance_history"]) == 4000  # flagged, not removed
