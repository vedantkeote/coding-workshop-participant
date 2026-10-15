"""Tests for the Gold layer: each business definition at its boundaries, plus grain/reconciliation checks."""

import dataclasses
from datetime import date

import pandas as pd
import pytest

import job

AS_OF = date(2026, 10, 15)


def d(s):
    return job.coerce(pd.Series(s), "date")


def dim(n=3, team_ids=None):
    team_ids = team_ids or [1] * n
    return pd.DataFrame({
        "employee_id": pd.array(range(1, n + 1), dtype="Int64"), "full_name": [f"E{i}" for i in range(1, n + 1)],
        "job_level": ["IC1"] * n, "hire_date": d(["2020-01-01"] * n), "tenure_years": [1.0] * n,
        "team_id": pd.array(team_ids, dtype="Int64"),
        "team_name": [f"T{t}" for t in team_ids], "department": ["Sales"] * n,
    })


def reviews(rows, before_hire=()):  # rows: (employee_id, period, rating); before_hire: row positions flagged
    return pd.DataFrame({
        "review_id": range(1, len(rows) + 1),
        "employee_id": pd.array([r[0] for r in rows], dtype="Int64"),
        "review_period": pd.array([r[1] for r in rows], dtype="string"),
        "rating": pd.array([r[2] for r in rows], dtype="Int64"),
        "manager_comment": None, "submitted_date": d(["2026-01-01"] * len(rows)),
        "recorded_before_hire": [i in before_hire for i in range(len(rows))],
    })


def talent(rows):  # rows: (employee_id, potential, readiness, attrition)
    return pd.DataFrame({"employee_id": pd.array([r[0] for r in rows], dtype="Int64"), "review_cycle": "2025-H1",
                         "potential_rating": [r[1] for r in rows], "promotion_readiness": [r[2] for r in rows],
                         "attrition_risk": [r[3] for r in rows]})


# --- Q1 performance history ----------------------------------------------------


def test_period_start():
    assert job.period_start(pd.Series(["2024-H2", "2025-H1"], dtype="string")).tolist() == [
        date(2024, 7, 1), date(2025, 1, 1)]


def test_history_orders_periods_and_computes_change_regardless_of_input_order():
    h = job.gold_performance_history(reviews([(1, "2025-H1", 2), (1, "2024-H2", 4), (2, "2025-H1", 3)]), dim())
    e1 = h[h["employee_id"] == 1]
    assert e1["review_period"].tolist() == ["2024-H2", "2025-H1"]
    assert e1["rating_change"].tolist()[1] == -2 and pd.isna(e1["rating_change"].tolist()[0])
    assert e1["is_latest_review"].tolist() == [False, True]
    assert h[h["employee_id"] == 2]["is_latest_review"].tolist() == [True]  # single review is latest


# --- Q2 skill gaps -------------------------------------------------------------

COMPETENCIES = pd.DataFrame({"competency_id": pd.array([1], dtype="Int64"), "name": ["SQL"],
                             "category": ["Technical"]})


def assessments(rows):  # rows: (employee_id, proficiency, required, assessed_date)
    return pd.DataFrame({
        "assessment_id": range(1, len(rows) + 1), "employee_id": pd.array([r[0] for r in rows], dtype="Int64"),
        "competency_id": pd.array([1] * len(rows), dtype="Int64"),
        "proficiency_level": pd.array([r[1] for r in rows], dtype="Int64"),
        "required_level": pd.array([r[2] for r in rows], dtype="Int64"), "assessed_date": d([r[3] for r in rows]),
        "recorded_before_hire": False,
    })


def test_gap_status_boundaries():
    g = job.gold_skill_gaps(assessments([(1, 1, 3, "2026-01-01"), (2, 2, 3, "2026-01-01"),
                                         (3, 3, 3, "2026-01-01")]), COMPETENCIES, dim())
    assert g["gap_levels"].tolist() == [2, 1, 0]
    assert g["gap_status"].tolist() == ["critical", "below", "meets"]
    assert g["is_critical_gap"].tolist() == [True, False, False]


def test_gap_exceeds_and_latest_assessment_wins():
    g = job.gold_skill_gaps(assessments([(1, 1, 4, "2025-01-01"), (1, 5, 4, "2026-01-01")]), COMPETENCIES, dim())
    assert len(g) == 1 and g["gap_status"].tolist() == ["exceeds"] and g["proficiency_level"].tolist() == [5]


# --- Q3 promotion readiness ----------------------------------------------------


def test_promotion_tiers():
    people = dim(5)
    h = job.gold_performance_history(reviews([(1, "2025-H1", 4), (2, "2025-H1", 3), (3, "2025-H1", 5),
                                              (4, "2025-H1", 5), (5, "2025-H1", 5)]), people)
    t = talent([(1, "High", "Ready Now", "Low"), (2, "High", "Ready Now", "Low"), (3, "High", "1-2 Years", "Low"),
                (4, "Medium", "Ready Now", "Low"), (5, "High", "Not Ready", "Low")])
    gaps = job.gold_skill_gaps(assessments([(1, 1, 3, "2026-01-01")]), COMPETENCIES, people)
    out = job.gold_promotion_readiness(people, t, h, gaps)
    assert out["promotion_tier"].tolist() == ["ready_strong", "ready", "pipeline", "not_ready", "not_ready"]
    assert out["critical_gap_count"].tolist() == [1, 0, 0, 0, 0]


def test_promotion_handles_employee_without_talent_review_or_reviews():
    out = job.gold_promotion_readiness(dim(1), talent([]), job.gold_performance_history(reviews([]), dim(1)),
                                       job.gold_skill_gaps(assessments([]), COMPETENCIES, dim(1)))
    assert out["promotion_tier"].tolist() == ["not_ready"] and not out["is_high_potential"].iloc[0]


# --- Q4 learning ---------------------------------------------------------------


def events(rows, before_hire=()):  # rows: (employee_id, type, course, ts, score); before_hire: positions
    ts = pd.to_datetime([r[3] for r in rows])
    return pd.DataFrame({"event_id": [f"EV{i}" for i in range(len(rows))],
                         "employee_id": pd.array([r[0] for r in rows], dtype="Int64"),
                         "event_type": [r[1] for r in rows], "course_name": [r[2] for r in rows],
                         "event_ts": ts, "event_date": pd.Series(ts).dt.normalize().astype(job.DATE),
                         "score": pd.array([r[4] for r in rows], dtype="Int64"),
                         "recorded_before_hire": [i in before_hire for i in range(len(rows))]})


def test_learning_summary_counts_courses_scores_and_recency():
    ev = events([(1, "course_completed", "SQL", "2026-09-01T10:00:00", None),
                 (1, "course_completed", "SQL", "2026-09-02T10:00:00", None),
                 (1, "course_completed", "Go", "2025-01-01T10:00:00", None),
                 (1, "quiz_passed", "Go", "2025-01-01T09:00:00", 90),
                 (1, "quiz_attempted", "Go", "2025-01-01T08:00:00", 50)])
    row = job.gold_learning_summary(ev, dim(2), AS_OF).set_index("employee_id").loc[1]
    assert (row.courses_completed, row.distinct_courses_completed, row.completed_courses) == (3, 2, "Go; SQL")
    assert row.avg_quiz_score == 70.0 and row.total_events == 5
    assert row[f"events_last_{job.LEARNING_RECENT_DAYS}d"] == 2  # only the two Sept 2026 events
    assert row.last_activity_date == date(2026, 9, 2)


def test_learning_summary_includes_inactive_employees_with_zeros():
    out = job.gold_learning_summary(events([(1, "enrollment", "SQL", "2026-09-01T10:00:00", None)]), dim(2), AS_OF)
    e2 = out.set_index("employee_id").loc[2]
    assert e2.total_events == 0 and e2.enrollments == 0 and e2.completed_courses == "" and pd.isna(e2.avg_quiz_score)


# --- Q5 attrition --------------------------------------------------------------


@pytest.mark.parametrize("ratings, hr, perf, trend, combined, alignment, hidden", [
    ((5, 3), "Low", "High", "declining", "High", "performance_higher", True),
    ((4, 3), "Low", "Medium", "declining", "Medium", "performance_higher", False),
    ((3, 3), "Medium", "Low", "stable", "Medium", "hr_higher", False),
    ((2, 4), "High", "Low", "improving", "High", "hr_higher", False),
    ((5, 3), "High", "High", "declining", "High", "aligned", False),
])
def test_attrition_risk_signals(ratings, hr, perf, trend, combined, alignment, hidden):
    people = dim(1)
    h = job.gold_performance_history(reviews([(1, "2024-H2", ratings[0]), (1, "2025-H1", ratings[1])]), people)
    learning = job.gold_learning_summary(events([]), people, AS_OF)
    row = job.gold_attrition_risk(people, talent([(1, "Low", "Not Ready", hr)]), h, learning).iloc[0]
    assert (row.performance_risk, row.rating_trend, row.combined_risk, row.risk_alignment, bool(row.hidden_risk)) \
        == (perf, trend, combined, alignment, hidden)


def test_attrition_single_review_is_unknown_not_low():
    people = dim(1)
    h = job.gold_performance_history(reviews([(1, "2025-H1", 2)]), people)
    row = job.gold_attrition_risk(people, talent([(1, "Low", "Not Ready", "Medium")]), h,
                                  job.gold_learning_summary(events([]), people, AS_OF)).iloc[0]
    assert (row.performance_risk, row.rating_trend, row.combined_risk, row.risk_alignment) == \
        ("Unknown", "unknown", "Medium", "unknown")


# --- Q6 teams ------------------------------------------------------------------


def test_team_outcomes_include_teams_without_projects_or_people():
    teams = pd.DataFrame({"team_id": pd.array([1, 2, 3], dtype="Int64"), "name": ["A", "B", "C"],
                          "department": ["Sales"] * 3})
    people = dim(3, team_ids=[1, 1, 2])
    h = job.gold_performance_history(reviews([(1, "2025-H1", 4), (2, "2025-H1", 2), (3, "2025-H1", 5)]), people)
    projects = pd.DataFrame({"project_id": [1, 2], "team_id": pd.array([1, 1], dtype="Int64"),
                             "outcome_status": ["on-time", "delayed"], "outcome_score": [80.0, 40.0]})
    out = job.gold_team_outcomes(teams, people, h, projects).set_index("team_id")
    assert out.loc[1, "avg_latest_rating"] == 3.0 and out.loc[1, "high_performer_rate"] == 0.5
    assert (out.loc[1, "project_count"], out.loc[1, "on_time_rate"], out.loc[1, "avg_project_outcome_score"]) \
        == (2, 0.5, 60.0)
    assert out.loc[2, "has_projects"] is False or not out.loc[2, "has_projects"]
    assert out.loc[3, "headcount"] == 0 and pd.isna(out.loc[3, "avg_latest_rating"])


# --- Q7 skill distribution -----------------------------------------------------


def test_skill_distribution_counts_reaggregate_exactly():
    people = dim(3)
    gaps = job.gold_skill_gaps(assessments([(1, 1, 3, "2026-01-01"), (2, 3, 3, "2026-01-01"),
                                            (3, 5, 3, "2026-01-01")]), COMPETENCIES, people)
    row = job.gold_skill_distribution(gaps).iloc[0]
    assert row.employees_assessed == 3 and row.avg_proficiency == 3.0 and row.critical_gaps == 1
    assert round(row.meeting_requirement_rate, 3) == 0.667
    assert [row[f"proficiency_level_{i}"] for i in range(1, 6)] == [1, 0, 1, 0, 1]


# --- Gold validation -----------------------------------------------------------


def silver_fixture():
    people = dim(2)
    return {
        "dim_employee": people,
        "teams": pd.DataFrame({"team_id": pd.array([1], dtype="Int64"), "name": ["A"], "department": ["Sales"]}),
        "competencies": COMPETENCIES,
        "projects": pd.DataFrame({"project_id": [1], "team_id": pd.array([1], dtype="Int64"),
                                  "outcome_status": ["on-time"], "outcome_score": [70.0]}),
        "performance_reviews": reviews([(1, "2025-H1", 4), (2, "2025-H1", 3)]),
        "competency_assessments": assessments([(1, 2, 3, "2026-01-01")]),
        "talent_review": talent([(1, "High", "Ready Now", "Low"), (2, "Low", "Not Ready", "High")]),
        "learning_events": events([(1, "enrollment", "SQL", "2026-09-01T10:00:00", None)]),
    }


def test_check_gold_accepts_consistent_output():
    silver = silver_fixture()
    job.check_gold(silver, job.compute_gold(silver, AS_OF))


def test_check_gold_catches_grain_violation_and_dropped_rows():
    silver = silver_fixture()
    gold = job.compute_gold(silver, AS_OF)
    gold["attrition_risk"] = pd.concat([gold["attrition_risk"], gold["attrition_risk"].head(1)])  # fan-out
    gold["promotion_readiness"] = gold["promotion_readiness"].head(1)                            # dropped row
    with pytest.raises(RuntimeError) as err:
        job.check_gold(silver, gold)
    assert "attrition_risk: 1 rows violate grain" in str(err.value)
    assert "promotion_readiness: 1 rows, expected 2" in str(err.value)


# --- Integration ---------------------------------------------------------------


@pytest.fixture
def env(local_settings, make_bucket):
    settings = dataclasses.replace(local_settings, landing_bucket=make_bucket("landing"),
                                   lake_bucket=make_bucket("lake"), run_date=AS_OF)
    lake = job.Lake(settings)
    lake.s3.put_object(Bucket=settings.landing_bucket,
                       Key="learning_activity/year=2026/month=09/day=01/events-000.jsonl",
                       Body=b'{"event_id": "EV0000000001", "employee_id": "1", "event_date": "2026-09-01T10:00:00", '
                            b'"event_type": "course_completed", "course_name": "SQL", "text": "Done.", "score": null}\n')
    ctx = job.RunContext(settings)
    job.ingest_bronze(ctx, job.SourceDatabase(settings), lake)
    job.build_silver(ctx, lake)
    return settings, lake


@pytest.mark.integration
def test_gold_end_to_end_and_idempotent(env):
    settings, lake = env
    for _ in range(2):
        ctx = job.RunContext(settings)
        job.build_gold(ctx, lake)
    metrics = ctx.steps[0].metrics
    assert metrics["promotion_readiness_rows"] == 2000 and metrics["team_performance_outcomes_rows"] == 100
    for table in job.GOLD_GRAIN:
        assert len(lake.list_objects(settings.lake_bucket, f"gold/{table}/")) == 1
    learning = lake.read_parquet(settings.lake_bucket, "gold/employee_learning_summary").set_index("employee_id")
    assert learning.loc[1, "completed_courses"] == "SQL" and learning["total_events"].sum() == 1
    assert set(learning["_as_of_date"]) == {"2026-10-15"}


@pytest.mark.integration
def test_gold_without_silver_fails_clearly(local_settings, make_bucket):
    settings = dataclasses.replace(local_settings, lake_bucket=make_bucket("lake"))
    with pytest.raises(RuntimeError, match="run the silver step"):
        job.build_gold(job.RunContext(settings), job.Lake(settings))


# --- Records dated before hire: flagged in Silver, carried into Gold ---------------


def test_history_and_promotion_carry_the_pre_hire_flag():
    people = dim(2)
    h = job.gold_performance_history(reviews([(1, "2024-H2", 3), (1, "2025-H1", 4), (2, "2025-H1", 5)],
                                             before_hire={1}), people)
    assert h["recorded_before_hire"].tolist() == [False, True, False]
    out = job.gold_promotion_readiness(people, talent([(1, "High", "Ready Now", "Low"), (2, "High", "Ready Now", "Low")]),
                                       h, job.gold_skill_gaps(assessments([]), COMPETENCIES, people))
    assert out["latest_review_before_hire"].tolist() == [True, False]
    assert out["promotion_tier"].tolist() == ["ready_strong", "ready_strong"]  # flagged, not excluded


def test_learning_summary_counts_events_before_hire():
    ev = events([(1, "enrollment", "SQL", "2019-01-01T10:00:00", None),
                 (1, "course_completed", "SQL", "2026-09-01T10:00:00", None)], before_hire={0})
    row = job.gold_learning_summary(ev, dim(1), AS_OF).iloc[0]
    assert (row.total_events, row.events_before_hire) == (2, 1)


@pytest.mark.parametrize("flagged, expected", [((), False), ((0,), True), ((1,), True)])
def test_attrition_trend_flags_pre_hire_reviews(flagged, expected):
    people = dim(1)
    h = job.gold_performance_history(reviews([(1, "2024-H2", 5), (1, "2025-H1", 3)], before_hire=set(flagged)), people)
    row = job.gold_attrition_risk(people, talent([(1, "Low", "Not Ready", "Low")]), h,
                                  job.gold_learning_summary(events([]), people, AS_OF)).iloc[0]
    assert bool(row.trend_uses_pre_hire_review) is expected
    assert row.performance_risk == "High"  # the signal is still computed; the flag qualifies it
