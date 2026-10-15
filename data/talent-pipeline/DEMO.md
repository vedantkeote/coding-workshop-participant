# talent-pipeline — Demo Playbook

What to do when the evaluator asks a question: the exact command, where to look, and what to say.
Every command below was run and checked against the reference dataset.

## 0. Before the session (about 10 minutes)

**Pick where you'll demo from.** Local always works; cloud only while the AWS environment is still running.

| | Local (LocalStack) | Cloud (AWS) |
|---|---|---|
| Prepare | `./bin/start-dev.sh` (leave running) → new terminal → `./data/talent-pipeline/scripts/seed_sources.sh` → run the job (below) | `./bin/setup-participant.sh` (fresh credentials — they expire) |
| Viewer commands | as written below | add `--cloud` to every `lake …` command |
| Notebook | VS Code, kernel *Python (talent-pipeline)*, **Run All** | same notebook locally, or JupyterHub (if login works) |

Run the job locally (from the repository root):

```sh
cd infra && source ../data/talent-pipeline/.venv/bin/activate
python ../data/talent-pipeline/job.py 2>&1 | tee /tmp/talent-pipeline.log      # ends with "status": "succeeded"
```

**Have these open:**

1. A terminal in `data/talent-pipeline` with a short alias for the viewer:
   ```sh
   cd ~/coding-workshop-participant/data/talent-pipeline
   alias lake='.venv/bin/python scripts/inspect_lake.py'
   ```
2. `notebooks/insights.ipynb` after **Run All** (all 8 charts visible).
3. `README.md` (architecture diagram at the top) and `LINEAGE.md`.

---

## 1. "Show me the Bronze / Silver / Gold data"

**Start with the overview** — one command shows every dataset in every layer with row counts:

```sh
lake --list
```

Say: *"The lake is one S3 bucket with a folder per layer. Bronze holds the raw copies, Silver the cleaned
data, quarantine the rejected rows, Gold one table per business question, and `_runs` a record of every run."*

### Bronze — raw copies, never modified

```sh
lake bronze/postgres/employees --rows 3          # a source table snapshot
lake bronze/learning_activity --rows 3           # the raw event files, exactly as received
```

Point at:
- the **same columns as the PostgreSQL table**, plus `_run_id`, `_ingested_at`, `_source` (lineage) and the
  `ingest_date=` partition — one snapshot per day; earlier days are never changed;
- the event files are **raw text**, byte-for-byte copies — the 13 broken lines are still in here, on purpose.

### Silver — cleaned and validated

```sh
lake silver/employees --rows 3
lake silver/learning_events --columns event_id,employee_id,event_ts,event_date,event_type,score,recorded_before_hire --rows 3
lake quarantine/learning_events --rows 5
```

Point at:
- **real types** (`int64`, `date32`, `timestamp`) where Bronze events were text;
- `event_date` was renamed `event_ts` (it's a timestamp); a real `event_date` is derived;
- `recorded_before_hire` — the cross-table flag (records dated before the employee was hired);
- events are partitioned by month (37 folders);
- **quarantine**: 294 rejected rows, each with `reject_reason`, the original `raw_record`, and the exact
  `_source_file` / `_source_line` it came from. Nothing is silently dropped: `read = Silver + quarantine`.

### Gold — business answers

```sh
lake gold/promotion_readiness --rows 5
```

### Strongest demo: one record through every layer

```sh
lake bronze/postgres/performance_reviews --where employee_id=1332 --columns review_id,employee_id,review_period,rating,submitted_date
lake silver/performance_reviews          --where employee_id=1332 --columns review_id,employee_id,review_period,rating,recorded_before_hire
lake gold/employee_performance_history   --where employee_id=1332 --columns full_name,review_period,rating,previous_rating,rating_change,recorded_before_hire
```

Say: *"Bronze is the source as-is; Silver validated it and flagged that both reviews pre-date this employee's
hire date (2026-08-02); Gold adds the business view — the previous rating and the change."*

(To show it in the AWS console instead: S3 → `coding-workshop-datalake-<id>` → browse the folders. The
console can't display Parquet rows — that's what the viewer and the notebook are for.)

---

## 2. The seven business questions

For each: open the notebook section (chart + written finding + table), or run the command for the raw Gold rows.

| Question | Notebook | Terminal |
|---|---|---|
| **Q1** Ratings and review history for each employee | §1 (overall) and **§8 lookup** (one person: set `EMPLOYEE_ID = 1332`, Shift+Enter) | `lake gold/employee_performance_history --where employee_id=1332 --columns full_name,review_period,rating,previous_rating,rating_change` |
| **Q2** Critical skill gaps | §2 | `lake gold/employee_skill_gaps --where is_critical_gap=True --columns full_name,department,competency_name,proficiency_level,required_level,gap_levels --rows 10` |
| **Q3** High-potential, ready for promotion | §3 | `lake gold/promotion_readiness --where promotion_tier=ready_strong --columns full_name,department,job_level,latest_rating,critical_gap_count --rows 10` |
| **Q4** Training completed per employee | §4 and §8 | `lake gold/employee_learning_summary --where employee_id=1332 --columns full_name,courses_completed,certifications_earned,avg_quiz_score,completed_courses` |
| **Q5** Attrition risk from performance trends | §5 | `lake gold/attrition_risk --where hidden_risk=True --columns full_name,department,previous_rating,latest_rating,hr_attrition_risk,performance_risk` |
| **Q6** Performance vs team and project outcomes | §6 | `lake gold/team_performance_outcomes --where has_projects=True --columns team_name,avg_latest_rating,project_count,avg_project_outcome_score,on_time_rate --rows 27` |
| **Q7** Distribution of skills | §7 | `lake gold/skill_distribution --where department=Sales --columns competency_name,category,employees_assessed,avg_proficiency,meeting_requirement_rate --rows 10` |

**What to say** (numbers from the reference data):

- **Q1** — Every employee has two reviews (2024-H2, 2025-H1). Average rating rose from 3.02 to 3.41; 45 % improved,
  12 % declined, 44 % held steady; 84 people dropped two or more points.
- **Q2** — "Critical" = proficiency 2+ levels below what the role requires (our agreed definition).
  325 critical gaps across 317 employees; most common in Strategic Thinking, Stakeholder Management,
  Presentation Skills. The rate is similar across categories (7.8–8.3 %), so gaps are spread out.
- **Q3** — "Ready" = HR potential *High* + readiness *Ready Now*; "strong" also needs a latest rating of 4+.
  166 strong, 191 more ready, 137 on the 1–2-year bench. **34 strong candidates still have a critical skill gap.**
- **Q4** — Median 2 courses completed; 339 employees completed none; 1,764 certifications. We report counts,
  not a completion *rate*: most completions in this data have no matching enrollment, so a rate would mislead.
- **Q5** — Two signals: HR's label and the rating trend (fell 2+ = High). 341 employees are high risk on at least one.
  **6 "hidden risks"**: HR says Low, but their rating fell sharply — the people most likely to be missed.
- **Q6** — r = 0.24 between team rating and project outcome, but a shuffle test gives p = 0.22: **not
  distinguishable from chance**. Only 27 of 100 teams have projects. Say this plainly — it's an honest "we can't tell".
- **Q7** — 75 % of assessments meet the required level. Weakest: Stakeholder Management (65 %); strongest:
  Vendor Management (82 %). The heatmap shows coverage by department and category.

---

## 3. Other questions you're likely to get

| Question | What to do |
|---|---|
| **"Run the pipeline."** | `python ../data/talent-pipeline/job.py` (from `infra/`). Point at the JSON log: each step's rows, duration, memory; the expected data-quality warnings. |
| **"Is it idempotent? What happens if you run it twice?"** | Run it again: Bronze logs `"copied": 0, "unchanged": 783`; `lake --list` shows identical counts. Proven by `tests/test_e2e.py` (Gold compared table-for-table across two runs). |
| **"How do you handle bad data?"** | `lake quarantine/learning_events --rows 10`. Reasons: duplicates 151, missing employee 76, bad dates 54, malformed JSON 13. Structural problems (a missing column) stop the run *before* Silver/Gold change and raise an alert. All reason codes: `LINEAGE.md` §6. |
| **"Show me lineage."** | `lake _runs/latest.json` → the latest complete run; open its manifest (`lake _runs` shows it): status, every step's counts, code SHA-256, package versions. Row level: `lake silver/employees --columns employee_id,_bronze_run_id,_run_id,_source --rows 3`. Column level: `LINEAGE.md`. |
| **"What data-quality checks run at each layer?"** | Bronze: row counts + file size/checksum vs source. Silver: schema contracts (types, required, allowed values, ranges, formats, keys, foreign keys) + `read = silver + quarantine`. Gold: unique grain, no dropped employees, totals reconcile with Silver. (README → How the Pipeline Works.) |
| **"What surprised you in the data?"** | Records dated **before the hire date** (17 % of reviews/events — flagged, kept, shown with/without in notebook §9); an **empty landing file** (2025-09-03); a field renamed in one file (`notes`→`text`); the quiz pass mark is 55, not 60. |
| **"Show me the tests."** | `.venv/bin/pytest -q -m "not integration"` (103 tests, ~5 s) or `.venv/bin/pytest -q` (134, ~55 s). Coverage vs targets: `TESTING.md` §5. Performance: `TESTING.md` §4 (20× volume in 23 s). |
| **"How is it deployed in the cloud?"** | README → Cloud Deployment: EKS job via Helm; four infra fixes (env vars, S3 policy, Aurora security group, IMDS hop limit). Evidence: `TESTING.md` §4.3 — cloud run identical to local on all 53 metrics. If the cluster is up: `kubectl get jobs`, `kubectl logs job/coding-workshop-talent-pipeline`. |
| **"Why pandas and not Spark? Why one file?"** | 14 MB of data — Spark's overhead would exceed the work; the guide recommends pandas. One file because the workshop's Helm chart ships only `job.py` + `requirements.txt`. (README → Design Decisions.) |
| **"What would you do next?"** | Process Silver incrementally (memory grows with history; safe to ~25× today); confirm `hire_date` with the data owner; per-pod IAM if allowed; fix JupyterHub login. (README → Known Limitations.) |

---

## 4. If something goes wrong mid-demo

| Symptom | Fix |
|---|---|
| Viewer: "LocalStack isn't running" | `./bin/start-dev.sh` → `seed_sources.sh` → run the job (≈2 min) — or switch to `--cloud` |
| Viewer/job: landing bucket not found (404) | LocalStack restarted and lost its data → `seed_sources.sh`, then the job |
| `--cloud`: "AWS rejected your credentials" | `./bin/setup-participant.sh` (credentials are temporary) |
| Notebook: "The data isn't loaded yet" | Run the first Setup cell (or Run All) |
| Notebook: `FileNotFoundError … latest.json` | No successful run in this lake yet → run the job |
| `lake: command not found` | Re-create the alias (section 0) or use `.venv/bin/python scripts/inspect_lake.py` |
