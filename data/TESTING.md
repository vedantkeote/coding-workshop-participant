# talent-pipeline — Test Evidence

Commands, results and conclusions for every test category in
[docs/data-engineer.md](../docs/data-engineer.md#testing-expectations).
All results below were produced locally (PostgreSQL 18 + LocalStack) on 2026-10-15.

## 1. How to run

From the repository root:

```sh
cd data/talent-pipeline

# one-time: isolated environment with the exact pinned versions
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

# every session: LocalStack keeps data in memory, so re-seed after a restart
localstack start -d && ./scripts/seed_sources.sh

.venv/bin/pytest -q                                   # all 135 tests (~55 s, needs Postgres + LocalStack)
.venv/bin/pytest -q -m "not integration"              # 103 pure unit tests (~5 s, no services needed)
.venv/bin/pytest --cov=job --cov-report=json:.pytest_cache/coverage.json -q \
  && .venv/bin/python scripts/coverage_by_area.py .pytest_cache/coverage.json   # vs guide targets
.venv/bin/python scripts/perf_test.py --scales 1 5 20 # performance (local only)
```

The guide's own example works unchanged (`job.py` reads all config from environment variables):

```sh
pip install -r ../data/talent-pipeline/requirements.txt
python ../data/talent-pipeline/job.py 2>&1 | tee /tmp/talent-pipeline.log
```

## 2. Results summary

| Check | Result |
|---|---|
| Test suite | **135 passed** (103 unit + 32 integration/end-to-end) |
| Coverage, whole `job.py` | **97.3 %** |
| Coverage targets (4 areas) | **all met** — see §5 |
| Largest volume tested | **20× (840 k records) in 23 s**, peak 1.55 GB |
| Simulated EKS pod (1 CPU, 2 GiB, Python 3.11, same image) | **passed at 20×**, peak 1.50 GB |
| Independent recomputation of Gold from raw files | **all match** |
| Both notebooks execute against a fresh lake | **pass** (`test_notebook.py`: insights 8 charts, lake explorer 6) |
| Both notebooks × {VS Code, JupyterHub image} × {LocalStack, AWS lake} | **8 / 8 pass**, AWS findings identical to local — see §4.4 |

Tests per file: `test_foundation` 20 · `test_bronze` 20 · `test_silver` 47 · `test_gold` 25 ·
`test_lineage` 9 · `test_resilience` 7 · `test_e2e` 5 · `test_notebook` 2.

## 3. Functional testing (guide §Functional Testing)

| # | Guide requirement | Evidence (tests) | Result |
|---|---|---|---|
| 1 | **Source ingestion validation** — expected row counts and required columns in Bronze | `test_bronze.py::test_bronze_postgres_matches_source_with_lineage` (every table: rows = source, columns = source + lineage), `test_bronze_files_are_byte_identical_with_lineage_metadata`, `test_bronze_keeps_source_types`, `test_landing_key_contract` | Bronze = source for all 7 tables; all 783 files byte-identical (sizes match the export on disk) |
| 2 | **Transformation validation** — cleaning, casting, filtering, dedup in Silver | `test_silver.py`: `test_coerce_*` (strict int/date/timestamp casting), `test_validate_*`, `test_learning_event_rules` (11 parametrized rules), `test_field_alias_maps_notes_to_text`, `test_learning_events_dedupe_keeps_first_line`, `test_dim_employee_joins_team_and_computes_tenure` | All pass |
| 3 | **Aggregation validation** — Gold metrics vs known expected values | `test_gold.py` (each definition at its boundaries, e.g. gap 1 vs 2, rating 3 vs 4, drop 1 vs 2) and `test_e2e.py::test_gold_matches_independent_computation` — recomputes Gold **from the raw CSV/JSON with plain pandas, no pipeline code**: every team's average rating, every competency×department average, every employee's completed-course count, promotion tiers (357 / 166), critical gaps (325), hidden risk (6) | All match |
| 4 | **Data quality validation** — invalid rows quarantined, rules enforced | `test_silver_reconciles_and_routes_bad_rows`, `test_employee_contract_rejects`, `test_review_contract_enforces_period_rating_and_one_review_per_period`, `test_resilience.py` (file- and table-level schema drift) | Real data: 294 events quarantined, matching profiling exactly (151 duplicates, 76 null employee, 39 unparseable + 15 empty dates, 13 malformed JSON) |
| 4b | **Cross-table consistency** (added after manual review of the data) — records dated before the employee's hire date are flagged, not dropped | `test_silver.py::test_review_before_hire_boundaries` (hired on the period's last day / the day after / mid-period), `test_flag_before_hire_handles_empty_input`, `test_gold.py::test_*pre_hire*`, `test_e2e.py::test_records_before_hire_match_independent_count` | 666 reviews, 234 assessments, 5,161 events flagged — equal to an independent count from the raw files; all 4,000 reviews still present |
| 5 | **Idempotency validation** — run twice, no duplicates | Per layer: `test_bronze_rerun_is_idempotent_and_incremental`, `test_silver_rerun_is_idempotent`, `test_gold_end_to_end_and_idempotent`; whole pipeline: `test_e2e.py::test_idempotency_second_run_produces_identical_gold` (all 7 Gold tables `assert_frame_equal` across two `main()` runs) | Identical output; second run copies 0 of 783 files |

End-to-end smoke test (guide: *"100% for at least one representative flow"*):
`test_e2e.py::test_smoke_full_pipeline_succeeds_and_publishes` runs `main()` exactly as the
cloud job does, on the full real dataset → exit 0, manifest `succeeded` with
preflight → bronze → silver → gold, `latest.json` updated.

## 4. Performance testing (guide §Performance Testing)

Command: `.venv/bin/python scripts/perf_test.py --scales 1 5 20`. For each scale N the
script builds an N-times copy of the dataset **with new unique ids and the bad records
kept**, loads it into its own Postgres schema and landing bucket, runs `job.py` as a
child process into a fresh lake, and measures that process (`os.wait4`). Machine:
2 vCPU, 7.6 GB RAM.

| Scale | Records | Cold run | Warm rerun | CPU | Peak memory | Records/min | Bronze | Silver | Gold | Quarantine = 294×N | SLA (≤ 10 min) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1× | 42,041 | 7.0 s | 5.3 s | 4.8 s | 651 MB | 360,543 | 2.3 s | 3.3 s | 0.9 s | yes (294) | pass |
| 5× | 210,045 | 9.3 s | 7.8 s | 7.3 s | 825 MB | 1,354,072 | 2.4 s | 4.5 s | 1.9 s | yes (1,470) | pass |
| 20× | 840,060 | 23.4 s | 18.3 s | 17.3 s | 1,551 MB | 2,155,584 | 3.1 s | 9.9 s | 9.7 s | yes (5,880) | pass |

| # | Guide requirement | Finding |
|---|---|---|
| 1 | **Batch throughput** | 0.36 M records/min at 1× rising to 2.2 M/min at 20× — fixed start-up cost (imports, connections) dominates small runs. |
| 2 | **Scale test** (nonlinear slowdowns) | **None in time**: 20× data takes 3.3× as long. Quarantine counts scale *exactly* ×N, so correctness holds at volume. Memory is the constraint — see below. |
| 3 | **Resource efficiency** (memory, CPU) | Every step now logs `cpu_s` and `peak_rss_mb` (also in the run manifest). Silver is the memory peak. A real problem was found and fixed — next section. |
| 4 | **External dependency** (retry/backoff) | `test_resilience.py`: Postgres fails twice then recovers; S3 returns 503 twice then recovers; persistent 503 gives up after exactly 5 attempts; 403 fails immediately (no retries) with an IAM hint. `test_foundation.py`: backoff bounds, jitter cap, permanent errors not retried. |
| 5 | **SLA verification** | No SLA was specified; **assumed 10 minutes** for a daily batch. Worst measured: 23 s at 20× (≈ 4 % of the window). |

### 4.1 Memory: problem found and fixed

The EKS job pod is limited to **2 GiB** (`infra/helm/values.yaml`); above that Kubernetes
kills it (OOMKilled). The first 20× run peaked at **2,024 MB — 24 MB from being killed**.

Profiling each phase (RSS after every stage at 20×) showed the peak was inside JSONL
parsing: one Python `dict` per line (≈600 k dicts) plus the DataFrame built from them.

| Change | Peak at 20× |
|---|---|
| Original | 2,024 MB |
| `parse_jsonl` collects column lists instead of per-row dicts; downloads streamed into the parser (each file's bytes released once parsed) | 1,782 MB |
| `validate` attaches lineage columns instead of joining (pandas copy-on-write: no full copy); raw text dropped from valid rows; intermediates freed early | **1,524 MB** (−25 %) |

All 117 tests pass unchanged after each change; outputs identical.

### 4.2 Simulated EKS pod

Same image (`jupyter/all-spark-notebook`, Python 3.11.6), same limits (1 CPU, 2 GiB, no
swap), same command as `infra/helm/templates/python-job.yaml`
(`pip install -r requirements.txt && python job.py`), run locally with `docker run`
against the 20× data:

| Step | Duration | CPU | Peak memory |
|---|---|---|---|
| preflight | 0.02 s | 0.01 s | 155 MB |
| bronze | 0.9 s | 0.6 s | 319 MB |
| silver | 9.8 s | 8.6 s | 1,497 MB |
| gold | 6.8 s | 6.4 s | 1,497 MB |

Exit 0, ≈ 550 MB headroom. This run also exposed an incompatibility: the image ships
NumPy-1 builds of `numexpr`/`bottleneck` (pandas' optional accelerators), which failed to
load under NumPy 2 (pandas fell back silently, with long tracebacks in the log). Fixed by
pinning NumPy-2-compatible versions in `requirements.txt`. One harmless pip notice remains:
the image's own `scipy`/`contourpy` (notebook libraries the job never imports) prefer NumPy 1.

### 4.3 Cloud run (AWS, EKS)

Run `20261015T085735Z-0e8677b4` on 2026-10-15: `status: succeeded`, Python 3.11.6, `is_local: false`,
7 Gold tables written to `s3://coding-workshop-datalake-<id>/gold/`.

| Step | Cloud | Local | Cloud peak memory |
|---|---|---|---|
| preflight | 12.6 s | 0.02 s | 163 MB |
| bronze | 6.5 s | 0.5 s | 214 MB |
| silver | 9.8 s | 3.9 s | 644 MB |
| gold | 4.9 s | 1.1 s | 644 MB |

All 53 metrics shared by the cloud and local manifests are equal (rows per table, 29,574 valid / 294 quarantined
events, 666 / 5,161 records flagged before hire, 1 empty file, Gold row counts) except `learning_bytes_copied`
(the cloud run copied the files fresh; the local reference was an incremental rerun). Cloud steps are slower because
of network round-trips to S3 and Aurora; preflight includes Aurora resuming from auto-pause.

Getting there exposed four infrastructure issues, each fixed in `infra/` with approval (see README → Cloud
Deployment): no environment variables reached the Helm job; the node role had no S3 access; pods couldn't reach
Aurora (security groups); pods couldn't obtain credentials (IMDSv2 hop limit 1).

### 4.4 Notebooks: local and AWS

Both notebooks were executed end-to-end (every cell, `nbconvert --execute`) in every way they can be run, on
2026-10-16. "VS Code" is the project's `.venv` kernel (pandas 3.0.6, pyarrow 24); "JupyterHub image" is the
`jupyter/all-spark-notebook` image the cluster's notebook servers use (Python 3.11, pandas 2.0.3, pyarrow 13).

| Notebook | VS Code + LocalStack | VS Code + AWS | JupyterHub image + LocalStack | JupyterHub image + AWS |
|---|---|---|---|---|
| `insights.ipynb` | ✅ 8 charts | ✅ 8 charts | ✅ 8 charts | ✅ 8 charts |
| `lake_explorer.ipynb` | ✅ 6 charts | ✅ 6 charts | ✅ 6 charts | ✅ 6 charts |

- **Same answers in both lakes:** every written finding in `insights.ipynb` is identical between the local and the
  AWS run; the only differences are run ids, timestamps, step timings and the Python version recorded in the manifest.
  In the AWS lake the explorer's live proofs also pass: Bronze file byte-for-byte equal to the AWS landing bucket,
  quarantine row equal to its raw Bronze line, every dataset `read = valid + quarantined`.
- **"JupyterHub + AWS"** is simulated: the image runs on the VDI with `JUPYTERHUB_USER` set (so cloud mode is
  auto-detected) and the variables the deploy passes to notebook servers (`IS_LOCAL`, `AWS_REGION`,
  `DATALAKE_BUCKET`), with the workshop credentials. On the real cluster the credentials come from the node role
  instead. Those settings are deployed (`helm get values jupyterhub`, checked 2026-10-16); the notebooks have not
  been run on the real JupyterHub yet: it rejected this account (`User 'u77a23259' not allowed` — password accepted,
  account not in the participant's LDAP group). The allow-list fix in `infra/eks.tf` takes effect on the next deploy.
- **Edited and failure paths** (all behave as intended): explorer §7 with a run manifest, raw event files, a filter
  plus column list, and an unknown dataset (lists what exists); §8 with an unknown employee; insights lookup by ID
  and by name; JupyterHub without the deploy's variables (stops with "Which data lake? Set DATALAKE_BUCKET…", and
  works once the bucket is typed in — the region is looked up); AWS without credentials (stops with the credentials
  hint and the underlying `ACCESS_DENIED`).

## 5. Coverage (guide §Test Coverage Goals)

`job.py` is a single file (the Helm chart ships only that file), so
`scripts/coverage_by_area.py` maps each function to a guide area and sums its line coverage.

| Area | Target | Actual | Lines |
|---|---|---|---|
| Core transformation functions | 80 % | **98.0 %** | 289/295 |
| Schema and validation rules | 90 % | **98.6 %** | 146/148 |
| Error-handling and retry paths | 85 % | **98.4 %** | 62/63 |
| Critical business metric calculations | 95 % | **100 %** | 102/102 |
| End-to-end smoke test | 100 % of one flow | **full flow** via `main()` | — |
| Whole `job.py` | — | 97.3 % | — |

## 6. Bugs the tests caught during development

| Found by | Bug | Fix |
|---|---|---|
| Bronze full run | 16 copy threads, 10 pooled connections → "connection pool full" warnings | pool sized to thread count |
| `test_silver` | unknown event type also reported `unexpected_score` (cascading reason) | rule limited to known types |
| `test_gold` edge cases | crash for an employee with a single review (NA comparison in `np.select`) | `flag()` helper; explicit `Unknown` |
| `test_silver` reconciliation | Silver row counts silently missing from metrics (a line ended up unreachable during an edit) | moved back into `_publish` |
| `test_resilience` | boto3 `max_attempts` counts retries (6 total attempts, not 5) | `total_max_attempts=5`, consistent with Postgres/pyarrow |
| `perf_test` 20× | peak memory 24 MB below the pod limit | §4.1 |
| Pod simulation | NumPy-1 accelerators in the image | §4.2 |
| Manual data review (user) | ~17 % of reviews/events dated before the employee's hire date — passed every single-table check | `recorded_before_hire` flag (LINEAGE.md §4.4), notebook section 9 |
| Local-deploy verification | one landing file (`2025-09-03`) is 0 bytes in the source; ingested correctly but **silently** (guide §7 "Zero Rows Written" asks for a warning) | per-file warning + `learning_files_empty` metric; `test_bronze_warns_about_empty_landing_files_but_still_ingests_them`; notebook section 10 |
| `test_resilience` | the new flag crashed Silver when a run has **zero** learning events (empty date column became float) | keep the DATE type (`set_axis`, not `to_numpy`) |
| `test_gold` | with zero events, an empty untyped mask was read as a *column selection* | mask forced to bool via `flag()` |

## 7. Conclusions and limitations

- **Correctness**: every layer reconciles (`read = silver + quarantine`; Gold grain,
  row counts and totals checked before publishing), and Gold agrees with an independent
  computation from the raw files.
- **Idempotency**: proven per layer and for the whole pipeline.
- **Performance**: comfortably inside the assumed SLA at 20× volume; time scales
  sub-linearly.
- **Memory is the scaling limit.** Silver rebuilds `learning_events` from all of Bronze
  in memory, so peak memory grows ≈ 45 MB per 1× of event history. With the 2 GiB pod
  limit the safe envelope is roughly **25× today's volume**. Beyond that, the fix is to
  process events incrementally (per month partition, carrying a set of seen `event_id`s
  for deduplication) or to raise the pod memory in `infra/helm/values.yaml` (an infra
  change, to be agreed first).
- **Cloud**: the real AWS run succeeded and matched local on every data metric (§4.3), so IAM,
  Aurora TLS and EKS networking were exercised for real; both notebooks read the AWS lake (§4.4).
- **Not covered**: pyarrow's own S3 retry strategy (configured, not fault-injected); the
  notebooks on the real JupyterHub (settings deployed; login fixed in `infra/eks.tf`, pending the next deploy) — tested in its image instead.
