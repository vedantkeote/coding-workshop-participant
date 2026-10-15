"""Performance tests from docs/data-engineer.md: batch throughput, scale (small/medium/large),
resource efficiency (CPU, peak memory) and SLA verification.

For each scale factor N it builds an N-times copy of the reference dataset with new, unique
ids (bad records included, so quarantine counts must scale exactly by N), loads it into its
own Postgres schema + LocalStack landing bucket, runs job.py as a subprocess into a fresh
lake bucket, and measures wall time, CPU time and peak memory (RSS) of that process.

LOCAL ONLY - talks to local Postgres and LocalStack, deploys nothing.

Usage (from data/talent-pipeline/):
    .venv/bin/python scripts/perf_test.py --scales 1 5 20 [--keep] [--json results.json]
"""

from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import pandas as pd
import psycopg2

HERE = Path(__file__).resolve().parents[1]
DATASET = Path(os.environ.get("SOURCE_DATA_DIR", HERE.parents[1]
                              / "sampleData/data-engineering-performance-talent-management"))
ENDPOINT = "http://localhost.localstack.cloud:4566"
PG = dict(host="localhost", port=5432, dbname="postgres", user="postgres", password="postgres123")
OFFSET = 1_000_000          # id offset per copy (ids stay unique across copies)
EVENT_OFFSET = 100_000_000  # event ids: EV + 10 digits, so copies k < 100 still fit
SLA_SECONDS = 600           # agreed daily-batch window for this exercise (see TESTING.md)
EXPECTED_QUARANTINE = 294   # learning_events rejects in the reference dataset


def s3():
    return boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test")


def scaled_tables(n: int) -> dict[str, pd.DataFrame]:
    """N copies of every table with shifted ids (competencies are reference data: not copied)."""
    out = {}
    for name in ("teams", "competencies", "employees", "projects", "performance_reviews",
                 "competency_assessments", "talent_review"):
        df = pd.read_csv(DATASET / f"{name}.csv")
        if name == "competencies":
            out[name] = df
            continue
        copies = []
        for k in range(n):
            c = df.copy()
            for col in ("team_id", "employee_id", "project_id", "review_id", "assessment_id"):
                if col in c:
                    c[col] = c[col] + k * OFFSET
            if name == "employees":
                c["email"] = c["email"].str.replace("@", f"+{k}@", regex=False)
            copies.append(c)
        out[name] = pd.concat(copies, ignore_index=True)
    return out


def load_postgres(schema: str, tables: dict[str, pd.DataFrame]) -> None:
    ddl = (DATASET / "schema.sql").read_text()
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}; SET search_path TO {schema};")
        cur.execute(ddl)
        for name, df in tables.items():  # dict order = schema.sql load order
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            cur.copy_expert(f"COPY {name} FROM STDIN WITH (FORMAT csv, HEADER true)", buf)


def scale_line(line: str, k: int) -> str:
    """Shift ids in one JSONL line; unparseable lines are kept verbatim (they must stay bad)."""
    if k == 0:
        return line
    try:
        e = json.loads(line)
    except json.JSONDecodeError:
        return line
    if isinstance(e.get("event_id"), str) and e["event_id"][2:].isdigit():
        e["event_id"] = f"EV{int(e['event_id'][2:]) + k * EVENT_OFFSET:010d}"
    if e.get("employee_id") is not None:
        e["employee_id"] = str(int(e["employee_id"]) + k * OFFSET)
    return json.dumps(e)


def load_landing(bucket: str, n: int) -> int:
    client = s3()
    try:
        client.create_bucket(Bucket=bucket)
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    files = sorted((DATASET / "learning_activity").rglob("*.jsonl"))
    lines_total = 0

    def put(path: Path) -> int:
        lines = path.read_text().splitlines()
        body = "\n".join(scale_line(line, k) for k in range(n) for line in lines) + "\n"
        client.put_object(Bucket=bucket, Key=str(path.relative_to(DATASET)), Body=body.encode())
        return len(lines) * n

    with ThreadPoolExecutor(16) as pool:
        lines_total = sum(pool.map(put, files))
    return lines_total


def empty_bucket(bucket: str) -> None:
    client = s3()
    try:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
        client.delete_bucket(Bucket=bucket)
    except client.exceptions.NoSuchBucket:
        pass


def measure(env: dict[str, str]) -> dict:
    """Run job.py as a child process; return its wall time, CPU time, peak RSS and parsed logs.

    os.wait4 returns resource usage for THIS child only (RUSAGE_CHILDREN would report the
    max over every child so far, hiding smaller runs behind earlier bigger ones).
    """
    t0 = time.perf_counter()
    proc = subprocess.Popen([sys.executable, str(HERE / "job.py")], env={**os.environ, **env},
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = proc.stdout.read()
    _, status, usage = os.wait4(proc.pid, 0)
    wall = time.perf_counter() - t0
    proc.returncode = os.waitstatus_to_exitcode(status)
    logs = [json.loads(line) for line in out.splitlines() if line.startswith("{")]
    return {
        "exit": proc.returncode, "wall_s": wall,
        "cpu_s": usage.ru_utime + usage.ru_stime,
        "peak_rss_mb": usage.ru_maxrss / 1024,  # Linux reports KB
        "steps": {d["step"]: d for d in logs if d.get("msg") == "step finished"},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scales", type=int, nargs="+", default=[1, 5, 20])
    parser.add_argument("--keep", action="store_true", help="keep generated schemas/buckets")
    parser.add_argument("--json", help="write results to this file")
    args = parser.parse_args()
    participant = os.environ["PARTICIPANT_ID"]
    results = []

    for n in sorted(args.scales):
        schema = "hr" if n == 1 else f"hr_perf_x{n}"
        landing = f"coding-workshop-landing-{participant}" if n == 1 else f"coding-workshop-landing-perf-x{n}"
        lake = f"coding-workshop-datalake-perf-x{n}"
        print(f"\n=== scale x{n}: preparing data", flush=True)
        tables = scaled_tables(n)
        if n > 1:
            load_postgres(schema, tables)
            lines = load_landing(landing, n)
        else:
            lines = sum(len(p.read_text().splitlines()) for p in (DATASET / "learning_activity").rglob("*.jsonl"))
        table_rows = sum(len(df) for df in tables.values())
        empty_bucket(lake)
        env = {"IS_LOCAL": "true", "SOURCE_SCHEMA": schema, "LANDING_BUCKET": landing, "DATALAKE_BUCKET": lake,
               "RUN_DATE": "2026-10-15", "LOG_LEVEL": "INFO"}

        print(f"=== scale x{n}: cold run ({table_rows:,} table rows, {lines:,} event lines)", flush=True)
        cold = measure(env)
        print(f"=== scale x{n}: warm rerun", flush=True)
        warm = measure(env)

        silver = cold["steps"].get("silver", {})
        gold = cold["steps"].get("gold", {})
        records = table_rows + lines
        row = {
            "scale": n, "table_rows": table_rows, "event_lines": lines, "records": records,
            "exit_codes": [cold["exit"], warm["exit"]],
            "cold_wall_s": round(cold["wall_s"], 2), "warm_wall_s": round(warm["wall_s"], 2),
            "cold_cpu_s": round(cold["cpu_s"], 2), "peak_rss_mb": round(cold["peak_rss_mb"]),
            "warm_peak_rss_mb": round(warm["peak_rss_mb"]),
            "step_s": {k: v["duration_s"] for k, v in cold["steps"].items()},
            "records_per_min": round(records / cold["wall_s"] * 60),
            "quarantined": silver.get("learning_events_quarantined"),
            "quarantine_scales_exactly": silver.get("learning_events_quarantined") == EXPECTED_QUARANTINE * n,
            "gold_employees": gold.get("promotion_readiness_rows"),
            "within_sla": cold["wall_s"] <= SLA_SECONDS and cold["exit"] == 0,
        }
        results.append(row)
        print(json.dumps(row), flush=True)
        if not args.keep and n > 1:
            empty_bucket(landing)
            with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        if not args.keep:
            empty_bucket(lake)

    print("\n| Scale | Records | Cold run | Warm rerun | CPU | Peak RSS | Records/min | Bronze | Silver | Gold "
          "| Quarantine = 294×N | SLA |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in results:
        s = r["step_s"]
        print(f"| {r['scale']}× | {r['records']:,} | {r['cold_wall_s']}s | {r['warm_wall_s']}s | {r['cold_cpu_s']}s "
              f"| {r['peak_rss_mb']} MB | {r['records_per_min']:,} | {s.get('bronze')}s | {s.get('silver')}s "
              f"| {s.get('gold')}s | {'yes' if r['quarantine_scales_exactly'] else 'NO'} ({r['quarantined']:,}) "
              f"| {'pass' if r['within_sla'] else 'FAIL'} |")
    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2))
    return 0 if all(r["within_sla"] and r["quarantine_scales_exactly"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
