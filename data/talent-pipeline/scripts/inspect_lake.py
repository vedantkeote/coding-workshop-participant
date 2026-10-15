"""Read-only viewer for the data lake: show any Bronze / Silver / Gold / quarantine dataset.

Prints where a dataset lives, its row count, columns and types, and the first rows - the quick
way to answer "show me the Silver layer". Works against LocalStack (default) or real AWS (--cloud).
Never writes anything.

Usage (from data/talent-pipeline/, with the virtual environment):
    .venv/bin/python scripts/inspect_lake.py --list                     # every dataset, with row counts
    .venv/bin/python scripts/inspect_lake.py silver/employees           # one dataset (local)
    .venv/bin/python scripts/inspect_lake.py bronze/postgres/teams --cloud
    .venv/bin/python scripts/inspect_lake.py quarantine/learning_events --rows 10
    .venv/bin/python scripts/inspect_lake.py bronze/learning_activity   # raw JSONL lines
    .venv/bin/python scripts/inspect_lake.py _runs/latest.json          # a run manifest pointer
    .venv/bin/python scripts/inspect_lake.py gold/employee_performance_history --where employee_id=1332
    .venv/bin/python scripts/inspect_lake.py silver/employees --columns employee_id,first_name,hire_date

Cloud mode uses your workshop credentials (written by ./bin/setup-participant.sh); they are temporary,
so re-run that script if you see "expired".
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.exceptions import ClientError, NoCredentialsError
from pyarrow.fs import S3FileSystem  # pyright: ignore[reportPrivateImportUsage]

LOCAL_ENDPOINT = "localhost.localstack.cloud:4566"


def connect(cloud: bool):
    """Return (boto3 client, pyarrow filesystem, bucket, where-label) for local or cloud."""
    bucket = os.environ.get("DATALAKE_BUCKET") or f"coding-workshop-datalake-{os.environ.get('PARTICIPANT_ID', '')}"
    if bucket.endswith("-"):
        sys.exit("Set PARTICIPANT_ID (source ~/.bashrc) or DATALAKE_BUCKET.")
    if cloud:
        for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"):  # make sure we talk to real AWS
            os.environ.pop(var, None)
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if not region:
            sys.exit("Set AWS_REGION (source ~/.bashrc).")
        s3 = boto3.client("s3", region_name=region)
        fs = S3FileSystem(region=region)
        return s3, fs, bucket, f"AWS S3 ({region})"
    s3 = boto3.client("s3", endpoint_url=f"http://{LOCAL_ENDPOINT}", region_name="us-east-1",
                      aws_access_key_id="test", aws_secret_access_key="test")
    fs = S3FileSystem(endpoint_override=LOCAL_ENDPOINT, scheme="http", region="us-east-1",
                      access_key="test", secret_key="test")
    return s3, fs, bucket, "LocalStack"


def list_files(s3, bucket: str, prefix: str) -> list[dict]:
    files = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        files += [o for o in page.get("Contents", []) if not o["Key"].endswith("/")]
    return files


def dataset_of(key: str) -> str:
    """Map a file key to its dataset: bronze/postgres/<t>, bronze/learning_activity, <layer>/<entity>, _runs."""
    parts = key.split("/")
    if parts[0] == "bronze" and parts[1] == "postgres":
        return "/".join(parts[:3])
    if parts[0] == "_runs":
        return "_runs"
    return "/".join(parts[:2])


def show_list(s3, fs, bucket: str) -> None:
    groups: dict[str, list[dict]] = defaultdict(list)
    for obj in list_files(s3, bucket, ""):
        groups[dataset_of(obj["Key"])].append(obj)
    print(f"{'dataset':42} {'files':>6} {'rows':>9} {'size':>9}")
    for name in sorted(groups, key=lambda n: (["bronze", "silver", "quarantine", "gold", "_runs"].index(n.split("/")[0])
                                              if n.split("/")[0] in ("bronze", "silver", "quarantine", "gold", "_runs")
                                              else 9, n)):
        files = groups[name]
        parquet = [f for f in files if f["Key"].endswith(".parquet")]
        rows = sum(pq.read_metadata(fs.open_input_file(f"{bucket}/{f['Key']}")).num_rows for f in parquet)
        size = sum(f["Size"] for f in files)
        print(f"{name:42} {len(files):>6} {rows if parquet else '-':>9} {size / 1024:>8.0f}K")


def show_dataset(s3, fs, bucket: str, dataset: str, rows: int, where: list[str] | None = None,
                 columns: str | None = None) -> None:
    dataset = dataset.strip("/")
    files = list_files(s3, bucket, dataset if dataset.endswith((".json", ".jsonl")) else dataset + "/")
    if not files:
        sys.exit(f"Nothing at s3://{bucket}/{dataset}/  - try --list to see what exists.")
    print(f"s3://{bucket}/{dataset}/  ({len(files)} file(s), {sum(f['Size'] for f in files) / 1024:.0f} KB)")

    if all(f["Key"].endswith(".json") for f in files):          # run manifests / latest.json
        body = s3.get_object(Bucket=bucket, Key=files[-1]["Key"])["Body"].read()
        print(f"\n{files[-1]['Key']}:\n{json.dumps(json.loads(body), indent=2)[:4000]}")
        return
    if all(f["Key"].endswith(".jsonl") for f in files):         # Bronze learning files: raw text
        non_empty = [f for f in files if f["Size"] > 0]
        first = non_empty[0] if non_empty else files[0]
        lines = s3.get_object(Bucket=bucket, Key=first["Key"])["Body"].read().decode("utf-8", "replace").splitlines()
        print(f"Raw JSONL (stored unchanged). First {rows} line(s) of {first['Key']}:\n")
        for line in lines[:rows]:
            print("  " + line)
        print(f"\nPartition folders (year=/month=/day=): {len({f['Key'].rsplit('/', 1)[0] for f in files})}")
        return

    table = pq.read_table(f"{bucket}/{dataset}", filesystem=fs)
    print(f"rows: {table.num_rows:,}\n\ncolumns:")
    for field in table.schema:
        note = "  <- lineage (added by the pipeline)" if field.name.startswith("_") else ""
        print(f"  {field.name:32} {str(field.type):28}{note}")
    folders = sorted({f["Key"][len(dataset) + 1:].rsplit("/", 1)[0] for f in files if "=" in f["Key"]})
    if folders:
        print(f"\npartitions ({len(folders)}): {', '.join(folders[:5])}{' ...' if len(folders) > 5 else ''}")
    df = table.to_pandas()
    for condition in where or []:                      # e.g. employee_id=1332 (compared as text)
        col, _, value = condition.partition("=")
        if col not in df.columns:
            sys.exit(f"--where: no column {col!r}. Columns: {', '.join(df.columns)}")
        df = df[df[col].astype(str) == value]
    if columns:
        wanted = [c.strip() for c in columns.split(",") if c.strip()]
        missing = [c for c in wanted if c not in df.columns]
        if missing:
            sys.exit(f"--columns: unknown {missing}. Columns: {', '.join(df.columns)}")
        df = df[wanted]
    label = f"{len(df):,} row(s) matching {' and '.join(where)}" if where else f"first {rows} row(s)"
    with pd.option_context("display.max_columns", None, "display.width", 220, "display.max_colwidth", 40):
        print(f"\n{label}{' (showing ' + str(rows) + ')' if where and len(df) > rows else ''}:")
        print(df.head(rows).to_string(index=False) if len(df) else "  (none)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("dataset", nargs="?", help="e.g. silver/employees, bronze/postgres/teams, gold/attrition_risk")
    parser.add_argument("--cloud", action="store_true", help="read the lake in real AWS instead of LocalStack")
    parser.add_argument("--rows", type=int, default=5, help="rows to show (default 5)")
    parser.add_argument("--list", action="store_true", help="list every dataset with file and row counts")
    parser.add_argument("--where", action="append", metavar="COLUMN=VALUE",
                        help="only rows where COLUMN equals VALUE (repeatable), e.g. --where employee_id=1332")
    parser.add_argument("--columns", metavar="A,B,...", help="only show these columns")
    args = parser.parse_args()
    if not args.list and not args.dataset:
        parser.error("give a dataset (e.g. silver/employees) or --list")

    s3, fs, bucket, where = connect(args.cloud)
    print(f"[{where}] bucket {bucket}\n")
    try:
        if args.list:
            show_list(s3, fs, bucket)
        else:
            show_dataset(s3, fs, bucket, args.dataset, args.rows, args.where, args.columns)
    except NoCredentialsError:
        sys.exit("No AWS credentials. Run ./bin/setup-participant.sh (cloud) - or drop --cloud for LocalStack.")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("ExpiredToken", "ExpiredTokenException", "InvalidToken", "InvalidAccessKeyId",
                    "SignatureDoesNotMatch"):
            sys.exit(f"AWS rejected your credentials ({code}) - they are temporary and probably expired. "
                     "Run ./bin/setup-participant.sh and try again.")
        if code in ("NoSuchBucket", "404"):
            sys.exit(f"Bucket {bucket} not found in {where}. Has the pipeline run there?")
        raise
    except OSError as exc:
        if "Connection refused" in str(exc) or "Could not connect" in str(exc):
            sys.exit("LocalStack isn't running. Start it (./bin/start-dev.sh), or use --cloud.")
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
