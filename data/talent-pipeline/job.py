"""talent-pipeline: batch ETL for ACME's Performance & Talent data lake.

    sources ──► Bronze (raw, immutable) ──► Silver (validated) ──► Gold (business answers)

Sources:
  * PostgreSQL schema ``hr``  - 7 operational tables (employees, reviews, ...)
  * S3 landing bucket         - ``learning_activity/`` JSONL events, partitioned by day

Deployment constraint: the workshop's Helm chart (infra/helm/templates/python-job.yaml)
ships ONLY this file and requirements.txt into the EKS job pod. Shared helpers therefore
live in the numbered sections below instead of separate modules.

Sections:
  1. Configuration   2. Logging   3. Run context (lineage + metrics)   4. Retry
  5. I/O (PostgreSQL source, S3 data lake)   6. Schema contracts & data quality
  7. Pipeline steps   8. Entry point
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import logging
import os
import platform
import random
import re
import resource
import sys
import time
import uuid
from collections.abc import Callable, Generator, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from typing import Any, TypeVar

import boto3
import numpy as np
import pandas as pd
import psycopg2
import pyarrow as pa
# Public pyarrow API, but only imported when pyarrow is built with S3 support (the PyPI wheels
# are), so static checkers can't see the names; they exist at runtime (covered by integration tests).
from pyarrow.fs import AwsStandardS3RetryStrategy, S3FileSystem  # pyright: ignore[reportPrivateImportUsage]
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from psycopg2 import sql

# =============================================================================
# 1. Configuration
# =============================================================================

# Local defaults mirror what infra/locals.tf injects when IS_LOCAL=true, so the
# job behaves the same whether started by hand or by the workshop tooling.
LOCAL_POSTGRES_DEFAULTS = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_NAME": "postgres",
    "POSTGRES_USER": "postgres",
    "POSTGRES_PASS": "postgres123",
}
LOCAL_AWS_ENDPOINT = "http://localhost.localstack.cloud:4566"
LOCAL_AWS_REGION = "us-east-1"

# Source contract: the 7 tables in schema.sql, in its documented load order.
SOURCE_TABLES = (
    "teams", "competencies", "employees", "projects",
    "performance_reviews", "competency_assessments", "talent_review",
)
# Landing files must follow the partition layout the source system promised.
LANDING_LEARNING_PREFIX = "learning_activity/"
LANDING_LEARNING_KEY = re.compile(r"^learning_activity/year=\d{4}/month=\d{2}/day=\d{2}/[^/]+\.jsonl$")

# Data lake layout (keys inside the lake bucket).
BRONZE_POSTGRES = "bronze/postgres"            # /<table>/ingest_date=YYYY-MM-DD/part-0.parquet
BRONZE_LEARNING = "bronze/learning_activity"   # /year=/month=/day=/events-000.jsonl (raw bytes)
SILVER = "silver"                               # /<entity>/part-0.parquet (events: /event_year=/event_month=/)
QUARANTINE = "quarantine"                       # /<entity>/run_date=YYYY-MM-DD/part-0.parquet
GOLD = "gold"                                   # /<table>/part-0.parquet - one table per business question
RUNS = "_runs"                                  # run manifests (metadata, not data): /run_date=YYYY-MM-DD/<run_id>.json
COPY_WORKERS = 16                               # parallel S3 requests (copies, reads)


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    """All runtime configuration, resolved once from environment variables."""

    is_local: bool
    run_date: date
    postgres_host: str
    postgres_port: int
    postgres_name: str
    postgres_user: str
    postgres_pass: str = field(repr=False)  # never printed in logs or tracebacks
    source_schema: str = "hr"
    landing_bucket: str = ""
    lake_bucket: str = ""
    aws_region: str = LOCAL_AWS_REGION
    aws_endpoint_url: str | None = None
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Build settings from ``env`` (defaults to ``os.environ``).

        Empty strings count as unset: the workshop injects empty POSTGRES_* values
        locally (see backend/README.md), and those must fall back to the defaults.
        """
        env = os.environ if env is None else env
        is_local = env.get("IS_LOCAL", "true").strip().lower() == "true"
        missing: list[str] = []

        def get(name: str, default: str | None = None) -> str:
            value = (env.get(name) or "").strip()
            if value:
                return value
            if default is not None:
                return default
            missing.append(name)
            return ""

        pg_defaults = LOCAL_POSTGRES_DEFAULTS if is_local else {}
        participant = (env.get("PARTICIPANT_ID") or "").strip()

        def bucket_default(kind: str) -> str | None:
            return f"coding-workshop-{kind}-{participant}" if participant else None

        postgres_host = get("POSTGRES_HOST", pg_defaults.get("POSTGRES_HOST"))
        port_raw = get("POSTGRES_PORT", pg_defaults.get("POSTGRES_PORT", "5432"))
        postgres_name = get("POSTGRES_NAME", pg_defaults.get("POSTGRES_NAME"))
        postgres_user = get("POSTGRES_USER", pg_defaults.get("POSTGRES_USER"))
        postgres_pass = get("POSTGRES_PASS", pg_defaults.get("POSTGRES_PASS"))
        landing_bucket = get("LANDING_BUCKET", bucket_default("landing"))
        lake_bucket = get("DATALAKE_BUCKET", bucket_default("datalake"))
        region = get("AWS_REGION", (env.get("AWS_DEFAULT_REGION") or "").strip()
                     or (LOCAL_AWS_REGION if is_local else None))
        if missing:
            raise ConfigError(
                "Missing required environment variables: " + ", ".join(missing)
                + (" (bucket names default from PARTICIPANT_ID)" if not participant else "")
            )
        try:
            postgres_port = int(port_raw)
        except ValueError as exc:
            raise ConfigError(f"POSTGRES_PORT must be an integer, got {port_raw!r}") from exc

        return cls(
            is_local=is_local,
            run_date=_parse_run_date(env.get("RUN_DATE")),
            postgres_host=postgres_host,
            postgres_port=postgres_port,
            postgres_name=postgres_name,
            postgres_user=postgres_user,
            postgres_pass=postgres_pass,
            source_schema=get("SOURCE_SCHEMA", "hr"),
            landing_bucket=landing_bucket,
            lake_bucket=lake_bucket,
            aws_region=region,
            aws_endpoint_url=(env.get("AWS_ENDPOINT_URL") or "").strip() or (LOCAL_AWS_ENDPOINT if is_local else None),
            log_level=get("LOG_LEVEL", "INFO").upper(),
        )


def _parse_run_date(raw: str | None) -> date:
    """RUN_DATE (YYYY-MM-DD) pins the logical date for reruns/backfills; default: today (UTC)."""
    if not raw or not raw.strip():
        return datetime.now(UTC).date()
    try:
        return date.fromisoformat(raw.strip())
    except ValueError as exc:
        raise ConfigError(f"RUN_DATE must be YYYY-MM-DD, got {raw!r}") from exc


# =============================================================================
# 2. Logging - one JSON object per line, so logs are both readable and queryable
#    (e.g. `python job.py | jq 'select(.level=="ERROR")'`, or CloudWatch filters).
# =============================================================================

log = logging.getLogger("talent_pipeline")
_LOG_CONTEXT: dict[str, Any] = {}  # fields stamped on every line (e.g. run_id)


class JsonFormatter(logging.Formatter):
    """Render a log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "msg": record.getMessage(),
            **_LOG_CONTEXT,
            **getattr(record, "fields", {}),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> None:
    """Send JSON logs to stdout (what `kubectl logs` and CloudWatch collect)."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log_event(level: int, msg: str, **fields: Any) -> None:
    """Log ``msg`` with structured ``fields`` (rows, step, duration_s, ...)."""
    log.log(level, msg, extra={"fields": fields})


# =============================================================================
# 3. Run context - identifies a run (lineage) and records per-step metrics
# =============================================================================


def peak_rss_mb() -> float:
    """Peak resident memory of this process so far, in MB (Linux reports ru_maxrss in KB).

    The EKS job pod is killed above 2 GiB (infra/helm/values.yaml), so this is logged per step.
    """
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def new_run_id(now: datetime | None = None) -> str:
    """Time-sortable, unique run id, e.g. ``20261015T091500Z-1a2b3c4d``."""
    now = now or datetime.now(UTC)
    return f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


@dataclass
class StepResult:
    """Outcome of one pipeline step; serialized into the run manifest."""

    name: str
    started_at: str
    status: str = "running"
    finished_at: str | None = None
    duration_s: float | None = None
    cpu_s: float | None = None          # CPU time used by this step (user + system)
    peak_rss_mb: float | None = None    # process peak memory so far, at the end of this step
    metrics: dict[str, int] = field(default_factory=dict)
    error: str | None = None


@dataclass
class RunContext:
    """State shared by all steps of one pipeline run."""

    settings: Settings
    run_id: str = field(default_factory=new_run_id)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    steps: list[StepResult] = field(default_factory=list)

    @contextmanager
    def step(self, name: str) -> Generator[StepResult, None, None]:
        """Time a step, log start/finish, and record success or failure.

        Usage::

            with ctx.step("bronze") as result:
                result.metrics["rows_written"] = n
        """
        result = StepResult(name=name, started_at=datetime.now(UTC).isoformat())
        self.steps.append(result)
        log_event(logging.INFO, "step started", step=name)
        t0, cpu0 = time.perf_counter(), time.process_time()
        try:
            yield result
        except Exception as exc:
            result.status = "failed"
            result.error = f"{type(exc).__name__}: {exc}"
            log.error("step failed", exc_info=True, extra={"fields": {"step": name}})
            raise
        else:
            result.status = "succeeded"
        finally:
            result.duration_s = round(time.perf_counter() - t0, 3)
            result.cpu_s = round(time.process_time() - cpu0, 3)
            result.peak_rss_mb = peak_rss_mb()
            result.finished_at = datetime.now(UTC).isoformat()
            log_event(logging.INFO, "step finished", step=name, status=result.status,
                      duration_s=result.duration_s, cpu_s=result.cpu_s, peak_rss_mb=result.peak_rss_mb,
                      **result.metrics)

    def summary(self) -> dict[str, Any]:
        """Run-level summary (becomes the lineage manifest in step 5)."""
        failed = any(s.status == "failed" for s in self.steps)
        return {
            "run_id": self.run_id,
            "run_date": self.settings.run_date.isoformat(),
            "status": "failed" if failed else "succeeded",
            "started_at": self.started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "steps": [asdict(s) for s in self.steps],
        }


# =============================================================================
# 4. Retry - exponential backoff with full jitter for transient failures
# =============================================================================

T = TypeVar("T")


def with_retry(
    fn: Callable[[], T],
    *,
    what: str,
    retry_on: tuple[type[BaseException], ...],
    attempts: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call ``fn``; on a ``retry_on`` error wait and retry, up to ``attempts`` times.

    Delay before retry n is random in [0, min(max_delay, base_delay * 2**(n-1))].
    The randomness ("jitter") stops many clients from retrying in lockstep.
    Errors not listed in ``retry_on`` (e.g. bad password) fail immediately.
    """
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            if attempt == attempts:
                log_event(logging.ERROR, "giving up after retries", what=what, attempts=attempts, error=str(exc),
                          alert=True)
                raise
            delay = random.uniform(0, min(max_delay, base_delay * 2 ** (attempt - 1)))
            log_event(logging.WARNING, "transient failure, retrying", what=what, attempt=attempt,
                      delay_s=round(delay, 2), error=str(exc).strip())
            sleep(delay)
    raise AssertionError("unreachable")


# =============================================================================
# 5. I/O
# =============================================================================


class SourceDatabase:
    """Read-only access to the PostgreSQL source system."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def connect(self) -> psycopg2.extensions.connection:
        """Open a connection, retrying transient failures (DB starting, network blips)."""
        s = self.settings
        return with_retry(
            lambda: psycopg2.connect(
                host=s.postgres_host, port=s.postgres_port, dbname=s.postgres_name,
                user=s.postgres_user, password=s.postgres_pass,
                # Aurora requires TLS; local PostgreSQL may not have it configured.
                sslmode="prefer" if s.is_local else "require",
                connect_timeout=10, application_name="talent-pipeline",
            ),
            what="postgres connect",
            retry_on=(psycopg2.OperationalError,),
        )

    def ping(self) -> None:
        with closing(self.connect()) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")

    def snapshot(self, tables: Iterable[str]) -> dict[str, pd.DataFrame]:
        """Read full copies of ``tables`` as ONE consistent point-in-time snapshot.

        All reads share a single REPEATABLE READ, read-only transaction, so a
        source write landing mid-ingestion can't produce e.g. a review whose
        employee is missing from the employees snapshot.
        """
        with closing(self.connect()) as conn:
            conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ, readonly=True)
            try:
                with conn.cursor() as cur:
                    return {table: self._read(cur, table) for table in tables}
            finally:
                conn.rollback()  # end the read-only transaction; nothing to commit

    def read_table(self, table: str) -> pd.DataFrame:
        """Return a full snapshot of ``<source_schema>.<table>``."""
        return self.snapshot([table])[table]

    def _read(self, cur: psycopg2.extensions.cursor, table: str) -> pd.DataFrame:
        query = sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(self.settings.source_schema), sql.Identifier(table)
        )
        cur.execute(query)
        if cur.description is None:  # only None for statements that return no result set
            raise RuntimeError(f"query on {table!r} returned no result set")
        columns = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


class Lake:
    """S3 access for the landing bucket and the data lake (LocalStack when local).

    Two clients on purpose:
      * boto3        - object operations (list, copy, small JSON documents)
      * pyarrow S3FS - Parquet datasets; supports replacing partitions on write,
                       which is what makes reruns idempotent
    Both retry throttling/5xx/connection errors via AWS "standard" retry mode.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        # Locally, pin dummy credentials so real AWS keys in the shell are never sent anywhere.
        creds = {"aws_access_key_id": "test", "aws_secret_access_key": "test"} if settings.is_local else {}
        self.s3 = boto3.client(
            "s3",
            region_name=settings.aws_region,
            endpoint_url=settings.aws_endpoint_url,
            # total_max_attempts counts the first try (botocore's "max_attempts" counts only retries),
            # matching the 5 total attempts used for Postgres and pyarrow.
            # Pool sized to the copy threads; the default (10) would discard connections.
            config=BotoConfig(retries={"total_max_attempts": 5, "mode": "standard"}, connect_timeout=10,
                              read_timeout=60, max_pool_connections=COPY_WORKERS),
            **creds,
        )
        fs_kwargs: dict[str, Any] = {
            "region": settings.aws_region,
            "retry_strategy": AwsStandardS3RetryStrategy(max_attempts=5),
        }
        if settings.aws_endpoint_url:
            scheme, _, host = settings.aws_endpoint_url.partition("://")
            fs_kwargs.update(endpoint_override=host, scheme=scheme)
        if settings.is_local:
            fs_kwargs.update(access_key="test", secret_key="test")
        self.fs = S3FileSystem(**fs_kwargs)

    def ensure_bucket(self, bucket: str, *, create: bool) -> None:
        """Fail fast if ``bucket`` is missing; optionally create it (idempotent)."""
        try:
            self.s3.head_bucket(Bucket=bucket)
            return
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("403", "AccessDenied"):
                raise PermissionError(
                    f"access denied to s3://{bucket} - verify the IAM role this job runs as (EKS node/pod role) "
                    f"allows s3:* on it; workshop policy only grants buckets named coding-workshop*<app_id>*"
                ) from exc
            if code not in ("404", "NoSuchBucket") or not create:
                raise
        params: dict[str, Any] = {"Bucket": bucket}
        if self.settings.aws_region != "us-east-1":  # us-east-1 rejects an explicit LocationConstraint
            params["CreateBucketConfiguration"] = {"LocationConstraint": self.settings.aws_region}
        self.s3.create_bucket(**params)
        log_event(logging.INFO, "created bucket", bucket=bucket)

    def list_objects(self, bucket: str, prefix: str) -> list[dict[str, Any]]:
        """All objects under ``prefix`` (paginated), excluding folder-marker keys."""
        objects: list[dict[str, Any]] = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            objects.extend(o for o in page.get("Contents", []) if not o["Key"].endswith("/"))
        return objects

    def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str,
                    metadata: Mapping[str, str]) -> None:
        """Server-side copy (bytes never pass through this job) with lineage metadata.

        Content is copied unchanged, so the ETag (MD5 for single-part objects)
        stays equal to the source's - which is how Bronze is verified.
        """
        self.s3.copy_object(
            CopySource={"Bucket": src_bucket, "Key": src_key}, Bucket=dst_bucket, Key=dst_key,
            MetadataDirective="REPLACE", Metadata=dict(metadata), ContentType="application/x-ndjson",
        )

    def head_metadata(self, bucket: str, key: str) -> dict[str, str]:
        return self.s3.head_object(Bucket=bucket, Key=key).get("Metadata", {})

    def get_bytes(self, bucket: str, key: str) -> bytes:
        return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        """Delete every object under ``prefix`` (folder markers included); return count.

        ``prefix`` must end with "/" so ``silver/teams/`` can't also match ``silver/teams_v2/``.
        """
        if not prefix.endswith("/"):
            raise ValueError(f"prefix must end with '/': {prefix!r}")
        keys = [o["Key"] for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                for o in page.get("Contents", [])]
        for i in range(0, len(keys), 1000):  # DeleteObjects accepts at most 1000 keys per call
            self.s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]],
                                                          "Quiet": True})
        return len(keys)

    def write_parquet(self, df: pd.DataFrame, bucket: str, prefix: str,
                      partition_cols: list[str] | None = None) -> int:
        """Write ``df`` as Parquet under ``s3://bucket/prefix``; return rows written.

        Idempotency: ``delete_matching`` first clears every partition present in
        ``df`` (or the whole prefix when unpartitioned), so a rerun replaces data
        instead of appending a second copy.
        """
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=f"{bucket}/{prefix.strip('/')}",
            partition_cols=partition_cols or None,
            filesystem=self.fs,
            existing_data_behavior="delete_matching",
            basename_template="part-{i}.parquet",
        )
        return table.num_rows

    def read_parquet(self, bucket: str, prefix: str) -> pd.DataFrame:
        """Read a Parquet dataset; hive partition folders (key=value) become columns."""
        return pq.read_table(f"{bucket}/{prefix.strip('/')}", filesystem=self.fs).to_pandas()

    def put_json(self, bucket: str, key: str, payload: Mapping[str, Any]) -> None:
        self.s3.put_object(Bucket=bucket, Key=key, ContentType="application/json",
                           Body=json.dumps(payload, indent=2, default=str).encode())


# =============================================================================
# 6. Schema contracts & data quality
#
#    Two kinds of problem, handled differently:
#      * structural - a required column is gone -> SchemaError, stop the step.
#        Every row would be affected; quarantining them all would hide an outage.
#      * row-level - bad value, missing field, duplicate -> quarantine that row
#        with a reason and keep going (one bad row never sinks the pipeline).
# =============================================================================

DATE = pd.ArrowDtype(pa.date32())  # a real calendar date (Parquet DATE), not a midnight timestamp

JOB_LEVELS = ("IC1", "IC2", "IC3", "Senior IC", "Manager", "Senior Manager", "Director")
COMPETENCY_CATEGORIES = ("Technical", "Leadership", "Domain")
PROJECT_STATUSES = ("on-time", "delayed", "at-risk", "cancelled")
LOW_MEDIUM_HIGH = ("Low", "Medium", "High")
PROMOTION_READINESS = ("Ready Now", "1-2 Years", "Not Ready")
HALF_YEAR_PERIOD = r"\d{4}-H[12]"  # e.g. 2025-H1
EMAIL = r"[^@\s]+@[^@\s]+\.[^@\s]+"

LEARNING_EVENT_TYPES = ("enrollment", "module_started", "module_completed", "quiz_attempted",
                        "quiz_passed", "course_completed", "certification_earned")
SCORED_EVENT_TYPES = ("quiz_attempted", "quiz_passed")
UNSCORED_EVENT_TYPES = tuple(t for t in LEARNING_EVENT_TYPES if t not in SCORED_EVENT_TYPES)
# Known schema drift: the 2024-09-03 landing file calls the `text` field `notes`.
LEARNING_FIELD_ALIASES = {"notes": "text"}
# Source field names (before renames) that every landing file must carry.
LEARNING_REQUIRED_SOURCE_FIELDS = ("event_id", "employee_id", "event_date", "event_type", "course_name")
LANDING_PARTITION_DATE = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/")

# child table -> ((column, parent table), ...). Parents are validated first (SOURCE_TABLES order).
FOREIGN_KEYS: dict[str, tuple[tuple[str, str], ...]] = {
    "employees": (("team_id", "teams"),),
    "projects": (("team_id", "teams"),),
    "performance_reviews": (("employee_id", "employees"),),
    "competency_assessments": (("employee_id", "employees"), ("competency_id", "competencies")),
    "talent_review": (("employee_id", "employees"),),
}


class SchemaError(Exception):
    """A source broke its structural contract (e.g. a required column disappeared)."""


@dataclass(frozen=True)
class Rule:
    """Row-level check. ``passes(df)`` returns a boolean Series: True = row is fine.

    Rules see typed columns. Nulls should pass - missing values have their own check.
    """

    name: str
    passes: Callable[[pd.DataFrame], pd.Series]


def one_of(col: str, allowed: Iterable[str]) -> Rule:
    values = list(allowed)
    return Rule(f"not_allowed_{col}", lambda df: df[col].isna() | df[col].isin(values))


def between(col: str, low: float, high: float) -> Rule:
    return Rule(f"out_of_range_{col}", lambda df: df[col].isna() | df[col].between(low, high))


def matches(col: str, pattern: str) -> Rule:
    return Rule(f"bad_format_{col}", lambda df: df[col].isna() | df[col].str.fullmatch(pattern))


def not_after(col: str, limit: date) -> Rule:
    return Rule(f"future_{col}", lambda df: df[col].isna() | (df[col] <= limit))


def unique_together(*cols: str) -> Rule:
    """Natural-key uniqueness beyond the primary key; later duplicates fail."""
    return Rule(f"duplicate_{'_'.join(cols)}", lambda df: ~df.duplicated(list(cols), keep="first"))


def references(col: str, parent: str, parent_keys: pd.Series) -> Rule:
    """Foreign key: ``col`` must exist among the parent's *valid* (Silver) keys."""
    return Rule(f"unknown_{parent}", lambda df: df[col].isna() | df[col].isin(parent_keys))


@dataclass(frozen=True)
class Contract:
    """Schema contract for one entity: what Silver promises its consumers."""

    entity: str
    columns: Mapping[str, str]   # name -> type ("int", "float", "str", "date", "timestamp"); also column order
    required: tuple[str, ...]    # must be non-null
    key: tuple[str, ...]         # unique; later duplicates are quarantined
    rules: tuple[Rule, ...] = ()


def source_contracts(run_date: date) -> dict[str, Contract]:
    """Contracts for the PostgreSQL tables (schema.sql + observed domain values)."""
    return {
        "teams": Contract(
            "teams", {"team_id": "int", "name": "str", "department": "str"},
            required=("team_id", "name", "department"), key=("team_id",)),
        "competencies": Contract(
            "competencies", {"competency_id": "int", "name": "str", "category": "str"},
            required=("competency_id", "name", "category"), key=("competency_id",),
            rules=(one_of("category", COMPETENCY_CATEGORIES),)),
        "employees": Contract(
            "employees", {"employee_id": "int", "first_name": "str", "last_name": "str", "email": "str",
                          "team_id": "int", "job_level": "str", "hire_date": "date"},
            required=("employee_id", "first_name", "last_name", "email", "team_id", "job_level", "hire_date"),
            key=("employee_id",),
            rules=(matches("email", EMAIL), one_of("job_level", JOB_LEVELS), not_after("hire_date", run_date))),
        "projects": Contract(
            "projects", {"project_id": "int", "name": "str", "team_id": "int",
                         "outcome_status": "str", "outcome_score": "float"},
            required=("project_id", "name", "team_id", "outcome_status", "outcome_score"), key=("project_id",),
            rules=(one_of("outcome_status", PROJECT_STATUSES), between("outcome_score", 0, 100))),
        "performance_reviews": Contract(
            "performance_reviews", {"review_id": "int", "employee_id": "int", "review_period": "str",
                                    "rating": "int", "manager_comment": "str", "submitted_date": "date"},
            required=("review_id", "employee_id", "review_period", "rating", "submitted_date"), key=("review_id",),
            rules=(matches("review_period", HALF_YEAR_PERIOD), between("rating", 1, 5),
                   not_after("submitted_date", run_date), unique_together("employee_id", "review_period"))),
        "competency_assessments": Contract(
            "competency_assessments", {"assessment_id": "int", "employee_id": "int", "competency_id": "int",
                                       "proficiency_level": "int", "required_level": "int", "assessed_date": "date"},
            required=("assessment_id", "employee_id", "competency_id", "proficiency_level", "required_level",
                      "assessed_date"),
            key=("assessment_id",),
            rules=(between("proficiency_level", 1, 5), between("required_level", 1, 5),
                   not_after("assessed_date", run_date))),
        "talent_review": Contract(
            "talent_review", {"employee_id": "int", "review_cycle": "str", "potential_rating": "str",
                              "promotion_readiness": "str", "attrition_risk": "str"},
            required=("employee_id", "review_cycle", "potential_rating", "promotion_readiness", "attrition_risk"),
            key=("employee_id",),
            rules=(matches("review_cycle", HALF_YEAR_PERIOD), one_of("potential_rating", LOW_MEDIUM_HIGH),
                   one_of("promotion_readiness", PROMOTION_READINESS), one_of("attrition_risk", LOW_MEDIUM_HIGH))),
    }


LEARNING_EVENTS = Contract(
    "learning_events",
    {"event_id": "str", "employee_id": "int", "event_ts": "timestamp", "event_type": "str",
     "course_name": "str", "text": "str", "score": "int"},
    required=("event_id", "employee_id", "event_ts", "event_type", "course_name"),
    key=("event_id",),
    rules=(
        matches("event_id", r"EV\d{10}"),
        one_of("event_type", LEARNING_EVENT_TYPES),
        between("score", 0, 100),
        Rule("missing_score", lambda df: ~df["event_type"].isin(SCORED_EVENT_TYPES) | df["score"].notna()),
        # Only judged for *known* unscored types; an unknown type is already rejected on its own.
        Rule("unexpected_score", lambda df: ~df["event_type"].isin(UNSCORED_EVENT_TYPES) | df["score"].isna()),
        # The landing partition (year=/month=/day=) must agree with the event's own timestamp.
        Rule("event_outside_partition",
             lambda df: df["event_ts"].isna() | (df["event_ts"].dt.normalize() == df["_partition_date"])),
    ),
)


def coerce(series: pd.Series, kind: str) -> pd.Series:
    """Convert ``series`` to a contract type. Values that can't convert become NA.

    Everything goes through trimmed text first, so the same code handles values that
    arrive typed (from Parquet) or as strings (from JSON). Formats are strict on
    purpose: "yesterday" or "2026-13-45" must fail, not be guessed at.
    """
    text = series.astype("string").str.strip()
    text = text.mask(text.eq(""))  # blank -> NA
    if kind == "str":
        return text
    if kind == "int":
        number = pd.to_numeric(text, errors="coerce")
        return number.where(number.isna() | (number % 1 == 0)).astype("Int64")  # "3.5" is not an int
    if kind == "float":
        return pd.to_numeric(text, errors="coerce").astype("Float64")
    if kind == "date":
        return pd.to_datetime(text, format="%Y-%m-%d", errors="coerce").astype(DATE)
    if kind == "timestamp":
        return pd.to_datetime(text, format="%Y-%m-%dT%H:%M:%S", errors="coerce")
    raise ValueError(f"unknown column type {kind!r}")


@dataclass
class Validated:
    valid: pd.DataFrame     # typed contract columns + lineage ("_"-prefixed) columns
    rejected: pd.DataFrame  # original values + reject_reason (every failed check, ";"-joined)


def validate(df: pd.DataFrame, contract: Contract, extra_rules: Iterable[Rule] = ()) -> Validated:
    """Enforce ``contract`` on ``df``: types, required fields, domain rules, uniqueness.

    A row is rejected with ALL the reasons it fails, not just the first, so one look
    at the quarantine tells you everything wrong with a record.
    """
    missing_cols = [c for c in contract.columns if c not in df.columns]
    if missing_cols:
        raise SchemaError(f"{contract.entity}: missing required column(s) {missing_cols}")
    unexpected = sorted(c for c in df.columns if c not in contract.columns and not c.startswith("_"))
    if unexpected:
        log_event(logging.WARNING, "unexpected columns dropped (additive schema drift)",
                  entity=contract.entity, columns=unexpected)

    lineage = [c for c in df.columns if c.startswith("_")]
    typed = pd.DataFrame({col: coerce(df[col], kind) for col, kind in contract.columns.items()}, index=df.index)
    checks: dict[str, pd.Series] = {}
    for col in contract.columns:
        present = (df[col].notna() & (df[col].astype("string").str.strip() != "")).fillna(False).astype(bool)
        checks[f"bad_type_{col}"] = present & typed[col].isna()   # had a value, but it didn't convert
        if col in contract.required:
            checks[f"missing_{col}"] = ~present
    for col in lineage:  # attach (not join): pandas copy-on-write shares the data, no full-frame copy
        typed[col] = df[col]
    for rule in (*contract.rules, *extra_rules):
        checks[rule.name] = ~rule.passes(typed).fillna(True).astype(bool)

    failures = pd.DataFrame(checks, index=df.index)
    failed = failures.any(axis=1)
    # Primary-key duplicates among otherwise-valid rows: the first occurrence wins.
    duplicate = pd.Series(False, index=df.index)
    duplicate[~failed] = typed.loc[~failed, list(contract.key)].duplicated(keep="first")
    failures[f"duplicate_{'_'.join(contract.key)}"] = duplicate
    failed |= duplicate

    reasons = pd.Series("", index=df.index, dtype="string")
    for name in failures.columns:
        reasons = reasons.mask(failures[name], reasons + name + ";")
    return Validated(
        valid=typed.loc[~failed],
        rejected=df.loc[failed].assign(reject_reason=reasons[failed].str.rstrip(";")),
    )


def parse_jsonl(files: Iterable[tuple[str, bytes]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse raw JSONL files into (records, malformed lines).

    Every line keeps its lineage: source file, line number and the raw text, so any
    quarantined record can be traced to the exact byte range it came from.

    Memory: values are collected column-by-column (one list per field) rather than one
    dict per line - at 600k lines the per-row dicts alone cost ~1 GB (measured, TESTING.md).
    ``files`` may be a lazy iterator; each body is released once parsed.
    """
    columns: dict[str, list[Any]] = {}
    lineage: dict[str, list[Any]] = {"_source_file": [], "_source_line": [], "_raw": [], "_partition_date": []}
    malformed: list[dict[str, Any]] = []
    rows = 0
    for key, body in files:
        m = LANDING_PARTITION_DATE.search(key)
        partition_date = pd.Timestamp(f"{m[1]}-{m[2]}-{m[3]}") if m else pd.NaT
        for line_no, line in enumerate(body.decode("utf-8", errors="replace").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                record = None
                reason = "malformed_json"
            else:
                reason = None if isinstance(record, dict) else "not_a_json_object"
            if reason or not isinstance(record, dict):
                malformed.append({"_source_file": key, "_source_line": line_no, "_raw": line,
                                  "reject_reason": reason})
                continue
            for field_name in record.keys() - columns.keys():  # first time we see this field
                columns[field_name] = [None] * rows
            for field_name, values in columns.items():
                values.append(record.get(field_name))
            for name, value in (("_source_file", key), ("_source_line", line_no), ("_raw", line),
                                ("_partition_date", partition_date)):
                lineage[name].append(value)
            rows += 1
        del body
    records = pd.DataFrame({**columns, **lineage}) if rows else pd.DataFrame()
    return (records,
            pd.DataFrame(malformed, columns=["_source_file", "_source_line", "_raw", "reject_reason"]))


def apply_field_aliases(df: pd.DataFrame, aliases: Mapping[str, str]) -> tuple[pd.DataFrame, int]:
    """Map known renamed fields onto their canonical name; return (df, rows affected).

    If a row somehow has both names, the canonical field wins.
    """
    affected = 0
    for old, new in aliases.items():
        if old not in df.columns:
            continue
        hits = df[old].notna()
        affected += int(hits.sum())
        log_event(logging.WARNING, "schema drift: field renamed by source, mapped to canonical name",
                  field=old, canonical=new, rows=int(hits.sum()),
                  files=sorted(df.loc[hits, "_source_file"].unique().tolist()) if "_source_file" in df else [])
        df = df.assign(**{new: df[new].where(df[new].notna(), df[old]) if new in df.columns else df[old]})
        df = df.drop(columns=[old])
    return df, affected


def period_start(period: pd.Series) -> pd.Series:
    """'2024-H2' -> 2024-07-01. Gives half-year periods a real, sortable date."""
    year = period.str.slice(0, 4).astype("int64")
    half = period.str.slice(-1).astype("int64")
    return pd.to_datetime(pd.DataFrame({"year": year, "month": (half - 1) * 6 + 1, "day": 1})).astype(DATE)


def period_end(period: pd.Series) -> pd.Series:
    """'2024-H2' -> 2024-12-31, '2025-H1' -> 2025-06-30 (last day of the half-year)."""
    year = period.str.slice(0, 4).astype("int64")
    half = period.str.slice(-1).astype("int64")
    day = half.map({1: 30, 2: 31})
    return pd.to_datetime(pd.DataFrame({"year": year, "month": half * 6, "day": day})).astype(DATE)


# Cross-table consistency: which date of a record must not precede the employee's hire date.
# Reviews use the END of the period: someone hired mid-period worked part of it (not flagged).
BEFORE_HIRE_DATES: dict[str, Callable[[pd.DataFrame], pd.Series]] = {
    "performance_reviews": lambda df: period_end(df["review_period"]),
    "competency_assessments": lambda df: df["assessed_date"],
}


def flag_before_hire(employee_ids: pd.Series, record_dates: pd.Series, employees: pd.DataFrame) -> pd.Series:
    """True where a record is dated before its employee's hire date.

    Flagged, never dropped: the data can't tell us whether the record or the hire date
    is wrong (hire_date may mean "joined current role"), so we keep the record, mark it,
    and let consumers show answers with and without it. Open question for the HR data owner.
    """
    hire = employee_ids.map(employees.set_index("employee_id")["hire_date"])
    # set_axis (not to_numpy) keeps the DATE type - an empty column would otherwise turn float.
    return flag(record_dates.set_axis(employee_ids.index) < hire)


def insert_before_lineage(df: pd.DataFrame, column: str, values: pd.Series) -> pd.DataFrame:
    """Add a data column just before the `_` lineage columns (keeps schemas readable)."""
    position = next((i for i, c in enumerate(df.columns) if c.startswith("_")), len(df.columns))
    out = df.copy(deep=False)
    out.insert(position, column, values)
    return out


def report_before_hire(metrics: dict[str, int], entity: str, flags: pd.Series) -> None:
    count = int(flags.sum())
    metrics[f"{entity}_before_hire"] = count
    if count:
        log_event(logging.WARNING, "records dated before the employee's hire date (flagged, kept)", entity=entity,
                  rows=count, share=round(count / len(flags), 4))


def build_dim_employee(employees: pd.DataFrame, teams: pd.DataFrame, as_of: date) -> pd.DataFrame:
    """Conformed employee dimension: one row per employee with team and department attached.

    Built once in Silver so every Gold table joins the same way. ``tenure_years`` is
    measured at ``as_of`` (the run date), keeping reruns of a given day reproducible.
    """
    dim = employees.merge(
        teams[["team_id", "name", "department"]].rename(columns={"name": "team_name"}),
        on="team_id", how="left", validate="many_to_one",
    )
    hired = pd.to_datetime(dim["hire_date"].astype("string"))
    return pd.DataFrame({
        "employee_id": dim["employee_id"],
        "full_name": dim["first_name"] + " " + dim["last_name"],
        "first_name": dim["first_name"],
        "last_name": dim["last_name"],
        "email": dim["email"],
        "job_level": dim["job_level"],
        "hire_date": dim["hire_date"],
        "tenure_years": ((pd.Timestamp(as_of) - hired).dt.days / 365.25).round(1),
        "team_id": dim["team_id"],
        "team_name": dim["team_name"],
        "department": dim["department"],
    })


# =============================================================================
# 7. Pipeline steps
# =============================================================================


def preflight(ctx: RunContext, db: SourceDatabase, lake: Lake) -> None:
    """Verify every dependency before touching data, so failures are early and clear."""
    s = ctx.settings
    with ctx.step("preflight"):
        db.ping()
        lake.ensure_bucket(s.landing_bucket, create=False)  # owned by the source system
        lake.ensure_bucket(s.lake_bucket, create=True)      # owned by this pipeline


def ingest_bronze(ctx: RunContext, db: SourceDatabase, lake: Lake) -> None:
    """Land raw source data in Bronze: unmodified, immutable, with lineage attached."""
    with ctx.step("bronze") as result:
        result.metrics.update(_bronze_postgres(ctx, db, lake))
        result.metrics.update(_bronze_learning(ctx, lake))


def _bronze_postgres(ctx: RunContext, db: SourceDatabase, lake: Lake) -> dict[str, int]:
    """Snapshot every source table to ``bronze/postgres/<table>/ingest_date=<run_date>/``.

    * Values and types are kept exactly as the source returns them (no cleaning).
    * Lineage columns (``_`` prefix = added by the pipeline, not the source):
      ``_run_id``, ``_ingested_at``, ``_source``.
    * Idempotent: a rerun for the same RUN_DATE replaces that day's partition;
      earlier days' snapshots are never touched (history stays immutable).
    """
    s = ctx.settings
    ingested_at = pd.Timestamp.now(tz="UTC")
    metrics: dict[str, int] = {}
    for table, df in db.snapshot(SOURCE_TABLES).items():
        if df.empty:
            log_event(logging.WARNING, "source table is empty", table=table)
        df = df.assign(
            _run_id=ctx.run_id,
            _ingested_at=ingested_at,
            _source=f"postgres://{s.postgres_host}/{s.postgres_name}/{s.source_schema}.{table}",
            ingest_date=s.run_date.isoformat(),
        )
        rows = lake.write_parquet(df, s.lake_bucket, f"{BRONZE_POSTGRES}/{table}", partition_cols=["ingest_date"])
        log_event(logging.INFO, "bronze table written", table=table, rows=rows,
                  path=f"s3://{s.lake_bucket}/{BRONZE_POSTGRES}/{table}/ingest_date={s.run_date.isoformat()}/")
        metrics[f"{table}_rows"] = rows
    return metrics


@dataclass
class CopyPlan:
    """What Bronze must do for each landing file (decided before any copying)."""

    to_copy: list[tuple[str, str]] = field(default_factory=list)  # (landing key, bronze key)
    unchanged: int = 0                                            # already in Bronze, identical
    conflicts: list[str] = field(default_factory=list)            # in Bronze, but source bytes changed
    bad_keys: list[str] = field(default_factory=list)             # violates the partition layout


def plan_learning_copy(landing: Iterable[Mapping[str, Any]], bronze: Iterable[Mapping[str, Any]]) -> CopyPlan:
    """Incremental-load decision for landing files (pure function - no I/O).

    New file -> copy. Same size + ETag already in Bronze -> skip (that's what makes
    reruns cheap and duplicate-free). Different bytes under the same key -> conflict:
    Bronze is immutable, so we keep the original and raise a warning instead of
    silently overwriting history.
    """
    existing = {o["Key"]: o for o in bronze}
    plan = CopyPlan()
    for obj in landing:
        key = obj["Key"]
        if not LANDING_LEARNING_KEY.match(key):
            plan.bad_keys.append(key)
            continue
        dst = f"{BRONZE_LEARNING}/{key.removeprefix(LANDING_LEARNING_PREFIX)}"
        have = existing.get(dst)
        if have is None:
            plan.to_copy.append((key, dst))
        elif have["ETag"] == obj["ETag"] and have["Size"] == obj["Size"]:
            plan.unchanged += 1
        else:
            plan.conflicts.append(key)
    return plan


def _bronze_learning(ctx: RunContext, lake: Lake) -> dict[str, int]:
    """Copy landing JSONL files byte-for-byte into Bronze, then verify every file."""
    s = ctx.settings
    landing = lake.list_objects(s.landing_bucket, LANDING_LEARNING_PREFIX)
    if not landing:
        log_event(logging.WARNING, "no landing files found", bucket=s.landing_bucket, prefix=LANDING_LEARNING_PREFIX)
    plan = plan_learning_copy(landing, lake.list_objects(s.lake_bucket, f"{BRONZE_LEARNING}/"))
    for key in plan.bad_keys:
        log_event(logging.WARNING, "landing key violates partition layout; skipped", key=key)
    for key in plan.conflicts:
        log_event(logging.WARNING, "landing file changed after ingestion; Bronze copy kept (immutable)", key=key)
    # Guide §7 "Zero Rows Written": an empty daily file usually means a failed or partial delivery.
    # It is still ingested (Bronze stays a faithful copy of what arrived) - this makes it visible.
    bad_keys = set(plan.bad_keys)
    empty = sorted(o["Key"] for o in landing if o["Size"] == 0 and o["Key"] not in bad_keys)
    for key in empty:
        log_event(logging.WARNING, "landing file is empty (0 bytes): possible failed or partial delivery", key=key)

    ingested_at = datetime.now(UTC).isoformat()

    def copy(pair: tuple[str, str]) -> None:
        src, dst = pair
        lake.copy_object(s.landing_bucket, src, s.lake_bucket, dst, metadata={
            "run-id": ctx.run_id, "ingested-at": ingested_at, "source": f"s3://{s.landing_bucket}/{src}",
        })

    # boto3 clients are thread-safe; copies are network-bound, so threads parallelize well.
    with ThreadPoolExecutor(max_workers=COPY_WORKERS) as pool:
        list(pool.map(copy, plan.to_copy))

    # Verification: every valid landing file must now exist in Bronze with identical size + ETag.
    bronze_now = {o["Key"]: o for o in lake.list_objects(s.lake_bucket, f"{BRONZE_LEARNING}/")}
    not_expected = set(plan.bad_keys) | set(plan.conflicts)
    mismatched = []
    for obj in landing:
        if obj["Key"] in not_expected:
            continue
        got = bronze_now.get(f"{BRONZE_LEARNING}/{obj['Key'].removeprefix(LANDING_LEARNING_PREFIX)}")
        if got is None or got["Size"] != obj["Size"] or got["ETag"] != obj["ETag"]:
            mismatched.append(obj["Key"])
    if mismatched:
        raise RuntimeError(f"{len(mismatched)} Bronze file(s) differ from landing, e.g. {mismatched[:3]}")

    copied_keys = {src for src, _ in plan.to_copy}
    copied_bytes = sum(o["Size"] for o in landing if o["Key"] in copied_keys)
    log_event(logging.INFO, "bronze learning files ingested", listed=len(landing), copied=len(plan.to_copy),
              unchanged=plan.unchanged, conflicts=len(plan.conflicts), bad_keys=len(plan.bad_keys),
              bytes_copied=copied_bytes)
    return {
        "learning_files_listed": len(landing),
        "learning_files_copied": len(plan.to_copy),
        "learning_files_unchanged": plan.unchanged,
        "learning_files_conflicting": len(plan.conflicts),
        "learning_files_bad_key": len(plan.bad_keys),
        "learning_files_verified": len(landing) - len(plan.conflicts) - len(plan.bad_keys),
        "learning_files_empty": len(empty),
        "learning_bytes_copied": copied_bytes,
    }


def build_silver(ctx: RunContext, lake: Lake) -> None:
    """Validate, clean and conform Bronze into Silver; route bad rows to quarantine.

    Silver is a full rebuild from Bronze on every run (small data, simplest way to be
    correct and idempotent). Invariant checked for every entity:
        rows read from Bronze == rows written to Silver + rows quarantined
    """
    with ctx.step("silver") as result:
        tables = _silver_tables(ctx, lake, result.metrics)
        _silver_learning_events(ctx, lake, tables["employees"], result.metrics)
        dim = build_dim_employee(tables["employees"], tables["teams"], ctx.settings.run_date)
        result.metrics["dim_employee_rows"] = write_silver(lake, ctx, "dim_employee", dim.assign(_run_id=ctx.run_id))


def read_bronze_table(lake: Lake, settings: Settings, table: str) -> pd.DataFrame:
    """Read this run date's Bronze snapshot of ``table``; fail clearly if it doesn't exist."""
    prefix = f"{BRONZE_POSTGRES}/{table}/ingest_date={settings.run_date.isoformat()}"
    if not lake.list_objects(settings.lake_bucket, f"{prefix}/"):
        raise RuntimeError(f"no Bronze snapshot at s3://{settings.lake_bucket}/{prefix}/ - run the bronze step "
                           f"for RUN_DATE={settings.run_date.isoformat()} first")
    return lake.read_parquet(settings.lake_bucket, prefix)


def write_silver(lake: Lake, ctx: RunContext, entity: str, df: pd.DataFrame,
                 partition_cols: list[str] | None = None) -> int:
    """Replace ``silver/<entity>/`` entirely with ``df``."""
    prefix = f"{SILVER}/{entity}/"
    if partition_cols:
        # delete_matching only clears partitions present in df; a full rebuild must
        # also drop partitions that no longer have data, so clear the prefix first.
        lake.delete_prefix(ctx.settings.lake_bucket, prefix)
    if df.empty:
        log_event(logging.WARNING, "zero rows written to Silver", entity=entity)
    return lake.write_parquet(df, ctx.settings.lake_bucket, prefix, partition_cols)


def write_quarantine(lake: Lake, ctx: RunContext, entity: str, df: pd.DataFrame) -> int:
    """Replace ``quarantine/<entity>/run_date=<run_date>/`` with this run's rejects."""
    prefix = f"{QUARANTINE}/{entity}/run_date={ctx.settings.run_date.isoformat()}/"
    lake.delete_prefix(ctx.settings.lake_bucket, prefix)  # a cleaner rerun must not leave stale rejects behind
    if df.empty:
        return 0
    return lake.write_parquet(df.assign(entity=entity, _run_id=ctx.run_id), ctx.settings.lake_bucket, prefix)


def _publish(ctx: RunContext, lake: Lake, metrics: dict[str, int], entity: str, *, read: int,
             silver: pd.DataFrame, quarantine: pd.DataFrame, partition_cols: list[str] | None = None) -> None:
    """Write Silver + quarantine for one entity, reconcile counts, log the outcome."""
    written = write_silver(lake, ctx, entity, silver, partition_cols)
    quarantined = write_quarantine(lake, ctx, entity, quarantine)
    if written + quarantined != read:
        raise RuntimeError(f"reconciliation failed for {entity}: read {read} != "
                           f"silver {written} + quarantine {quarantined}")
    reasons = quarantine["reject_reason"].str.split(";").explode().value_counts().to_dict() if quarantined else {}
    log_event(logging.WARNING if quarantined else logging.INFO, "silver entity written", entity=entity,
              read=read, valid=written, quarantined=quarantined, reasons=reasons,
              examples=_quarantine_examples(quarantine))
    metrics.update({f"{entity}_read": read, f"{entity}_valid": written, f"{entity}_quarantined": quarantined})


def _quarantine_examples(quarantine: pd.DataFrame, limit: int = 3) -> list[dict[str, Any]]:
    """A few rejected rows with location + reason, so the log alone is enough to start debugging."""
    examples = []
    for _, row in quarantine.head(limit).iterrows():
        where = (f"{row['_source_file']}:{row['_source_line']}" if "_source_file" in row
                 else row.get("_source", ""))
        examples.append({"reason": row["reject_reason"], "where": where, "record": str(row["raw_record"])[:160]})
    return examples


def _table_quarantine(rejected: pd.DataFrame) -> pd.DataFrame:
    """Quarantine rows for a source table: reason + the original record as JSON + lineage."""
    payload = [c for c in rejected.columns if not c.startswith("_") and c != "reject_reason"]
    raw = rejected[payload].to_json(orient="records", lines=True, date_format="iso",
                                    default_handler=str).splitlines() if len(rejected) else []
    out = rejected[[c for c in ("_source", "_bronze_run_id") if c in rejected.columns]].copy()
    out.insert(0, "raw_record", raw)
    out.insert(0, "reject_reason", rejected["reject_reason"])
    return out


def _silver_tables(ctx: RunContext, lake: Lake, metrics: dict[str, int]) -> dict[str, pd.DataFrame]:
    """Validate each source table; parents first so foreign keys check against *valid* parents."""
    s = ctx.settings
    contracts = source_contracts(s.run_date)
    snapshots = {table: (read_bronze_table(lake, s, table)
                         .drop(columns=["ingest_date"], errors="ignore")
                         .rename(columns={"_run_id": "_bronze_run_id"}))
                 for table in SOURCE_TABLES}

    # Structural check for ALL tables before writing ANY: a broken source must not leave
    # Silver half-updated. Broken snapshots are quarantined whole, support is alerted, and
    # the run stops - Silver/Gold keep serving the last good version.
    broken = {table: missing for table, df in snapshots.items()
              if (missing := [c for c in contracts[table].columns if c not in df.columns])}
    for table, missing in broken.items():
        reason = f"schema_mismatch: missing column(s) {','.join(missing)}"
        write_quarantine(lake, ctx, table, _table_quarantine(snapshots[table].assign(reject_reason=reason)))
        log_event(logging.ERROR, "schema mismatch: source snapshot quarantined; Silver and Gold left at last "
                  "good version", alert=True, entity=table, missing_columns=missing, rows=len(snapshots[table]))
    if broken:
        raise SchemaError(f"source schema changed for {sorted(broken)}; see quarantine/ and alerts")

    valid: dict[str, pd.DataFrame] = {}
    for table in SOURCE_TABLES:
        bronze = snapshots[table]
        foreign_keys = [references(col, parent, valid[parent][contracts[parent].key[0]])
                        for col, parent in FOREIGN_KEYS.get(table, ())]
        result = validate(bronze, contracts[table], foreign_keys)
        silver = result.valid.assign(_run_id=ctx.run_id)
        if table in BEFORE_HIRE_DATES:  # employees is validated earlier in SOURCE_TABLES order
            flags = flag_before_hire(silver["employee_id"], BEFORE_HIRE_DATES[table](silver), valid["employees"])
            silver = insert_before_lineage(silver, "recorded_before_hire", flags)
            report_before_hire(metrics, table, flags)
        _publish(ctx, lake, metrics, table, read=len(bronze), silver=silver,
                 quarantine=_table_quarantine(result.rejected))
        valid[table] = silver
    return valid


def _silver_learning_events(ctx: RunContext, lake: Lake, employees: pd.DataFrame, metrics: dict[str, int]) -> None:
    """Parse every Bronze JSONL file, fix known drift, validate, dedupe, partition by month."""
    s = ctx.settings
    keys = sorted(o["Key"] for o in lake.list_objects(s.lake_bucket, f"{BRONZE_LEARNING}/"))
    if not keys:
        log_event(logging.WARNING, "no Bronze learning files found", prefix=BRONZE_LEARNING)
    # Sorted keys + line order make "first occurrence wins" deterministic across reruns.
    # pool.map yields bodies in key order as they arrive; each is parsed then released,
    # so the raw bytes of all files are never held at once.
    with ThreadPoolExecutor(max_workers=COPY_WORKERS) as pool:
        records, malformed = parse_jsonl(zip(keys, pool.map(lambda k: lake.get_bytes(s.lake_bucket, k), keys)))
    if records.empty:
        records = pd.DataFrame(columns=["event_id", "employee_id", "event_date", "event_type", "course_name",
                                        "text", "score", "_source_file", "_source_line", "_raw", "_partition_date"])
    records, drift_rows = apply_field_aliases(records, LEARNING_FIELD_ALIASES)
    records, drifted_files = quarantine_drifted_files(records, LEARNING_REQUIRED_SOURCE_FIELDS)
    records = records.rename(columns={"event_date": "event_ts"})  # it's a timestamp; event_date is derived below

    read = len(records) + len(malformed) + len(drifted_files)
    result = validate(records, LEARNING_EVENTS, [references("employee_id", "employees", employees["employee_id"])])
    del records  # memory: at 20x volume each live copy of the events is ~300+ MB (TESTING.md)
    v = result.valid.drop(columns=["_raw"])  # raw text is only needed for rejected rows
    event_date = v["event_ts"].dt.normalize().astype(DATE)
    before_hire = flag_before_hire(v["employee_id"], event_date, employees)
    report_before_hire(metrics, "learning_events", before_hire)
    silver = pd.DataFrame({
        "event_id": v["event_id"],
        "employee_id": v["employee_id"],
        "event_ts": v["event_ts"],
        "event_date": event_date,
        "event_type": v["event_type"],
        "course_name": v["course_name"],
        "text": v["text"],
        "score": v["score"],
        "recorded_before_hire": before_hire,
        "event_year": v["event_ts"].dt.year.astype("int32"),
        "event_month": v["event_ts"].dt.month.astype("int32"),
        "_source_file": v["_source_file"],
        "_source_line": v["_source_line"],
        "_run_id": ctx.run_id,
    })
    quarantine = pd.concat(
        [malformed, drifted_files, result.rejected[["_source_file", "_source_line", "_raw", "reject_reason"]]],
        ignore_index=True,
    ).rename(columns={"_raw": "raw_record"})[["reject_reason", "raw_record", "_source_file", "_source_line"]]
    del result, v
    _publish(ctx, lake, metrics, "learning_events", read=read,
             silver=silver, quarantine=quarantine, partition_cols=["event_year", "event_month"])
    metrics["learning_files_read"] = len(keys)
    metrics["learning_schema_drift_rows"] = drift_rows
    metrics["learning_files_quarantined"] = int(drifted_files["_source_file"].nunique())


def quarantine_drifted_files(records: pd.DataFrame, required: Iterable[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """File-level schema check: a file in which a required field never has a value has
    a different schema, not a few bad rows. Quarantine the whole file and alert.

    Returns (records from healthy files, quarantined rows of drifted files).
    """
    required = list(required)
    columns = ["_source_file", "_source_line", "_raw", "reject_reason"]
    if records.empty:
        return records, pd.DataFrame(columns=columns)
    present = records.reindex(columns=required).notna().groupby(records["_source_file"]).any()
    drifted = {file: [c for c in required if not row[c]] for file, row in present.iterrows() if not row.all()}
    if not drifted:
        return records, pd.DataFrame(columns=columns)
    in_drifted = records["_source_file"].isin(list(drifted))
    rejected = records.loc[in_drifted, ["_source_file", "_source_line", "_raw"]].copy()
    rejected["reject_reason"] = rejected["_source_file"].map(
        lambda f: f"schema_mismatch_file: missing field(s) {','.join(drifted[f])}")
    for file, missing in drifted.items():
        log_event(logging.ERROR, "schema mismatch: file quarantined", alert=True, file=file,
                  missing_fields=missing, rows=int((records["_source_file"] == file).sum()))
    return records.loc[~in_drifted], rejected


# --- Gold: business definitions (agreed 2026-10-15; change here, nowhere else) ---
CRITICAL_GAP_LEVELS = 2       # Q2: proficiency >= 2 levels below required = critical gap
STRONG_RATING = 4             # Q3/Q6: latest rating >= 4 = strong / high performer
LEARNING_RECENT_DAYS = 180    # Q4/Q5: "recent" learning window, counted back from the run date
RISK_LEVELS = ("Low", "Medium", "High")

EMPLOYEE_COLS = ["employee_id", "full_name", "job_level", "hire_date", "tenure_years", "team_id", "team_name",
                 "department"]
SILVER_INPUTS = ("dim_employee", "teams", "competencies", "projects", "performance_reviews",
                 "competency_assessments", "talent_review", "learning_events")
# Gold table -> its grain (the columns that identify exactly one row).
GOLD_GRAIN: dict[str, tuple[str, ...]] = {
    "employee_performance_history": ("employee_id", "review_period"),
    "employee_skill_gaps": ("employee_id", "competency_id"),
    "promotion_readiness": ("employee_id",),
    "employee_learning_summary": ("employee_id",),
    "attrition_risk": ("employee_id",),
    "team_performance_outcomes": ("team_id",),
    "skill_distribution": ("competency_id", "department"),
}


def flag(condition: pd.Series) -> pd.Series:
    """Nullable comparison -> plain bool (NA -> False).

    `x < 0` on a column with NA gives NA ("unknown"), which np.select rejects.
    Callers handle the unknown case explicitly before relying on this.
    """
    return condition.fillna(False).astype(bool)


def gold_performance_history(reviews: pd.DataFrame, dim: pd.DataFrame) -> pd.DataFrame:
    """Q1 - every review per employee, in order, with the change since the previous one."""
    r = reviews.assign(period_start=period_start(reviews["review_period"]))
    r = r.sort_values(["employee_id", "period_start"], kind="stable")
    by_emp = r.groupby("employee_id")
    r["previous_rating"] = by_emp["rating"].shift(1)
    r["rating_change"] = r["rating"] - r["previous_rating"]
    r["review_number"] = by_emp.cumcount() + 1
    r["is_latest_review"] = r["review_number"] == by_emp["review_number"].transform("max")
    r = r.merge(dim[EMPLOYEE_COLS], on="employee_id", how="left", validate="many_to_one")
    return r[[*EMPLOYEE_COLS, "review_period", "period_start", "review_number", "rating", "previous_rating",
              "rating_change", "is_latest_review", "manager_comment", "submitted_date",
              "recorded_before_hire"]].reset_index(drop=True)


def gold_skill_gaps(assessments: pd.DataFrame, competencies: pd.DataFrame, dim: pd.DataFrame) -> pd.DataFrame:
    """Q2 - latest assessment per employee x competency, with gap = required - proficiency.

    gap_levels > 0 means below requirement; >= CRITICAL_GAP_LEVELS is a critical gap.
    """
    latest = (assessments.sort_values(["assessed_date", "assessment_id"], kind="stable")
              .drop_duplicates(["employee_id", "competency_id"], keep="last"))
    gap = (latest["required_level"] - latest["proficiency_level"]).astype("int64")
    out = latest.assign(
        gap_levels=gap,
        gap_status=np.select([gap >= CRITICAL_GAP_LEVELS, gap >= 1, gap == 0], ["critical", "below", "meets"],
                             default="exceeds"),
        is_critical_gap=gap >= CRITICAL_GAP_LEVELS,
    )
    out = (out.merge(competencies[["competency_id", "name", "category"]].rename(columns={"name": "competency_name"}),
                     on="competency_id", how="left", validate="many_to_one")
              .merge(dim[EMPLOYEE_COLS], on="employee_id", how="left", validate="many_to_one"))
    return out[[*EMPLOYEE_COLS, "competency_id", "competency_name", "category", "proficiency_level",
                "required_level", "gap_levels", "gap_status", "is_critical_gap", "assessed_date",
                "recorded_before_hire"]] \
        .sort_values(["employee_id", "competency_id"]).reset_index(drop=True)


def gold_promotion_readiness(dim: pd.DataFrame, talent: pd.DataFrame, history: pd.DataFrame,
                             skill_gaps: pd.DataFrame) -> pd.DataFrame:
    """Q3 - one row per employee with a promotion tier.

    Tiers: ready_strong = High potential + Ready Now + latest rating >= STRONG_RATING
           ready        = High potential + Ready Now
           pipeline     = High potential + 1-2 Years (succession bench)
           not_ready    = everyone else
    """
    latest = history.loc[history["is_latest_review"], ["employee_id", "review_period", "rating", "recorded_before_hire"]] \
        .rename(columns={"review_period": "latest_review_period", "rating": "latest_rating",
                         "recorded_before_hire": "latest_review_before_hire"})
    gaps = skill_gaps.groupby("employee_id", as_index=False).agg(critical_gap_count=("is_critical_gap", "sum"))
    out = (dim[EMPLOYEE_COLS]
           .merge(talent[["employee_id", "review_cycle", "potential_rating", "promotion_readiness", "attrition_risk"]],
                  on="employee_id", how="left", validate="one_to_one")
           .merge(latest, on="employee_id", how="left", validate="one_to_one")
           .merge(gaps, on="employee_id", how="left", validate="one_to_one"))
    out["critical_gap_count"] = out["critical_gap_count"].fillna(0).astype("int64")
    out["latest_review_before_hire"] = flag(out["latest_review_before_hire"])
    high = flag(out["potential_rating"] == "High")
    ready = high & flag(out["promotion_readiness"] == "Ready Now")
    strong = ready & flag(out["latest_rating"] >= STRONG_RATING)
    pipeline = high & flag(out["promotion_readiness"] == "1-2 Years")
    out["is_high_potential"] = high
    out["is_promotion_ready"] = ready
    out["promotion_tier"] = np.select([strong, ready, pipeline], ["ready_strong", "ready", "pipeline"],
                                      default="not_ready")
    return out.sort_values("employee_id").reset_index(drop=True)


def gold_learning_summary(events: pd.DataFrame, dim: pd.DataFrame, as_of: date) -> pd.DataFrame:
    """Q4 - learning activity per employee (all employees, zeros where none).

    Deliberately no "completion rate": in this data most completions have no matching
    enrollment, so a funnel ratio would be misleading (see README, limitations).
    """
    names = {"enrollment": "enrollments", "module_started": "modules_started",
             "module_completed": "modules_completed", "quiz_attempted": "quiz_attempts",
             "quiz_passed": "quizzes_passed", "course_completed": "courses_completed",
             "certification_earned": "certifications_earned"}
    counts = pd.crosstab(events["employee_id"], events["event_type"]) \
        .reindex(columns=list(names), fill_value=0).rename(columns=names)
    completed = events[events["event_type"] == "course_completed"].groupby("employee_id")["course_name"]
    recent_from = pd.Timestamp(as_of) - pd.Timedelta(days=LEARNING_RECENT_DAYS)
    by_emp = events.groupby("employee_id")
    summary = pd.DataFrame({
        "total_events": by_emp.size(),
        "distinct_courses_completed": completed.nunique(),
        "completed_courses": completed.agg(lambda s: "; ".join(sorted(set(s)))),
        "avg_quiz_score": events[events["score"].notna()].groupby("employee_id")["score"].mean().round(1),
        "first_activity_date": by_emp["event_date"].min(),
        "last_activity_date": by_emp["event_date"].max(),
        f"events_last_{LEARNING_RECENT_DAYS}d": events[events["event_ts"] >= recent_from].groupby("employee_id").size(),
        # flag() guarantees a bool mask: an empty untyped column would be read as a column selection.
        "events_before_hire": events[flag(events["recorded_before_hire"])].groupby("employee_id").size(),
    }).join(counts)
    out = dim[EMPLOYEE_COLS].merge(summary, left_on="employee_id", right_index=True, how="left",
                                   validate="one_to_one")
    count_cols = ["total_events", "distinct_courses_completed", f"events_last_{LEARNING_RECENT_DAYS}d",
                  "events_before_hire", *names.values()]
    out[count_cols] = out[count_cols].fillna(0).astype("int64")
    out["completed_courses"] = out["completed_courses"].fillna("")
    return out.sort_values("employee_id").reset_index(drop=True)


def gold_attrition_risk(dim: pd.DataFrame, talent: pd.DataFrame, history: pd.DataFrame,
                        learning: pd.DataFrame) -> pd.DataFrame:
    """Q5 - HR's attrition label next to an objective signal from the rating trend.

    performance_risk: High = rating fell >= 2 levels, Medium = fell 1, Low = flat/up,
    Unknown = fewer than two reviews. combined_risk takes the higher of the two;
    hidden_risk flags people HR rates Low whose performance is falling sharply.
    """
    latest = history.loc[history["is_latest_review"],
                         ["employee_id", "previous_rating", "rating", "rating_change"]] \
        .rename(columns={"rating": "latest_rating"})
    recent = f"events_last_{LEARNING_RECENT_DAYS}d"
    out = (dim[EMPLOYEE_COLS]
           .merge(talent[["employee_id", "attrition_risk"]].rename(columns={"attrition_risk": "hr_attrition_risk"}),
                  on="employee_id", how="left", validate="one_to_one")
           .merge(latest, on="employee_id", how="left", validate="one_to_one")
           .merge(learning[["employee_id", recent]], on="employee_id", how="left", validate="one_to_one"))
    change = out["rating_change"]
    known = flag(change.notna())
    out["rating_trend"] = np.select([~known, flag(change < 0), flag(change > 0)],
                                    ["unknown", "declining", "improving"], default="stable")
    out["performance_risk"] = np.select([~known, flag(change <= -2), flag(change < 0)],
                                        ["Unknown", "High", "Medium"], default="Low")
    rank = {level: i for i, level in enumerate(RISK_LEVELS)}
    hr_rank = out["hr_attrition_risk"].map(rank)
    perf_rank = out["performance_risk"].map(rank)  # Unknown -> NaN: no evidence either way
    combined = pd.concat([hr_rank, perf_rank], axis=1).max(axis=1)
    out["combined_risk"] = combined.map(dict(enumerate(RISK_LEVELS)))
    out["risk_alignment"] = np.select(
        [flag(hr_rank.isna() | perf_rank.isna()), flag(hr_rank > perf_rank), flag(perf_rank > hr_rank)],
        ["unknown", "hr_higher", "performance_higher"], default="aligned")
    out["hidden_risk"] = flag((out["performance_risk"] == "High") & (out["hr_attrition_risk"] == "Low"))
    # Data caveat: the trend compares the last two reviews; flag it if either is dated before hire.
    pre_hire = history.groupby("employee_id").tail(2).groupby("employee_id")["recorded_before_hire"].any()
    out["trend_uses_pre_hire_review"] = flag(out["employee_id"].map(pre_hire))
    return out.sort_values("employee_id").reset_index(drop=True)


def gold_team_outcomes(teams: pd.DataFrame, dim: pd.DataFrame, history: pd.DataFrame,
                       projects: pd.DataFrame) -> pd.DataFrame:
    """Q6 - one row per team: people performance next to project outcomes.

    Correlation itself is computed in the notebook; only 27 of 100 teams have projects,
    so any correlation rests on a small sample.
    """
    latest = history.loc[history["is_latest_review"], ["employee_id", "team_id", "rating", "rating_change"]]
    people = latest.groupby("team_id").agg(
        reviewed_employees=("employee_id", "nunique"),
        avg_latest_rating=("rating", "mean"),
        avg_rating_change=("rating_change", "mean"),
        high_performer_rate=("rating", lambda r: (r >= STRONG_RATING).mean()),
    )
    status = pd.crosstab(projects["team_id"], projects["outcome_status"]) \
        .reindex(columns=list(PROJECT_STATUSES), fill_value=0)
    proj = projects.groupby("team_id").agg(project_count=("project_id", "count"),
                                           avg_project_outcome_score=("outcome_score", "mean")).join(status)
    proj["on_time_rate"] = proj["on-time"] / proj["project_count"]
    proj = proj.rename(columns={s: f"projects_{s.replace('-', '_')}" for s in PROJECT_STATUSES})
    out = (teams[["team_id", "name", "department"]].rename(columns={"name": "team_name"})
           .merge(dim.groupby("team_id").size().rename("headcount"), left_on="team_id", right_index=True, how="left")
           .merge(people, left_on="team_id", right_index=True, how="left")
           .merge(proj, left_on="team_id", right_index=True, how="left"))
    int_cols = ["headcount", "reviewed_employees", "project_count", *[f"projects_{s.replace('-', '_')}"
                                                                      for s in PROJECT_STATUSES]]
    out[int_cols] = out[int_cols].fillna(0).astype("int64")
    out["has_projects"] = out["project_count"] > 0
    for col in ("avg_latest_rating", "avg_rating_change", "high_performer_rate", "avg_project_outcome_score",
                "on_time_rate"):
        out[col] = out[col].astype("Float64").round(3)
    return out.sort_values("team_id").reset_index(drop=True)


def gold_skill_distribution(skill_gaps: pd.DataFrame) -> pd.DataFrame:
    """Q7 - proficiency per competency x department.

    The per-level counts (proficiency_level_1..5) let the notebook re-aggregate to
    org-wide or category level exactly, without averaging averages.
    """
    keys = ["competency_id", "competency_name", "category", "department"]
    agg = skill_gaps.groupby(keys).agg(
        employees_assessed=("employee_id", "nunique"),
        avg_proficiency=("proficiency_level", "mean"),
        avg_required=("required_level", "mean"),
        meeting_requirement_rate=("gap_levels", lambda g: (g <= 0).mean()),
        critical_gaps=("is_critical_gap", "sum"),
    )
    levels = pd.crosstab([skill_gaps[k] for k in keys], skill_gaps["proficiency_level"]) \
        .reindex(columns=[1, 2, 3, 4, 5], fill_value=0)
    levels.columns = [f"proficiency_level_{c}" for c in levels.columns]
    out = agg.join(levels).reset_index()
    for col in ("avg_proficiency", "avg_required", "meeting_requirement_rate"):
        out[col] = out[col].astype("Float64").round(3)
    out["critical_gaps"] = out["critical_gaps"].astype("int64")
    return out.sort_values(["competency_id", "department"]).reset_index(drop=True)


GoldBuilder = Callable[[Mapping[str, pd.DataFrame], date], pd.DataFrame]

# Gold table -> (declared inputs, builder). Order matters: a table may only use tables above it.
# compute_gold() hands each builder ONLY its declared inputs, so an undeclared dependency
# fails immediately - which is what keeps the recorded lineage truthful.
GOLD_TABLES: dict[str, tuple[tuple[str, ...], GoldBuilder]] = {
    "employee_performance_history": (
        ("silver/performance_reviews", "silver/dim_employee"),
        lambda i, _as_of: gold_performance_history(i["silver/performance_reviews"], i["silver/dim_employee"])),
    "employee_skill_gaps": (
        ("silver/competency_assessments", "silver/competencies", "silver/dim_employee"),
        lambda i, _as_of: gold_skill_gaps(i["silver/competency_assessments"], i["silver/competencies"],
                                         i["silver/dim_employee"])),
    "promotion_readiness": (
        ("silver/dim_employee", "silver/talent_review", "gold/employee_performance_history",
         "gold/employee_skill_gaps"),
        lambda i, _as_of: gold_promotion_readiness(i["silver/dim_employee"], i["silver/talent_review"],
                                                  i["gold/employee_performance_history"],
                                                  i["gold/employee_skill_gaps"])),
    "employee_learning_summary": (
        ("silver/learning_events", "silver/dim_employee"),
        lambda i, as_of: gold_learning_summary(i["silver/learning_events"], i["silver/dim_employee"], as_of)),
    "attrition_risk": (
        ("silver/dim_employee", "silver/talent_review", "gold/employee_performance_history",
         "gold/employee_learning_summary"),
        lambda i, _as_of: gold_attrition_risk(i["silver/dim_employee"], i["silver/talent_review"],
                                             i["gold/employee_performance_history"],
                                             i["gold/employee_learning_summary"])),
    "team_performance_outcomes": (
        ("silver/teams", "silver/dim_employee", "gold/employee_performance_history", "silver/projects"),
        lambda i, _as_of: gold_team_outcomes(i["silver/teams"], i["silver/dim_employee"],
                                            i["gold/employee_performance_history"], i["silver/projects"])),
    "skill_distribution": (
        ("gold/employee_skill_gaps",),
        lambda i, _as_of: gold_skill_distribution(i["gold/employee_skill_gaps"])),
}


def compute_gold(silver: Mapping[str, pd.DataFrame], as_of: date) -> dict[str, pd.DataFrame]:
    """All Gold tables from Silver inputs (pure: no I/O, so fully unit-testable)."""
    available: dict[str, pd.DataFrame] = {f"silver/{name}": df for name, df in silver.items()}
    gold: dict[str, pd.DataFrame] = {}
    for name, (inputs, build) in GOLD_TABLES.items():
        gold[name] = build({key: available[key] for key in inputs}, as_of)
        available[f"gold/{name}"] = gold[name]
    return gold


def check_gold(silver: Mapping[str, pd.DataFrame], gold: Mapping[str, pd.DataFrame]) -> None:
    """Gold validation checklist: grain is unique, nothing dropped, aggregates add up."""
    problems: list[str] = []
    for name, grain in GOLD_GRAIN.items():
        dupes = int(gold[name].duplicated(list(grain)).sum())
        if dupes:
            problems.append(f"{name}: {dupes} rows violate grain {grain}")
        if gold[name].empty:
            log_event(logging.WARNING, "gold table is empty", table=name)
    employees = len(silver["dim_employee"])
    expected_rows = {
        "employee_performance_history": len(silver["performance_reviews"]),
        "employee_skill_gaps": len(silver["competency_assessments"]
                                   .drop_duplicates(["employee_id", "competency_id"])),
        "promotion_readiness": employees, "employee_learning_summary": employees, "attrition_risk": employees,
        "team_performance_outcomes": len(silver["teams"]),
    }
    for name, expected in expected_rows.items():
        if len(gold[name]) != expected:
            problems.append(f"{name}: {len(gold[name])} rows, expected {expected} (rows dropped or fanned out)")
    sums = {
        "team headcount == employees": (int(gold["team_performance_outcomes"]["headcount"].sum()), employees),
        "learning events == silver events": (int(gold["employee_learning_summary"]["total_events"].sum()),
                                             len(silver["learning_events"])),
        "skill_distribution assessed == skill gaps": (int(gold["skill_distribution"]["employees_assessed"].sum()),
                                                      len(gold["employee_skill_gaps"])),
        "team projects == projects": (int(gold["team_performance_outcomes"]["project_count"].sum()),
                                      len(silver["projects"])),
        "pre-hire events == silver flags": (int(gold["employee_learning_summary"]["events_before_hire"].sum()),
                                            int(silver["learning_events"]["recorded_before_hire"].sum())),
        "pre-hire reviews == silver flags": (
            int(gold["employee_performance_history"]["recorded_before_hire"].sum()),
            int(silver["performance_reviews"]["recorded_before_hire"].sum())),
    }
    for label, (got, expected) in sums.items():
        if got != expected:
            problems.append(f"reconciliation {label}: {got} != {expected}")
    if problems:
        raise RuntimeError("Gold validation failed: " + "; ".join(problems))


def read_silver(lake: Lake, settings: Settings, entity: str) -> pd.DataFrame:
    prefix = f"{SILVER}/{entity}"
    if not lake.list_objects(settings.lake_bucket, f"{prefix}/"):
        raise RuntimeError(f"no Silver data at s3://{settings.lake_bucket}/{prefix}/ - run the silver step first")
    df = lake.read_parquet(settings.lake_bucket, prefix)
    # Hive partition columns come back as categories; Gold doesn't need them (event_date has the date).
    return df.drop(columns=["event_year", "event_month"], errors="ignore")


def build_gold(ctx: RunContext, lake: Lake) -> None:
    """Build the 7 Gold tables (one per business question), validate, then publish."""
    s = ctx.settings
    with ctx.step("gold") as result:
        silver = {entity: read_silver(lake, s, entity) for entity in SILVER_INPUTS}
        gold = compute_gold(silver, s.run_date)
        check_gold(silver, gold)  # validate everything before publishing anything
        for name, df in gold.items():
            rows = lake.write_parquet(df.assign(_run_id=ctx.run_id, _as_of_date=s.run_date.isoformat()),
                                      s.lake_bucket, f"{GOLD}/{name}/")
            log_event(logging.INFO, "gold table written", table=name, rows=rows, grain=list(GOLD_GRAIN[name]))
            result.metrics[f"{name}_rows"] = rows


STEPS = ("bronze", "silver", "gold")


# --- Lineage & run manifests -----------------------------------------------------


def dataset_lineage(settings: Settings) -> dict[str, list[str]]:
    """Dataset-level lineage: each lake dataset -> the datasets it is built from.

    Derived from the same structures the pipeline executes (SOURCE_TABLES, FOREIGN_KEYS,
    GOLD_TABLES), so it can't drift from what the code actually does.
    """
    source = f"postgres://{settings.postgres_host}/{settings.postgres_name}/{settings.source_schema}"
    lineage: dict[str, list[str]] = {
        f"{BRONZE_LEARNING}": [f"s3://{settings.landing_bucket}/{LANDING_LEARNING_PREFIX}"],
    }
    for table in SOURCE_TABLES:
        lineage[f"{BRONZE_POSTGRES}/{table}"] = [f"{source}.{table}"]
    for table in SOURCE_TABLES:
        parents = [f"{SILVER}/{parent}" for _, parent in FOREIGN_KEYS.get(table, ())]
        lineage[f"{SILVER}/{table}"] = [f"{BRONZE_POSTGRES}/{table}", *parents]
        lineage[f"{QUARANTINE}/{table}"] = lineage[f"{SILVER}/{table}"]
    lineage[f"{SILVER}/learning_events"] = [BRONZE_LEARNING, f"{SILVER}/employees"]
    lineage[f"{QUARANTINE}/learning_events"] = lineage[f"{SILVER}/learning_events"]
    lineage[f"{SILVER}/dim_employee"] = [f"{SILVER}/employees", f"{SILVER}/teams"]
    for name, (inputs, _) in GOLD_TABLES.items():
        lineage[f"{GOLD}/{name}"] = list(inputs)
    return lineage


def lineage_mermaid(settings: Settings) -> str:
    """Render dataset lineage as a Mermaid flowchart (used in LINEAGE.md and the notebook)."""
    lineage = dataset_lineage(settings)
    nodes = sorted({n for target, inputs in lineage.items() for n in (target, *inputs)})
    ids = {n: f"n{i}" for i, n in enumerate(nodes)}
    lines = ["flowchart LR"]
    lines += [f'    {ids[n]}["{n}"]' for n in nodes]
    lines += [f"    {ids[i]} --> {ids[t]}" for t, inputs in lineage.items() for i in inputs]
    return "\n".join(lines)


def code_fingerprint() -> str:
    """SHA-256 of this file: ties every output to the exact code that produced it."""
    try:
        with open(__file__, "rb") as fh:
            return hashlib.sha256(fh.read()).hexdigest()
    except OSError:
        return "unknown"


def package_versions() -> dict[str, str]:
    versions = {}
    for package in ("pandas", "numpy", "pyarrow", "boto3", "psycopg2-binary"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "not installed"
    return versions


def build_manifest(ctx: RunContext, steps_requested: list[str], exit_code: int) -> dict[str, Any]:
    """Everything needed to audit a run: what ran, how it went, with which code, on which data."""
    s = ctx.settings
    return {
        **ctx.summary(),
        "exit_code": exit_code,
        "steps_requested": steps_requested,
        "code": {"sha256": code_fingerprint(), "python": platform.python_version(), "packages": package_versions()},
        "config": {  # never secrets
            "is_local": s.is_local,
            "source": f"postgres://{s.postgres_host}:{s.postgres_port}/{s.postgres_name}/{s.source_schema}",
            "landing_bucket": s.landing_bucket, "lake_bucket": s.lake_bucket, "aws_region": s.aws_region,
        },
        "lineage": dataset_lineage(s),
    }


def write_manifest(lake: Lake, ctx: RunContext, manifest: Mapping[str, Any]) -> str | None:
    """Store the manifest; on a complete successful run also move the ``latest.json`` pointer.

    Best effort: a manifest failure is logged but never changes the run's exit code
    (e.g. if S3 itself is down, preflight already reported the real problem).
    """
    s = ctx.settings
    key = f"{RUNS}/run_date={s.run_date.isoformat()}/{ctx.run_id}.json"
    try:
        lake.put_json(s.lake_bucket, key, manifest)
        if manifest["status"] == "succeeded" and set(manifest["steps_requested"]) == set(STEPS):
            lake.put_json(s.lake_bucket, f"{RUNS}/latest.json", {
                "run_id": ctx.run_id, "run_date": s.run_date.isoformat(),
                "finished_at": manifest["finished_at"], "manifest": key,
            })
    except Exception as exc:
        log_event(logging.ERROR, "could not write run manifest", key=key, error=f"{type(exc).__name__}: {exc}")
        return None
    log_event(logging.INFO, "run manifest written", path=f"s3://{s.lake_bucket}/{key}")
    return key


# =============================================================================
# 8. Entry point
# =============================================================================


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    # Not __doc__: it is None when Python runs with -OO (docstrings stripped).
    parser = argparse.ArgumentParser(description="ACME Performance & Talent batch ETL (bronze -> silver -> gold)")
    parser.add_argument(
        "--steps", default=",".join(STEPS),
        help=f"comma-separated subset of {','.join(STEPS)} to run (default: all)",
    )
    args = parser.parse_args(argv)
    args.steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = sorted(set(args.steps) - set(STEPS))
    if unknown:
        parser.error(f"unknown step(s): {', '.join(unknown)}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the pipeline. Exit codes: 0 success, 1 step failure, 2 bad configuration."""
    args = parse_args(argv)
    setup_logging(os.environ.get("LOG_LEVEL", "INFO").upper())
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        log_event(logging.ERROR, "invalid configuration", error=str(exc))
        return 2

    ctx = RunContext(settings)
    _LOG_CONTEXT["run_id"] = ctx.run_id
    log_event(logging.INFO, "run started", run_date=settings.run_date.isoformat(), is_local=settings.is_local,
              steps=args.steps, source=f"{settings.postgres_host}/{settings.postgres_name}.{settings.source_schema}",
              landing_bucket=settings.landing_bucket, lake_bucket=settings.lake_bucket)

    db, lake = SourceDatabase(settings), Lake(settings)
    try:
        preflight(ctx, db, lake)
        if "bronze" in args.steps:
            ingest_bronze(ctx, db, lake)
        if "silver" in args.steps:
            build_silver(ctx, lake)
        if "gold" in args.steps:
            build_gold(ctx, lake)
    except Exception:
        # Failures inside a step were already logged (with traceback) by ctx.step.
        if not any(s.status == "failed" for s in ctx.steps):
            log.error("run aborted outside a step", exc_info=True)
            ctx.steps.append(StepResult(name="unknown", started_at=datetime.now(UTC).isoformat(), status="failed"))
    summary = ctx.summary()
    exit_code = 0 if summary["status"] == "succeeded" else 1
    write_manifest(lake, ctx, build_manifest(ctx, args.steps, exit_code))  # failed runs get one too
    log_event(logging.INFO if exit_code == 0 else logging.ERROR, "run finished", status=summary["status"],
              **({"alert": True} if exit_code else {}),
              duration_s=round(sum(s["duration_s"] or 0 for s in summary["steps"]), 3))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
