"""Shared fixtures. Integration fixtures use throwaway buckets so tests never touch the real lake."""

import uuid

import pytest

import job


@pytest.fixture
def local_settings():
    return job.Settings.from_env({"PARTICIPANT_ID": "it"})


@pytest.fixture
def make_bucket(local_settings):
    """Factory: create empty scratch buckets; all are emptied and deleted after the test."""
    lake = job.Lake(local_settings)
    created = []

    def _make(kind: str = "test") -> str:
        bucket = f"coding-workshop-{kind}-{uuid.uuid4().hex[:8]}"
        lake.ensure_bucket(bucket, create=True)
        created.append(bucket)
        return bucket

    yield _make
    for bucket in created:
        for obj in lake.list_objects(bucket, ""):
            lake.s3.delete_object(Bucket=bucket, Key=obj["Key"])
        for obj in lake.s3.list_objects_v2(Bucket=bucket).get("Contents", []):  # folder markers
            lake.s3.delete_object(Bucket=bucket, Key=obj["Key"])
        lake.s3.delete_bucket(Bucket=bucket)
