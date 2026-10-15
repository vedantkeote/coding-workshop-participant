#!/usr/bin/env bash
# Seeds the two *source systems* the pipeline ingests from. This is environment
# setup, not part of the pipeline: in a real company these systems already exist.
#
#   1. PostgreSQL schema "hr"  <- the 7 CSV tables (via the dataset's own load.sh)
#   2. S3 landing bucket       <- learning_activity/ JSONL files, partitions kept as-is
#
# Safe to re-run: the schema is dropped/recreated and `s3 sync` only uploads changes.
# Uses the workshop's standard env vars (IS_LOCAL, POSTGRES_*); local defaults
# mirror infra/locals.tf.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SOURCE_DATA_DIR="${SOURCE_DATA_DIR:-${REPO_ROOT}/sampleData/data-engineering-performance-talent-management}"
SOURCE_SCHEMA="${SOURCE_SCHEMA:-hr}"

export IS_LOCAL="${IS_LOCAL:-true}"
if [ "${IS_LOCAL}" = "true" ]; then
    export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
    export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
    export POSTGRES_NAME="${POSTGRES_NAME:-postgres}"
    export POSTGRES_USER="${POSTGRES_USER:-postgres}"
    export POSTGRES_PASS="${POSTGRES_PASS:-postgres123}"
    export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost.localstack.cloud:4566}"
    export AWS_ACCESS_KEY_ID="test" AWS_SECRET_ACCESS_KEY="test"
    export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
    SSLMODE=""
else
    SSLMODE="sslmode=require"
fi
LANDING_BUCKET="${LANDING_BUCKET:-coding-workshop-landing-${PARTICIPANT_ID:?PARTICIPANT_ID must be set}}"

if [ ! -f "${SOURCE_DATA_DIR}/load.sh" ]; then
    echo "ERROR: dataset not found at ${SOURCE_DATA_DIR} (set SOURCE_DATA_DIR)" >&2
    exit 1
fi

echo "==> PostgreSQL: recreating schema '${SOURCE_SCHEMA}' on ${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_NAME}"
CONN="host=${POSTGRES_HOST} port=${POSTGRES_PORT} dbname=${POSTGRES_NAME} user=${POSTGRES_USER} password=${POSTGRES_PASS} ${SSLMODE}"
psql "${CONN}" -v ON_ERROR_STOP=1 -q -c "DROP SCHEMA IF EXISTS ${SOURCE_SCHEMA} CASCADE; CREATE SCHEMA ${SOURCE_SCHEMA};"

# load.sh creates unqualified table names; search_path points them at our schema.
PGOPTIONS="-c search_path=${SOURCE_SCHEMA}" bash "${SOURCE_DATA_DIR}/load.sh"

echo "==> S3: syncing learning_activity/ to s3://${LANDING_BUCKET}/"
aws s3api head-bucket --bucket "${LANDING_BUCKET}" 2>/dev/null \
    || aws s3 mb "s3://${LANDING_BUCKET}"
aws s3 sync "${SOURCE_DATA_DIR}/learning_activity/" "s3://${LANDING_BUCKET}/learning_activity/" --only-show-errors

echo "==> Verification"
for table in teams competencies employees projects performance_reviews competency_assessments talent_review; do
    printf "%s=%s " "${table}" "$(psql "${CONN}" -At -c "SELECT count(*) FROM ${SOURCE_SCHEMA}.${table}")"
done
echo
echo "s3 objects: $(aws s3 ls "s3://${LANDING_BUCKET}/learning_activity/" --recursive | wc -l)"
