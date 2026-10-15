#!/usr/bin/env bash
# Seeds the two source systems IN AWS, for the cloud run of talent-pipeline.
# Run it AFTER ./bin/deploy-backend.sh has created Aurora and the EKS cluster
# (with TF_VAR_aws_eks_enabled=true). Cloud counterpart of seed_sources.sh.
#
#   1. S3 landing bucket  <- learning_activity/ JSONL files, uploaded from this machine
#   2. Aurora schema "hr" <- the 7 CSVs, loaded by the dataset's own load.sh running in a
#                            one-off pod INSIDE the EKS cluster (Aurora is not reachable from
#                            the VDI; the cluster sits in its network). The pod reads the DB
#                            password from the Kubernetes Secret created by infra/eks.tf, so the
#                            password never passes through this machine.
#
# Safe to re-run: the bucket sync only uploads changes; the hr schema is dropped and recreated.
set -euo pipefail
export AWS_PAGER=""   # never open "less" for AWS CLI output (it would pause the script)

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SOURCE_DATA_DIR="${SOURCE_DATA_DIR:-${REPO_ROOT}/sampleData/data-engineering-performance-talent-management}"
SOURCE_SCHEMA="${SOURCE_SCHEMA:-hr}"
PROJECT="coding-workshop"
JOB_NAME="talent-pipeline"                                  # folder name under data/
DB_SECRET="${PROJECT}-${JOB_NAME}-env"                      # created by the Helm job (infra/eks.tf)
POD="hr-source-seed"
POD_IMAGE="postgres:17"                                     # has psql + bash + tar (needed by kubectl cp)

# --- Safety: this script targets REAL AWS ------------------------------------------
if [ -n "${AWS_ENDPOINT_URL:-}${AWS_ENDPOINT_URL_S3:-}" ]; then
    echo "ERROR: AWS_ENDPOINT_URL is set (LocalStack?). This script seeds real AWS; for local use seed_sources.sh." >&2
    exit 1
fi
if [ ! -f "${SOURCE_DATA_DIR}/load.sh" ]; then
    echo "ERROR: dataset not found at ${SOURCE_DATA_DIR} (set SOURCE_DATA_DIR)" >&2
    exit 1
fi
for tool in aws kubectl jq; do
    command -v "$tool" > /dev/null || { echo "ERROR: '$tool' is missing" >&2; exit 1; }
done

# --- Credentials: same as bin/deploy-backend.sh (fresh temporary credentials) -------
"${REPO_ROOT}/bin/setup-participant.sh" > /dev/null
# shellcheck source=/dev/null
source "${REPO_ROOT}/ENVIRONMENT.config"
: "${PARTICIPANT_ID:?PARTICIPANT_ID must be set}"
: "${AWS_REGION:?AWS_REGION must be set (see docs/validation.md)}"
LANDING_BUCKET="${LANDING_BUCKET:-${PROJECT}-landing-${PARTICIPANT_ID}}"
CLUSTER="${PROJECT}-rds-${PARTICIPANT_ID}"

echo "==> AWS account $(aws sts get-caller-identity --query Account --output text), region ${AWS_REGION}"

# --- 1. S3 landing bucket ---------------------------------------------------------
echo "==> S3: s3://${LANDING_BUCKET}/learning_activity/"
if ! aws s3api head-bucket --bucket "${LANDING_BUCKET}" > /dev/null 2>&1; then
    aws s3 mb "s3://${LANDING_BUCKET}" --region "${AWS_REGION}"
fi
aws s3 sync "${SOURCE_DATA_DIR}/learning_activity/" "s3://${LANDING_BUCKET}/learning_activity/" --only-show-errors
echo "    objects: $(aws s3 ls "s3://${LANDING_BUCKET}/learning_activity/" --recursive | wc -l)"

# --- 2. Aurora "hr" schema, loaded from inside the cluster ---------------------------
echo "==> Aurora: cluster ${CLUSTER}"
read -r PG_HOST PG_PORT PG_NAME PG_USER < <(aws rds describe-db-clusters --db-cluster-identifier "${CLUSTER}" \
    --query 'DBClusters[0].[Endpoint,Port,DatabaseName,MasterUsername]' --output text)
echo "    endpoint ${PG_HOST}:${PG_PORT}, database ${PG_NAME}, user ${PG_USER}"

aws eks update-kubeconfig --region "${AWS_REGION}" --name "${PROJECT}-${PARTICIPANT_ID}" > /dev/null
kubectl get secret "${DB_SECRET}" > /dev/null || {
    echo "ERROR: Kubernetes Secret ${DB_SECRET} not found - deploy with TF_VAR_aws_eks_enabled=true first" >&2
    exit 1
}

# On exit (including Ctrl+C) remove the pod without waiting. Before creating it, remove any
# leftover pod and WAIT until it is gone - otherwise "object is being deleted ... already exists".
cleanup() { kubectl delete pod "${POD}" --ignore-not-found --grace-period=1 --wait=false > /dev/null 2>&1 || true; }
trap cleanup EXIT
kubectl delete pod "${POD}" --ignore-not-found --grace-period=1 --wait=true --timeout=90s > /dev/null

OVERRIDES=$(jq -n --arg pod "$POD" --arg image "$POD_IMAGE" --arg secret "$DB_SECRET" \
    --arg host "$PG_HOST" --arg port "$PG_PORT" --arg name "$PG_NAME" --arg user "$PG_USER" '{
  spec: {terminationGracePeriodSeconds: 1, containers: [{
    name: $pod, image: $image, command: ["sleep", "1800"],
    env: [{name: "IS_LOCAL", value: "false"}, {name: "POSTGRES_HOST", value: $host},
          {name: "POSTGRES_PORT", value: $port}, {name: "POSTGRES_NAME", value: $name},
          {name: "POSTGRES_USER", value: $user}],
    envFrom: [{secretRef: {name: $secret}}]
  }]}}')
kubectl run "${POD}" --image="${POD_IMAGE}" --restart=Never --overrides="${OVERRIDES}" > /dev/null
kubectl wait --for=condition=Ready "pod/${POD}" --timeout=300s > /dev/null
echo "    seed pod ready"

# kubectl cp/exec stream data through the Kubernetes API. Check that works at all (with a time
# limit) so a connectivity problem fails clearly instead of hanging.
echo "    checking kubectl exec into the pod..."
if ! timeout 60 kubectl exec "${POD}" -- true; then
    echo "ERROR: 'kubectl exec' into the pod did not work within 60 s - the copy step would hang." >&2
    echo "       Check: kubectl get pod ${POD} -o wide ; kubectl exec ${POD} -- echo ok" >&2
    exit 1
fi

# One tar stream for all files (what kubectl cp does internally, once instead of 9 times).
FILES=(schema.sql load.sh teams.csv competencies.csv employees.csv projects.csv
       performance_reviews.csv competency_assessments.csv talent_review.csv)
echo "    copying ${#FILES[@]} files ($(du -ch "${FILES[@]/#/${SOURCE_DATA_DIR}/}" | tail -1 | cut -f1)) into the pod..."
tar -C "${SOURCE_DATA_DIR}" -cf - "${FILES[@]}" \
    | timeout 300 kubectl exec -i "${POD}" -- sh -c 'mkdir -p /seed && tar -xf - -C /seed'
echo "    copied: $(timeout 60 kubectl exec "${POD}" -- sh -c 'ls /seed | wc -l') files"

# Runs inside the pod. Aurora Serverless may be paused (min capacity 0): retry the first
# connection for up to ~2 minutes while it resumes.
echo "    loading into Aurora (waits up to 2 min if Aurora is paused)..."
timeout 900 kubectl exec "${POD}" -- env SCHEMA="${SOURCE_SCHEMA}" bash -euo pipefail -c '
CONN="host=$POSTGRES_HOST port=$POSTGRES_PORT dbname=$POSTGRES_NAME user=$POSTGRES_USER password=$POSTGRES_PASS sslmode=require connect_timeout=10"
for i in $(seq 1 24); do
    psql "$CONN" -Atqc "SELECT 1" > /dev/null 2>&1 && break
    [ "$i" -eq 24 ] && { echo "ERROR: Aurora not reachable from the cluster" >&2; exit 1; }
    echo "    waiting for Aurora ($i/24)..."; sleep 5
done
psql "$CONN" -v ON_ERROR_STOP=1 -q -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE; CREATE SCHEMA $SCHEMA;"
PGOPTIONS="-c search_path=$SCHEMA" bash /seed/load.sh
echo "==> Verification"
for t in teams competencies employees projects performance_reviews competency_assessments talent_review; do
    printf "%s=%s " "$t" "$(psql "$CONN" -Atc "SELECT count(*) FROM $SCHEMA.$t")"
done
echo'
echo "==> Done. Re-run the pipeline:"
echo "    kubectl delete job ${PROJECT}-${JOB_NAME} --ignore-not-found"
echo "    ./bin/deploy-backend.sh"
echo "    kubectl logs -f job/${PROJECT}-${JOB_NAME}"
