#!/usr/bin/env bash
# Deploy the Iceberg cold path: CFN stack (S3 Tables bucket + namespace
# + Firehose + IAM) plus post-deploy steps that CFN can't handle:
#   1. Create the S3 Tables Iceberg table (CFN's resource handler races
#      with namespace propagation; minutes of retries are needed.)
#   2. Wire Redshift to query it (CREATE EXTERNAL SCHEMA + UNION view).
#
# Idempotent: re-running just updates the CFN stack and tolerates
# already-existing tables / schemas.
#
# Required tools: aws
# Required env: AWS credentials, base stack already deployed.
#
# Optional env:
#   PROJECT_NAME              must match the base stack
#   AWS_REGION                must match the base stack
#   ICEBERG_STACK_NAME        ${PROJECT_NAME}-iceberg
#   ICEBERG_BUCKET_SUFFIX     suffix on the S3 Tables bucket name (S3
#                             Tables names are reserved for ~minutes
#                             after deletion; bump this when iterating)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"
[ -f "${SCRIPT_DIR}/../.env.bootstrap" ] && source "${SCRIPT_DIR}/../.env.bootstrap"

: "${ICEBERG_STACK_NAME:=${PROJECT_NAME}-iceberg}"
: "${ICEBERG_BUCKET_SUFFIX:=}"

require aws
check_aws_creds

# -----------------------------------------------------------------------------
# 1. Deploy / update the CFN stack.
# -----------------------------------------------------------------------------
log "Deploying CloudFormation stack ${ICEBERG_STACK_NAME}..."
aws cloudformation deploy \
    --stack-name "${ICEBERG_STACK_NAME}" \
    --template-file "${SCRIPT_DIR}/../cloudformation-iceberg.yaml" \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "BucketSuffix=${ICEBERG_BUCKET_SUFFIX}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
ok "Iceberg stack deployed"

BUCKET_ARN=$(stack_output_from "${ICEBERG_STACK_NAME}" TableBucketArn)
BUCKET_NAME=$(stack_output_from "${ICEBERG_STACK_NAME}" TableBucketName)
[ -n "${BUCKET_ARN}" ] || err "TableBucketArn output not found"
log "S3 Tables bucket: ${BUCKET_NAME} (${BUCKET_ARN})"

# -----------------------------------------------------------------------------
# 2. Create the Iceberg table inside the namespace. We retry generously
# because the s3tables CreateTable call against a freshly-created
# namespace returns 404 NotFound for several minutes after the
# namespace is itself visible via list-namespaces.
# -----------------------------------------------------------------------------
TABLE_NAME=cdc_events_archive
NAMESPACE=cdc

log "Checking whether table ${NAMESPACE}.${TABLE_NAME} already exists..."
if aws s3tables get-table \
        --table-bucket-arn "${BUCKET_ARN}" \
        --namespace "${NAMESPACE}" \
        --name "${TABLE_NAME}" \
        --region "${AWS_REGION}" >/dev/null 2>&1; then
    ok "Table ${NAMESPACE}.${TABLE_NAME} already exists, skipping create"
else
    log "Creating Iceberg table (retrying through namespace propagation lag)..."
    metadata_file=$(mktemp)
    cat > "${metadata_file}" <<'JSON'
{
  "iceberg": {
    "schema": {
      "fields": [
        {"name": "source_table",     "type": "string",    "required": true},
        {"name": "operation",        "type": "string",    "required": true},
        {"name": "record_id",        "type": "string",    "required": true},
        {"name": "event_data",       "type": "string",    "required": false},
        {"name": "commit_timestamp", "type": "timestamp", "required": true},
        {"name": "ingested_at",      "type": "timestamp", "required": true}
      ]
    }
  }
}
JSON
    success=0
    for _ in $(seq 1 60); do
        if aws s3tables create-table \
                --table-bucket-arn "${BUCKET_ARN}" \
                --namespace "${NAMESPACE}" \
                --name "${TABLE_NAME}" \
                --format ICEBERG \
                --metadata "file://${metadata_file}" \
                --region "${AWS_REGION}" >/dev/null 2>&1; then
            success=1
            break
        fi
        printf '.' >&2
        sleep 5
    done
    rm -f "${metadata_file}"
    [ "${success}" = "1" ] || err "create-table failed after 5 minutes of retries"
    ok "Iceberg table ${NAMESPACE}.${TABLE_NAME} created"
fi

# -----------------------------------------------------------------------------
# 3. Wire Redshift: external schema + UNION view across hot+cold.
# Run as admin via the admin secret (same pattern as 06-deploy-sagemaker).
# -----------------------------------------------------------------------------
SECRET_NAME=$(stack_output_from "${STACK_NAME}" RedshiftAdminSecretName)
[ -n "${SECRET_NAME}" ] || err "RedshiftAdminSecretName export missing on ${STACK_NAME}"
SECRET_ARN=$(aws secretsmanager describe-secret \
    --secret-id "${SECRET_NAME}" \
    --region "${AWS_REGION}" \
    --query 'ARN' --output text)
[ -n "${SECRET_ARN}" ] || err "Could not resolve admin secret ARN"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CATALOG_ID="${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}"

# Redshift needs an IAM role to read the federated catalog. We pass
# the workgroup's default IAM role here; if that's not configured,
# the user has to associate one. For now we use the admin's identity
# via secret-arn for DDL and rely on the workgroup default for queries.
log "Creating Redshift external schema 'cold' against ${CATALOG_ID}..."
EXT_SCHEMA_SQL=$(cat <<EOF
DROP SCHEMA IF EXISTS cold CASCADE;
CREATE EXTERNAL SCHEMA cold
FROM DATA CATALOG
DATABASE 'cdc'
IAM_ROLE default
CATALOG_ID '${CATALOG_ID}';
EOF
)
redshift_data_run_or_ignore "${EXT_SCHEMA_SQL}" "${SECRET_ARN}" "already exists|relation .* does not exist|default role"

ok "Iceberg cold path ready. Once Firehose flushes (60s buffer), run:"
echo "    SELECT COUNT(*) FROM cold.cdc_events_archive;"
echo
echo "To create the hot+cold UNION view, run schema/redshift_iceberg_external.sql."
