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
# 1a. Phase 1 deploy: bucket + namespace only. Firehose is held back
# until the Iceberg table exists; Firehose validates the destination
# table at create time.
# -----------------------------------------------------------------------------
log "Phase 1: deploying bucket + namespace..."
aws cloudformation deploy \
    --stack-name "${ICEBERG_STACK_NAME}" \
    --template-file "${SCRIPT_DIR}/../cloudformation-iceberg.yaml" \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "BucketSuffix=${ICEBERG_BUCKET_SUFFIX}" \
        "EnableFirehose=false" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
ok "Phase 1 deployed (bucket + namespace + IAM)"

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
# 2b. Phase 2 deploy: now that the Iceberg table exists, add Firehose.
# -----------------------------------------------------------------------------
log "Phase 2: adding Firehose to the stack..."
aws cloudformation deploy \
    --stack-name "${ICEBERG_STACK_NAME}" \
    --template-file "${SCRIPT_DIR}/../cloudformation-iceberg.yaml" \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "BucketSuffix=${ICEBERG_BUCKET_SUFFIX}" \
        "EnableFirehose=true" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
ok "Phase 2 deployed (Firehose live)"

# -----------------------------------------------------------------------------
# 3. Wire Redshift: attach Spectrum role to the namespace, grant Lake
# Formation read on the federated catalog, then create the external
# schema. Run DDL as admin via the admin secret.
# -----------------------------------------------------------------------------
SPECTRUM_ROLE_ARN=$(stack_output_from "${ICEBERG_STACK_NAME}" RedshiftSpectrumRoleArn)
[ -n "${SPECTRUM_ROLE_ARN}" ] || err "RedshiftSpectrumRoleArn export missing"
log "Spectrum role: ${SPECTRUM_ROLE_ARN}"

# 3a. Attach the Spectrum role to the Redshift Serverless namespace.
# Idempotent: if already attached, this is a no-op (we read existing
# roles, dedupe, and only call update if the new role isn't there).
NAMESPACE_NAME=$(aws redshift-serverless get-workgroup \
    --workgroup-name "${REDSHIFT_WORKGROUP}" \
    --region "${AWS_REGION}" \
    --query 'workgroup.namespaceName' --output text)
EXISTING_ROLES=$(aws redshift-serverless get-namespace \
    --namespace-name "${NAMESPACE_NAME}" \
    --region "${AWS_REGION}" \
    --query 'namespace.iamRoles' --output text)
if echo "${EXISTING_ROLES}" | grep -q "${SPECTRUM_ROLE_ARN}"; then
    ok "Spectrum role already attached to namespace"
else
    log "Attaching Spectrum role to namespace ${NAMESPACE_NAME}..."
    # Build the JSON list of roles: existing + new. Each role string
    # in the API is the bare ARN; we wrap in a JSON array.
    ROLES_JSON=$(python3 - <<PY
import json, os
existing = os.environ.get("EXISTING", "").split() or []
new = os.environ["NEW"]
roles = [r for r in existing if r and r != "None"]
if new not in roles: roles.append(new)
print(json.dumps(roles))
PY
)
    EXISTING="${EXISTING_ROLES}" NEW="${SPECTRUM_ROLE_ARN}" \
    aws redshift-serverless update-namespace \
        --namespace-name "${NAMESPACE_NAME}" \
        --iam-roles "${ROLES_JSON}" \
        --default-iam-role-arn "${SPECTRUM_ROLE_ARN}" \
        --region "${AWS_REGION}" >/dev/null
    log "Waiting for namespace IAM update to apply..."
    for _ in $(seq 1 30); do
        STATUS=$(aws redshift-serverless get-namespace \
            --namespace-name "${NAMESPACE_NAME}" \
            --region "${AWS_REGION}" \
            --query 'namespace.status' --output text)
        [ "${STATUS}" = "AVAILABLE" ] && break
        sleep 3
    done
    ok "Spectrum role attached"
fi

# 3b. Grant Lake Formation DESCRIBE+SELECT to the Spectrum role on the
# bucket-nested federated catalog. Without this, Redshift can call Glue
# but Lake Formation blocks the actual data read.
log "Granting Lake Formation permissions to Spectrum role..."
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${SPECTRUM_ROLE_ARN}" \
    --resource "{\"Catalog\":{\"Id\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\"}}" \
    --permissions DESCRIBE \
    --region "${AWS_REGION}" 2>&1 | tail -2 || warn "Catalog DESCRIBE grant may already exist"
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${SPECTRUM_ROLE_ARN}" \
    --resource "{\"Database\":{\"CatalogId\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\",\"Name\":\"${NAMESPACE}\"}}" \
    --permissions DESCRIBE \
    --region "${AWS_REGION}" 2>&1 | tail -2 || warn "Database DESCRIBE grant may already exist"
aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${SPECTRUM_ROLE_ARN}" \
    --resource "{\"Table\":{\"CatalogId\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\",\"DatabaseName\":\"${NAMESPACE}\",\"TableWildcard\":{}}}" \
    --permissions SELECT DESCRIBE \
    --region "${AWS_REGION}" 2>&1 | tail -2 || warn "Table SELECT grant may already exist"
ok "Lake Formation permissions granted"

# 3c. Create the Redshift external schema.
SECRET_NAME=$(stack_output_from "${STACK_NAME}" RedshiftAdminSecretName)
[ -n "${SECRET_NAME}" ] || err "RedshiftAdminSecretName export missing on ${STACK_NAME}"
SECRET_ARN=$(aws secretsmanager describe-secret \
    --secret-id "${SECRET_NAME}" \
    --region "${AWS_REGION}" \
    --query 'ARN' --output text)
[ -n "${SECRET_ARN}" ] || err "Could not resolve admin secret ARN"

CATALOG_ID="${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}"
log "Creating Redshift external schema 'cold' against ${CATALOG_ID}..."
EXT_SCHEMA_SQL=$(cat <<EOF
DROP SCHEMA IF EXISTS cold CASCADE;
CREATE EXTERNAL SCHEMA cold
FROM DATA CATALOG
DATABASE '${NAMESPACE}'
IAM_ROLE '${SPECTRUM_ROLE_ARN}'
CATALOG_ID '${CATALOG_ID}';
EOF
)
redshift_data_run_or_ignore "${EXT_SCHEMA_SQL}" "${SECRET_ARN}" "already exists"

ok "Iceberg cold path ready. Once Firehose flushes (60s buffer), run:"
echo "    SELECT COUNT(*) FROM cold.cdc_events_archive;"
echo
echo "To create the hot+cold UNION view, run schema/redshift_iceberg_external.sql."
