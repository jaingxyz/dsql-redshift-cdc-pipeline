#!/usr/bin/env bash
# Deploy the Iceberg cold path. Three-phase deploy + post-stack wire-up:
#
#   Phase A: CFN deploy with EnableFirehose=true, EnableFirehoseStream=false
#            - provisions bucket, namespace, IAM (Firehose role,
#            transform Lambda + role + log group), and error bucket.
#            Stream is held back so we can apply LF grants against the
#            Firehose role principal that now exists.
#   Step 2:  Create the Iceberg table inside the S3 Tables namespace.
#            Out-of-CFN because the API races namespace propagation
#            (CreateTable returns 404 for several minutes after the
#            namespace is itself visible). Retried for ~5 minutes.
#   Step 3:  Lake Formation grants for the Firehose role on the
#            bucket-nested s3tablescatalog (catalog DESCRIBE; database
#            DESCRIBE+CREATE_TABLE+ALTER; table SELECT+INSERT+ALTER).
#            Required before Phase B because Firehose validates Glue
#            access at stream-create time.
#   Step 4:  Update the transform Lambda's code (the CFN placeholder
#            raises). Wait function-updated so Phase B sees real code.
#   Phase B: CFN deploy with EnableFirehoseStream=true - creates the
#            Firehose delivery stream wired to the now-ready Lambda.
#   Step 5:  Attach the Spectrum role to the Redshift Serverless
#            namespace and grant the namespace-default-role.
#   Step 6:  Lake Formation grants for the Spectrum role on the same
#            catalog + the Glue resource link in the default catalog,
#            then the IAM glue:Get* policy scoped to the resource link.
#   Step 7:  Run schema/redshift_iceberg_external.sql via batch-execute
#            to create the `cold` external schema, the cdc_events_all
#            UNION view, and the *_unified current-state views.
#
# Idempotent: every phase tolerates already-existing resources. Re-runs
# update CFN if needed and skip table creation if the table is already
# present.
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
# Phase A: everything EXCEPT the Firehose delivery stream
# (EnableFirehose=true, EnableFirehoseStream=false). This creates the
# bucket, namespace, IAM roles, the transform Lambda, and the error
# bucket - but not the stream. The only hard ordering constraint in this
# pipeline is namespace -> table -> stream, and Phase A creates the
# namespace, so a separate "bucket + namespace only" pre-phase is
# redundant.
#
# Why NOT toggle EnableFirehose=false first: `aws cloudformation deploy`
# reuses the stack's previous value for any unspecified parameter, but
# more importantly, downgrading EnableFirehose to false on a re-run TEARS
# DOWN the error bucket (and role/log group). CFN then can't delete the
# error bucket while it holds failed-delivery objects, and even when it
# can, recreating a same-named bucket races S3's name-reservation
# cooldown. Keeping EnableFirehose=true throughout means a re-run only
# toggles the stream itself, which is safe.
# -----------------------------------------------------------------------------
log "Phase A: deploying bucket + namespace + IAM + transform Lambda (no stream)..."
aws cloudformation deploy \
    --stack-name "${ICEBERG_STACK_NAME}" \
    --template-file "${SCRIPT_DIR}/../cloudformation-iceberg.yaml" \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "BucketSuffix=${ICEBERG_BUCKET_SUFFIX}" \
        "EnableFirehose=true" \
        "EnableFirehoseStream=false" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
ok "Phase A deployed (bucket + namespace + IAM + transform Lambda)"

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
# Grant Lake Formation perms to the Firehose role BEFORE the stream
# exists. The bucket-Glue catalog is in Lake Formation access control
# mode, and Firehose validates glue:GetTable synchronously at stream
# create time - so the role (created in Phase A) needs LF grants now,
# while the stream itself is still held back. LF grants are idempotent
# and tolerate "already exists" on re-runs.
# -----------------------------------------------------------------------------
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CATALOG_ID_NESTED="${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}"

FIREHOSE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PROJECT_NAME}-iceberg-firehose-role"
log "Granting LF perms to Firehose role ${FIREHOSE_ROLE_ARN}..."
# LF grant-permissions requires the caller to be a Data Lake Admin (and
# to hold glue:GetCatalog on the bucket-nested catalog). If the default
# identity isn't an admin, set LF_ADMIN_PROFILE to a profile that is, or
# add the default identity via `aws lakeformation put-data-lake-settings`.
# lf_grant() aborts on AccessDenied rather than silently skipping -
# a missing grant surfaces later as an opaque Firehose glue:GetTable
# failure when the stream is created.
LF=(aws)
[ -n "${LF_ADMIN_PROFILE:-}" ] && LF=(aws --profile "${LF_ADMIN_PROFILE}")
lf_grant "${LF[@]}" lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${FIREHOSE_ROLE_ARN}" \
    --resource "{\"Catalog\":{\"Id\":\"${CATALOG_ID_NESTED}\"}}" \
    --permissions DESCRIBE \
    --region "${AWS_REGION}"
lf_grant "${LF[@]}" lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${FIREHOSE_ROLE_ARN}" \
    --resource "{\"Database\":{\"CatalogId\":\"${CATALOG_ID_NESTED}\",\"Name\":\"${NAMESPACE}\"}}" \
    --permissions DESCRIBE CREATE_TABLE ALTER \
    --region "${AWS_REGION}"
lf_grant "${LF[@]}" lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${FIREHOSE_ROLE_ARN}" \
    --resource "{\"Table\":{\"CatalogId\":\"${CATALOG_ID_NESTED}\",\"DatabaseName\":\"${NAMESPACE}\",\"TableWildcard\":{}}}" \
    --permissions DESCRIBE SELECT INSERT ALTER \
    --region "${AWS_REGION}"
ok "Firehose role grants applied"

# -----------------------------------------------------------------------------
# Deploy the transform Lambda's real code. Phase A created it with a
# raise-only placeholder (the source is too big for an inline CFN
# ZipFile). Firehose's IcebergDestinationConfiguration uses this Lambda
# to reshape raw DSQL CDC records into the cdc_events_archive column
# layout; without working code, every record fails the schema check and
# lands in the error bucket. Must run BEFORE Phase B wires the stream.
# -----------------------------------------------------------------------------
TRANSFORM_FN="${PROJECT_NAME}-iceberg-transform"
TRANSFORM_SRC="${SCRIPT_DIR}/../../app/firehose_transform.py"
[ -f "${TRANSFORM_SRC}" ] || err "Transform Lambda source not found at ${TRANSFORM_SRC}"
log "Deploying transform Lambda code to ${TRANSFORM_FN}..."
require zip
TRANSFORM_BUILD="$(mktemp -d)"
cp "${TRANSFORM_SRC}" "${TRANSFORM_BUILD}/firehose_transform.py"
( cd "${TRANSFORM_BUILD}" && zip -q firehose_transform.zip firehose_transform.py )
aws lambda update-function-code \
    --function-name "${TRANSFORM_FN}" \
    --zip-file "fileb://${TRANSFORM_BUILD}/firehose_transform.zip" \
    --region "${AWS_REGION}" \
    --query '{Function:FunctionName,Sha:CodeSha256}' \
    --output table
rm -rf "${TRANSFORM_BUILD}"
# update-function-code returns before the new code is active; wait so
# Phase B's stream validation invokes the real handler, not the stub.
aws lambda wait function-updated \
    --function-name "${TRANSFORM_FN}" \
    --region "${AWS_REGION}"
ok "Transform Lambda code deployed"

log "Phase B: creating the Firehose delivery stream..."
aws cloudformation deploy \
    --stack-name "${ICEBERG_STACK_NAME}" \
    --template-file "${SCRIPT_DIR}/../cloudformation-iceberg.yaml" \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "BucketSuffix=${ICEBERG_BUCKET_SUFFIX}" \
        "EnableFirehose=true" \
        "EnableFirehoseStream=true" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
ok "Phase B deployed (Firehose live)"

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
    # in the API is the bare ARN; we wrap in a JSON array. The env-var
    # prefix MUST be on the same line as `python3 - <<PY` (not on the
    # later `aws ... update-namespace` line) - the heredoc captures
    # ROLES_JSON before update-namespace runs, so env vars set there
    # would be too late.
    ROLES_JSON=$(EXISTING="${EXISTING_ROLES}" NEW="${SPECTRUM_ROLE_ARN}" python3 - <<'PY'
import json, os
existing = os.environ.get("EXISTING", "").split() or []
new = os.environ["NEW"]
roles = [r for r in existing if r and r != "None"]
if new not in roles:
    roles.append(new)
print(json.dumps(roles))
PY
)
    aws redshift-serverless update-namespace \
        --namespace-name "${NAMESPACE_NAME}" \
        --iam-roles "${ROLES_JSON}" \
        --default-iam-role-arn "${SPECTRUM_ROLE_ARN}" \
        --region "${AWS_REGION}" >/dev/null
    log "Waiting for namespace IAM update to apply..."
    NAMESPACE_READY=0
    for _ in $(seq 1 30); do
        STATUS=$(aws redshift-serverless get-namespace \
            --namespace-name "${NAMESPACE_NAME}" \
            --region "${AWS_REGION}" \
            --query 'namespace.status' --output text)
        if [ "${STATUS}" = "AVAILABLE" ]; then
            NAMESPACE_READY=1
            break
        fi
        sleep 3
    done
    # Loud-failure guard: without this, a still-MODIFYING namespace
    # would let Phase B race the IAM update, and downstream Spectrum
    # queries would intermittently fail with stale-role errors.
    [ "${NAMESPACE_READY}" = "1" ] || \
        err "Namespace ${NAMESPACE_NAME} did not return to AVAILABLE within 90s (last status: ${STATUS})"
    ok "Spectrum role attached"
fi

# 3b. Grant Lake Formation DESCRIBE+SELECT to the Spectrum role on the
# bucket-nested federated catalog. Without this, Redshift can call Glue
# but Lake Formation blocks the actual data read. Same loud-failure
# invariant as the Firehose grants above: AccessDenied on these grants
# means the caller isn't a Data Lake Admin, and the resulting Spectrum
# read failures from Redshift surface as opaque Glue errors hours later.
log "Granting Lake Formation permissions to Spectrum role..."
lf_grant "${LF[@]}" lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${SPECTRUM_ROLE_ARN}" \
    --resource "{\"Catalog\":{\"Id\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\"}}" \
    --permissions DESCRIBE \
    --region "${AWS_REGION}"
lf_grant "${LF[@]}" lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${SPECTRUM_ROLE_ARN}" \
    --resource "{\"Database\":{\"CatalogId\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\",\"Name\":\"${NAMESPACE}\"}}" \
    --permissions DESCRIBE \
    --region "${AWS_REGION}"
lf_grant "${LF[@]}" lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${SPECTRUM_ROLE_ARN}" \
    --resource "{\"Table\":{\"CatalogId\":\"${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}\",\"DatabaseName\":\"${NAMESPACE}\",\"TableWildcard\":{}}}" \
    --permissions SELECT DESCRIBE \
    --region "${AWS_REGION}"
ok "Lake Formation permissions granted"

# 3c. Create the Redshift external schema.
SECRET_NAME=$(stack_output_from "${STACK_NAME}" RedshiftAdminSecretName)
[ -n "${SECRET_NAME}" ] || err "RedshiftAdminSecretName export missing on ${STACK_NAME}"
SECRET_ARN=$(aws secretsmanager describe-secret \
    --secret-id "${SECRET_NAME}" \
    --region "${AWS_REGION}" \
    --query 'ARN' --output text)
[ -n "${SECRET_ARN}" ] || err "Could not resolve admin secret ARN"

# Path through the resource link in the DEFAULT catalog. Redshift's
# CREATE EXTERNAL SCHEMA can resolve the bucket-nested federated
# catalog via a Glue resource link in the default catalog (account-id
# only). This avoids needing CATALOG_ID '<acct>:s3tablescatalog/<bucket>'
# which Redshift can't resolve from CREATE EXTERNAL SCHEMA at the
# child-catalog scope.
RESOURCE_LINK_NAME="${PROJECT_NAME}_iceberg_link"
log "Creating Glue resource link ${RESOURCE_LINK_NAME} pointing at ${NAMESPACE}..."
# Idempotent: AlreadyExistsException is the only outcome we accept.
# Anything else (AccessDenied, malformed JSON, wrong account) aborts.
create_db_rc=0
create_db_out=$(aws glue create-database \
    --region "${AWS_REGION}" \
    --cli-input-json "$(cat <<JSON
{
  "CatalogId": "${ACCOUNT_ID}",
  "DatabaseInput": {
    "Name": "${RESOURCE_LINK_NAME}",
    "TargetDatabase": {
      "CatalogId": "${ACCOUNT_ID}:s3tablescatalog/${BUCKET_NAME}",
      "DatabaseName": "${NAMESPACE}"
    }
  }
}
JSON
)" 2>&1) || create_db_rc=$?
if [ "${create_db_rc}" -ne 0 ]; then
    if printf '%s' "${create_db_out}" | grep -Eqi 'AlreadyExistsException|already exists'; then
        log "Resource link ${RESOURCE_LINK_NAME} already present (ok)"
    else
        err "create-database failed: ${create_db_out}"
    fi
fi

# Grant LF DESCRIBE on the resource link itself. Same loud-failure
# invariant as the catalog/database/table grants above.
log "Granting LF perms on resource link to Spectrum role..."
lf_grant "${LF[@]}" lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${SPECTRUM_ROLE_ARN}" \
    --resource "{\"Database\":{\"CatalogId\":\"${ACCOUNT_ID}\",\"Name\":\"${RESOURCE_LINK_NAME}\"}}" \
    --permissions DESCRIBE \
    --region "${AWS_REGION}"

log "Creating Redshift external schema 'cold'..."
EXT_SCHEMA_SQL=$(cat <<EOF
DROP SCHEMA IF EXISTS cold CASCADE;
CREATE EXTERNAL SCHEMA cold
FROM DATA CATALOG
DATABASE '${RESOURCE_LINK_NAME}'
IAM_ROLE '${SPECTRUM_ROLE_ARN}'
CATALOG_ID '${ACCOUNT_ID}';
EOF
)
redshift_data_run_or_ignore "${EXT_SCHEMA_SQL}" "${SECRET_ARN}" "already exists"

# The Spectrum role also needs Glue perms on the default catalog so
# Redshift can traverse the resource link to the bucket-nested
# s3tablescatalog. Scoped to JUST the resource link database and its
# tables - not catalog-wide - so a copy-paste deploy in a multi-tenant
# account doesn't grant read on every database/table.
log "Attaching DefaultCatalogTraversal policy to Spectrum role..."
aws iam put-role-policy --role-name "$(basename "${SPECTRUM_ROLE_ARN}")" \
    --policy-name DefaultCatalogTraversal \
    --policy-document "$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "TraverseResourceLink",
      "Effect": "Allow",
      "Action": ["glue:GetDatabase","glue:GetDatabases","glue:GetTable","glue:GetTables","glue:GetPartition","glue:GetPartitions"],
      "Resource": [
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:catalog",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:database/${RESOURCE_LINK_NAME}",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:table/${RESOURCE_LINK_NAME}/*"
      ]
    },
    {
      "Sid": "ReadTargetThroughResourceLink",
      "Effect": "Allow",
      "Action": ["glue:GetDatabase","glue:GetDatabases","glue:GetTable","glue:GetTables","glue:GetPartition","glue:GetPartitions"],
      "Resource": [
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:catalog/s3tablescatalog",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:catalog/s3tablescatalog/${BUCKET_NAME}",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:database/s3tablescatalog/${BUCKET_NAME}/${NAMESPACE}",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:table/s3tablescatalog/${BUCKET_NAME}/${NAMESPACE}/*"
      ]
    }
  ]
}
JSON
)" --region "${AWS_REGION}" >/dev/null
ok "DefaultCatalogTraversal policy attached"

# -----------------------------------------------------------------------------
# 4. Apply the unified hot+cold view layer. Defines `cdc_events_all`
# (UNION ALL of `cdc_events` + `cold.cdc_events_archive`) and the
# *_unified current-state views over it. Idempotent: each statement is
# CREATE OR REPLACE VIEW, run via redshift-data batch.
# -----------------------------------------------------------------------------
UNION_SQL_PATH="${SCRIPT_DIR}/../../schema/redshift_iceberg_external.sql"
if [ -f "${UNION_SQL_PATH}" ]; then
    log "Applying unified hot+cold view layer..."
    UNION_BATCH=$(python3 - "${UNION_SQL_PATH}" "${REDSHIFT_WORKGROUP}" "${SECRET_ARN}" "${AWS_REGION}" <<'PY'
import os, sys, subprocess
path, wg, secret, region = sys.argv[1:]
lines = [l for l in open(path).read().splitlines() if not l.strip().startswith('--')]
stmts = [s.strip() for s in '\n'.join(lines).split(';') if s.strip()]
r = subprocess.run(
    ['aws','redshift-data','batch-execute-statement',
     '--workgroup-name',wg,'--database','dev','--secret-arn',secret,
     '--region',region,'--sqls',*stmts,'--query','Id','--output','text'],
    capture_output=True, text=True,
)
sys.stdout.write(r.stdout.strip())
sys.stderr.write(r.stderr)
sys.exit(r.returncode)
PY
)
    [ -n "${UNION_BATCH}" ] || err "Failed to submit unified view SQL"
    log "Unified-view batch ${UNION_BATCH} submitted; polling..."
    UNION_DONE=0
    for _ in $(seq 1 60); do
        UNION_STATUS=$(aws redshift-data describe-statement \
            --id "${UNION_BATCH}" --region "${AWS_REGION}" \
            --query 'Status' --output text)
        case "${UNION_STATUS}" in
            FINISHED) UNION_DONE=1; break ;;
            FAILED|ABORTED)
                aws redshift-data describe-statement --id "${UNION_BATCH}" \
                    --region "${AWS_REGION}" --output json \
                    | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("Error"));[print("  ",s.get("Error"),s.get("QueryString","")[:120]) for s in d.get("SubStatements",[]) if s.get("Status")=="FAILED"]'
                err "Unified view batch ${UNION_STATUS}"
                ;;
        esac
        sleep 2
    done
    # Without this guard the loop falls off the end on a stuck statement
    # and the script reports success even though the views didn't apply.
    [ "${UNION_DONE}" = "1" ] || err "Timed out waiting for unified-view batch ${UNION_BATCH} after 120s"
    ok "Unified hot+cold views applied"
else
    warn "Skipping unified view layer: ${UNION_SQL_PATH} not found"
fi

ok "Iceberg cold path ready. Once Firehose flushes (60s buffer), check:"
echo "    SELECT COUNT(*) FROM cold.cdc_events_archive;"
echo "    SELECT source_store, COUNT(*) FROM cdc_events_all GROUP BY source_store;"
echo "    SELECT * FROM orders_unified LIMIT 10;"
