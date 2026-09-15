#!/usr/bin/env bash
# Deploy the Part 3 Iceberg-first stack: a SECOND Redshift Serverless
# namespace + workgroup that reads the EXISTING Iceberg cold archive
# directly - no hot table, no CDC writer, no prune, no UNION views.
#
# Additive and optional. Stands alongside the base dual-write pipeline so
# a reader can follow the series journey OR start straight on Iceberg-first.
# Both workgroups read the same Iceberg table = a clean cost/latency A/B.
#
# Prerequisites:
#   - Base stack deployed (cloudformation.yaml)        -> the Kinesis + DSQL source
#   - Iceberg cold-path deployed (07-deploy-iceberg.sh) -> the Iceberg table,
#     the data-lake query role (${PROJECT_NAME}-iceberg-redshift-role) with
#     its Lake Formation grants, and the Glue resource link this script reuses.
#
# On Redshift Serverless the cold external tables are read by the workgroup's
# integrated data lake query engine, not the dedicated Spectrum server fleet
# (that's the provisioned DC2/RA3 path). The IAM role and grants are identical
# either way, which is why the same role works for both workgroups.
#
# Why so much shorter than 07-deploy-iceberg.sh: that script created the
# role, the LF grants, and the resource link. This one REUSES all of them
# (the role's trust policy is service-principal scoped, so a second
# namespace assumes it with no IAM/LF change), so there is no three-phase
# dance here - just: deploy the stack, create the external schema on the
# new workgroup, verify the read.
#
# Steps:
#   1. CFN deploy: 2nd namespace + workgroup, reusing the imported
#      data-lake query role as attached + default role.
#   2. Create the `cold` external schema on the NEW workgroup, pointing at
#      the SAME Glue resource link the base workgroup uses.
#   3. Verify: count cold.cdc_events_archive and run a current-state dedup
#      straight off Iceberg (no hot table involved).
#
# Idempotent: CFN deploy updates in place; the external schema is created
# with DROP ... IF EXISTS first (matches 07's pattern).
#
# Required tools: aws
# Required env: AWS credentials, base + iceberg stacks already deployed.
#
# Optional env:
#   PROJECT_NAME                  must match the base/iceberg stacks
#   AWS_REGION                    must match the base/iceberg stacks
#   ICEBERG_FIRST_STACK_NAME      ${PROJECT_NAME}-iceberg-first
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"
[ -f "${SCRIPT_DIR}/../.env.bootstrap" ] && source "${SCRIPT_DIR}/../.env.bootstrap"

: "${ICEBERG_STACK_NAME:=${PROJECT_NAME}-iceberg}"
: "${ICEBERG_FIRST_STACK_NAME:=${PROJECT_NAME}-iceberg-first}"

require aws
check_aws_creds

# The iceberg cold-path stack must exist - this stack imports its data-lake
# query role export. Fail early and clearly if it's missing rather than
# letting CFN emit an opaque "No export named ... found" error.
if ! aws cloudformation describe-stacks \
        --stack-name "${ICEBERG_STACK_NAME}" \
        --region "${AWS_REGION}" >/dev/null 2>&1; then
    err "Iceberg cold-path stack '${ICEBERG_STACK_NAME}' not found. Run 07-deploy-iceberg.sh first."
fi

# -----------------------------------------------------------------------------
# 1. Deploy the namespace + workgroup.
# -----------------------------------------------------------------------------
log "Deploying Iceberg-first namespace + workgroup..."
aws cloudformation deploy \
    --stack-name "${ICEBERG_FIRST_STACK_NAME}" \
    --template-file "${SCRIPT_DIR}/../cloudformation-iceberg-first.yaml" \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "RedshiftBaseCapacity=${REDSHIFT_BASE_CAPACITY}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
ok "Iceberg-first stack deployed"

IF_WORKGROUP=$(stack_output_from "${ICEBERG_FIRST_STACK_NAME}" IcebergFirstWorkgroupName)
[ -n "${IF_WORKGROUP}" ] || err "IcebergFirstWorkgroupName output not found"
log "Iceberg-first workgroup: ${IF_WORKGROUP}"

# Wait for the new workgroup to be AVAILABLE before issuing DDL against it.
log "Waiting for workgroup ${IF_WORKGROUP} to become AVAILABLE..."
WG_READY=0
for _ in $(seq 1 40); do
    WG_STATUS=$(aws redshift-serverless get-workgroup \
        --workgroup-name "${IF_WORKGROUP}" \
        --region "${AWS_REGION}" \
        --query 'workgroup.status' --output text 2>/dev/null || echo PENDING)
    if [ "${WG_STATUS}" = "AVAILABLE" ]; then
        WG_READY=1
        break
    fi
    printf '.' >&2
    sleep 10
done
[ "${WG_READY}" = "1" ] || err "Workgroup ${IF_WORKGROUP} not AVAILABLE in ~6.5 min (last: ${WG_STATUS})"
ok "Workgroup AVAILABLE"

# -----------------------------------------------------------------------------
# 2. Create the `cold` external schema on the NEW workgroup, reusing the
# SAME data-lake query role + Glue resource link the base workgroup uses.
# The role already has the LF grants and the resource link already exists
# (both created by 07-deploy-iceberg.sh), so this is the only wiring step.
# -----------------------------------------------------------------------------
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
# The output key is RedshiftSpectrumRoleArn (and the var keeps the legacy
# SPECTRUM name) because that's the existing export the iceberg stack
# publishes - we read it as-is rather than rename a deployed export. The
# role itself is just an IAM role for data-lake reads; on Serverless it
# drives the integrated query engine, not the Spectrum fleet.
DATALAKE_ROLE_ARN=$(stack_output_from "${ICEBERG_STACK_NAME}" RedshiftSpectrumRoleArn)
[ -n "${DATALAKE_ROLE_ARN}" ] || err "RedshiftSpectrumRoleArn export missing on ${ICEBERG_STACK_NAME}"
RESOURCE_LINK_NAME="${PROJECT_NAME}_iceberg_link"

# DDL on the new workgroup runs as admin via its own admin secret.
IF_SECRET_NAME=$(stack_output_from "${ICEBERG_FIRST_STACK_NAME}" IcebergFirstAdminSecretName)
[ -n "${IF_SECRET_NAME}" ] || err "IcebergFirstAdminSecretName export missing"
IF_SECRET_ARN=$(aws secretsmanager describe-secret \
    --secret-id "${IF_SECRET_NAME}" \
    --region "${AWS_REGION}" \
    --query 'ARN' --output text)
[ -n "${IF_SECRET_ARN}" ] || err "Could not resolve Iceberg-first admin secret ARN"

# Run DDL against the NEW workgroup, not the base one. The _lib helpers
# read ${REDSHIFT_WORKGROUP} at call time, so reassigning it here redirects
# the redshift_data_run_* calls below to the new workgroup.
# shellcheck disable=SC2034  # consumed by the sourced _lib.sh helpers, not locally
REDSHIFT_WORKGROUP="${IF_WORKGROUP}"

log "Creating Redshift external schema 'cold' on ${IF_WORKGROUP}..."
EXT_SCHEMA_SQL=$(cat <<EOF
DROP SCHEMA IF EXISTS cold CASCADE;
CREATE EXTERNAL SCHEMA cold
FROM DATA CATALOG
DATABASE '${RESOURCE_LINK_NAME}'
IAM_ROLE '${DATALAKE_ROLE_ARN}'
CATALOG_ID '${ACCOUNT_ID}';
EOF
)
redshift_data_run_or_ignore "${EXT_SCHEMA_SQL}" "${IF_SECRET_ARN}" "already exists"
ok "External schema 'cold' created on Iceberg-first workgroup"

# -----------------------------------------------------------------------------
# 3. Verify the new workgroup reads the EXISTING Iceberg archive end-to-end.
# Two checks: a plain COUNT(*) (catalog + table reachable) and the actual
# current-state dedup-on-read (JSON_PARSE + ROW_NUMBER) the demo ships, so
# "ready" means the headline query itself ran, not just that the table is
# visible.
# -----------------------------------------------------------------------------
log "Verifying Iceberg-first read (count of cold.cdc_events_archive)..."
redshift_data_run_and_wait \
    "SELECT COUNT(*) FROM cold.cdc_events_archive" \
    "${IF_SECRET_ARN}"

log "Verifying current-state dedup-on-read (JSON_PARSE + ROW_NUMBER)..."
redshift_data_run_and_wait \
    "SELECT COUNT(*) FROM (
       SELECT ROW_NUMBER() OVER (PARTITION BY record_id
                                 ORDER BY commit_timestamp DESC) AS rn,
              operation
       FROM cold.cdc_events_archive
       WHERE source_table = 'orders'
     ) WHERE rn = 1 AND operation <> 'd'" \
    "${IF_SECRET_ARN}"
ok "Iceberg-first workgroup reads the cold archive and reconstructs current state."

echo
ok "Iceberg-first stack ready. This workgroup has NO writer and NO scheduled"
echo "    reader, so it suspends to 0 RPU when idle - unlike the base"
echo "    ${PROJECT_NAME}-wg workgroup. Compare them:"
echo
echo "    # current-state orders, straight off Iceberg (no hot table)."
echo "    # Run from the repo root (the path is repo-relative):"
echo "    aws redshift-data execute-statement \\"
echo "        --workgroup-name ${IF_WORKGROUP} \\"
echo "        --database dev --secret-arn ${IF_SECRET_ARN} \\"
echo "        --sql \"\$(cat analytics/iceberg_first_current_state.sql)\""
echo
echo "    # watch ComputeCapacity on both workgroups in CloudWatch - do NOT"
echo "    # query the workgroup to check idleness; that resets the suspend timer."
