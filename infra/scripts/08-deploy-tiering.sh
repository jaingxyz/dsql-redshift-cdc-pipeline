#!/usr/bin/env bash
# Deploy the hot/cold tiering automation: Step Functions + EventBridge
# Scheduler that prune cdc_events older than the retention horizon, gated
# on cold.cdc_events_archive having received rows for the same window.
#
# Prerequisites:
#   - Base stack deployed (exports redshift workgroup, admin secret name)
#   - Iceberg cold-path stack deployed AND working (cold.cdc_events_archive
#     external schema must exist; the safety check queries it directly)
#   - Some traffic has flowed through the cold path. If Firehose hasn't
#     ever flushed, the safety check returns 0 on every run and prune
#     never fires.
#
# Idempotent: re-running just updates the stack and tolerates already-
# present resources.
#
# Required tools: aws
# Required env: AWS credentials, base + iceberg stacks present.
#
# Optional env:
#   PROJECT_NAME          must match the base stack       (default: dsql-cdc)
#   AWS_REGION            must match the base stack       (default: us-east-1)
#   TIERING_STACK_NAME    name of the tiering CFN stack   (default: ${PROJECT_NAME}-tiering)
#   TIERING_RETENTION_HOURS  hot retention window         (default: 24 - demo)
#   TIERING_SCHEDULE      EventBridge Scheduler expression (default: rate(1 day))
#   TIERING_SCHEDULE_STATE  ENABLED|DISABLED              (default: DISABLED - manual first)
#   TIERING_ALERT_EMAIL   email to subscribe to alerts    (default: empty)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"
[ -f "${SCRIPT_DIR}/../.env.bootstrap" ] && source "${SCRIPT_DIR}/../.env.bootstrap"

: "${TIERING_STACK_NAME:=${PROJECT_NAME}-tiering}"
: "${TIERING_RETENTION_HOURS:=24}"
: "${TIERING_SCHEDULE:=rate(1 day)}"
: "${TIERING_SCHEDULE_STATE:=DISABLED}"
: "${TIERING_ALERT_EMAIL:=}"
: "${ICEBERG_STACK_NAME:=${PROJECT_NAME}-iceberg}"

require aws
check_aws_creds

# Sanity check: base + iceberg stacks must both exist. The safety check
# also requires the `cold` external schema and the cdc_events_archive
# Iceberg table to be queryable from Redshift - both are created by
# 07-deploy-iceberg.sh's POST-stack steps, not by the iceberg CFN stack
# itself. We validate the stacks here; if the schema/table aren't yet
# applied, the first manual prune execution will fail at SubmitSafetyCheck
# with a clear "relation does not exist" error in the SFN console.
log "Validating prerequisites..."
if ! aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    err "Base stack ${STACK_NAME} not found. Run 01-deploy-cfn.sh first."
fi
if ! aws cloudformation describe-stacks \
        --stack-name "${ICEBERG_STACK_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    err "Iceberg cold-path stack ${ICEBERG_STACK_NAME} not found. Run 07-deploy-iceberg.sh first."
fi
ok "Base + iceberg stacks present"

# Resolve the full Redshift admin secret ARN. The base stack only exports
# the secret NAME (the deterministic "redshift!<namespace>-admin"
# convention) because CFN doesn't surface AdminPasswordSecretArn as a
# GetAtt on the Namespace. Step Functions' executeStatement integration
# requires the FULL ARN including the 6-char random suffix Secrets
# Manager appends - partial-match wildcards aren't accepted there. So we
# resolve it now and pass as a CFN parameter into the tiering stack.
SECRET_NAME=$(stack_output_from "${STACK_NAME}" RedshiftAdminSecretName)
[ -n "${SECRET_NAME}" ] || err "RedshiftAdminSecretName output missing on ${STACK_NAME}"
SECRET_ARN=$(aws secretsmanager describe-secret \
    --secret-id "${SECRET_NAME}" \
    --region "${AWS_REGION}" \
    --query 'ARN' --output text 2>/dev/null || true)
[ -n "${SECRET_ARN}" ] && [ "${SECRET_ARN}" != "None" ] \
    || err "Could not resolve Redshift admin secret ARN. Is the base stack fully deployed?"
log "Redshift admin secret: ${SECRET_ARN}"

# ---------------------------------------------------------------------------
# Deploy the tiering stack. CAPABILITY_NAMED_IAM is required for the
# named roles (state-machine + scheduler), matching the rest of the repo.
# ---------------------------------------------------------------------------
log "Deploying tiering stack ${TIERING_STACK_NAME}..."
log "  RetentionHours:   ${TIERING_RETENTION_HOURS}"
log "  Schedule:         ${TIERING_SCHEDULE}"
log "  ScheduleEnabled:  ${TIERING_SCHEDULE_STATE}"
log "  AlertEmail:       ${TIERING_ALERT_EMAIL:-(none)}"

deploy_args=(
    --stack-name "${TIERING_STACK_NAME}"
    --template-file "${SCRIPT_DIR}/../cloudformation-tiering.yaml"
    --parameter-overrides
        "ProjectName=${PROJECT_NAME}"
        "RedshiftAdminSecretArn=${SECRET_ARN}"
        "RetentionHours=${TIERING_RETENTION_HOURS}"
        "ScheduleExpression=${TIERING_SCHEDULE}"
        "ScheduleEnabled=${TIERING_SCHEDULE_STATE}"
        "AlertEmail=${TIERING_ALERT_EMAIL}"
    --capabilities CAPABILITY_NAMED_IAM
    --region "${AWS_REGION}"
    --no-fail-on-empty-changeset
)
aws cloudformation deploy "${deploy_args[@]}"
ok "Tiering stack deployed"

# ---------------------------------------------------------------------------
# Surface the manual-test command and what the operator should do next.
# Default schedule state is DISABLED so the operator runs one execution
# by hand to confirm the safety check works against live data before
# letting EventBridge fire it on a clock.
# ---------------------------------------------------------------------------
SM_ARN=$(stack_output_from "${TIERING_STACK_NAME}" StateMachineArn)
SCHEDULE_NAME=$(stack_output_from "${TIERING_STACK_NAME}" ScheduleName)
ALERT_TOPIC=$(stack_output_from "${TIERING_STACK_NAME}" AlertTopicArn)

ok "Tiering automation deployed."
echo
echo "  State machine:  ${SM_ARN}"
echo "  Schedule:       ${SCHEDULE_NAME} (${TIERING_SCHEDULE_STATE})"
echo "  Alert topic:    ${ALERT_TOPIC}"
[ -n "${TIERING_ALERT_EMAIL}" ] && echo "  Alert email:    ${TIERING_ALERT_EMAIL} (confirm subscription in your inbox)"
echo
echo "Recommended next step - manual one-shot prune to verify the safety"
echo "check works against live data:"
echo
echo "    aws stepfunctions start-execution \\"
echo "        --state-machine-arn ${SM_ARN} \\"
echo "        --region ${AWS_REGION}"
echo
echo "Watch the execution in the Step Functions console. Expected outcomes:"
echo "  - Cold archive has rows older than ${TIERING_RETENTION_HOURS}h:"
echo "    SafetyCheck -> SubmitDelete -> Vacuum -> Analyze -> Success."
echo "  - Cold archive empty for that window:"
echo "    SafetyCheck -> AbortNoArchive (publishes SNS, ends successfully)."
echo
if [ "${TIERING_SCHEDULE_STATE}" = "DISABLED" ]; then
    echo "When you're satisfied with the manual run, enable the schedule by"
    echo "re-running this script with TIERING_SCHEDULE_STATE=ENABLED:"
    echo
    echo "    TIERING_SCHEDULE_STATE=ENABLED ./infra/scripts/08-deploy-tiering.sh"
    echo
    echo "(Don't use 'aws scheduler update-schedule' for this - it's a"
    echo "full-replace API and would lose Input, RetryPolicy, and timezone."
    echo "The CFN stack reapplies all those fields together.)"
fi
