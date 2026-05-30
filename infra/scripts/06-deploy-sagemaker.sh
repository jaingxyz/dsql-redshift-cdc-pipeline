#!/usr/bin/env bash
# Deploy the SageMaker access stack and run the one-time GRANTs that the
# federated-user auth path needs.
#
# What the stack creates: an IAM role (or augments an existing one) with
# Secrets Manager read on the admin secret + redshift-serverless:GetCredentials
# on the workgroup + redshift-data:* actions.
#
# What this script does on top of "aws cloudformation deploy":
#   1. Looks up the admin secret ARN (CFN doesn't surface it as an attribute)
#   2. Runs CREATE USER + GRANT + ALTER DEFAULT PRIVILEGES via the Data API
#      using the admin secret, so the federated-creds path works without a
#      manual follow-up step.
#
# Idempotent: re-running just updates the CFN stack; the GRANT statements
# are wrapped in DO blocks so they survive a re-run.
#
# Required tools: aws
# Required env: AWS credentials, base stack already deployed.
#
# Optional env:
#   PROJECT_NAME              must match the base stack
#   AWS_REGION                must match the base stack
#   SAGEMAKER_STACK_NAME      ${PROJECT_NAME}-sagemaker
#   EXISTING_ROLE_NAME        empty = create a new role; set to attach
#                             the policy to a role you already own
#   SKIP_GRANTS               1 = stack-only deploy, no DB-level grants
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"
[ -f "${SCRIPT_DIR}/../.env.bootstrap" ] && source "${SCRIPT_DIR}/../.env.bootstrap"

: "${SAGEMAKER_STACK_NAME:=${PROJECT_NAME}-sagemaker}"
: "${EXISTING_ROLE_NAME:=}"
: "${SKIP_GRANTS:=0}"

require aws
check_aws_creds

[ -n "${REDSHIFT_WORKGROUP:-}" ] || err "REDSHIFT_WORKGROUP not set. Run 01-deploy-cfn.sh first."
[ -n "${REDSHIFT_DATABASE:-}" ]  || err "REDSHIFT_DATABASE not set. Run 01-deploy-cfn.sh first."

# -----------------------------------------------------------------------------
# Deploy the CFN stack.
# -----------------------------------------------------------------------------
log "Deploying CloudFormation stack ${SAGEMAKER_STACK_NAME}..."
aws cloudformation deploy \
    --stack-name "${SAGEMAKER_STACK_NAME}" \
    --template-file "${SCRIPT_DIR}/../cloudformation-sagemaker.yaml" \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "ExistingRoleName=${EXISTING_ROLE_NAME}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
ok "SageMaker access stack deployed"

ROLE_ARN=$(stack_output_from "${SAGEMAKER_STACK_NAME}" ExecutionRoleArn)
[ -n "${ROLE_ARN}" ] \
    || err "ExecutionRoleArn output not found on stack ${SAGEMAKER_STACK_NAME}. Did the deploy actually finish?"
ROLE_NAME="${ROLE_ARN##*/}"
log "Execution role: ${ROLE_ARN}"

if [ "${SKIP_GRANTS}" = "1" ]; then
    warn "SKIP_GRANTS=1: not running DB-level GRANTs."
    warn "If you'll connect via Temporary credentials (federated user),"
    warn "you'll need to run them manually as admin:"
    cat <<EOF
    CREATE USER "IAMR:${ROLE_NAME}" WITH PASSWORD DISABLE;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO "IAMR:${ROLE_NAME}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT ON TABLES TO "IAMR:${ROLE_NAME}";
EOF
    exit 0
fi

# -----------------------------------------------------------------------------
# One-time GRANTs against Redshift, scoped to the role's federated user.
# We use the admin secret to authorize the statement. The base stack
# exports the deterministic secret name; we resolve it here and turn
# it into the ARN that secretsmanager / redshift-data expect.
# -----------------------------------------------------------------------------
SECRET_NAME=$(stack_output_from "${STACK_NAME}" RedshiftAdminSecretName)
[ -n "${SECRET_NAME}" ] \
    || err "RedshiftAdminSecretName export missing on ${STACK_NAME}. Re-deploy the base stack to publish it."
log "Looking up admin secret ARN for '${SECRET_NAME}'..."
SECRET_LOOKUP=$(aws secretsmanager describe-secret \
    --secret-id "${SECRET_NAME}" \
    --region "${AWS_REGION}" \
    --query 'ARN' --output text 2>&1) \
    || err "Could not find admin secret '${SECRET_NAME}'. AWS said: ${SECRET_LOOKUP}"
SECRET_ARN="${SECRET_LOOKUP}"

DB_USER="IAMR:${ROLE_NAME}"

# CREATE USER + GRANT in one round-trip. Idempotent: re-running tolerates
# the "already exists" error from CREATE USER. GRANT/ALTER are themselves
# idempotent at the SQL level.
log "Creating federated user (if not present) and granting SELECT..."
CREATE_SQL="CREATE USER \"${DB_USER}\" WITH PASSWORD DISABLE"
redshift_data_run_or_ignore "${CREATE_SQL}" "${SECRET_ARN}" "already exists"
ok "Federated user ${DB_USER} ready"

GRANT_SQL="GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"${DB_USER}\";
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO \"${DB_USER}\";"
redshift_data_run_and_wait "${GRANT_SQL}" "${SECRET_ARN}"
ok "GRANT SELECT applied to ${DB_USER}"

cat <<EOF

SageMaker access ready.

  Execution role:   ${ROLE_ARN}
  DB user (fed):    ${DB_USER}
  Admin secret:     ${SECRET_NAME}

When creating a SageMaker domain or notebook, use the role above. To
connect from a notebook, see the README "Querying from SageMaker
Studio" section for the three auth patterns (Secrets Manager,
GetCredentials, Data API).

Tear down (keeps base stack and pipeline intact):
  aws cloudformation delete-stack \\
    --stack-name "${SAGEMAKER_STACK_NAME}" --region "${AWS_REGION}"
EOF
