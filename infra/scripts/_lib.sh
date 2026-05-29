#!/usr/bin/env bash
# Common helpers and configuration for all bootstrap scripts.
# Source this file at the top of any other script:
#   source "$(dirname "$0")/_lib.sh"
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration (override via environment if needed)
# ---------------------------------------------------------------------------
: "${PROJECT_NAME:=dsql-cdc}"
: "${AWS_REGION:=us-east-1}"
: "${STACK_NAME:=${PROJECT_NAME}-stack}"
: "${REDSHIFT_BASE_CAPACITY:=8}"

export PROJECT_NAME AWS_REGION STACK_NAME REDSHIFT_BASE_CAPACITY

# Resolved at runtime by the scripts that need them
DSQL_CLUSTER_ID="${DSQL_CLUSTER_ID:-}"
KINESIS_STREAM_NAME="${KINESIS_STREAM_NAME:-${PROJECT_NAME}-stream}"
KINESIS_STREAM_ARN="${KINESIS_STREAM_ARN:-}"
DSQL_CDC_ROLE_ARN="${DSQL_CDC_ROLE_ARN:-}"
REDSHIFT_WORKGROUP="${REDSHIFT_WORKGROUP:-${PROJECT_NAME}-wg}"
REDSHIFT_DATABASE="${REDSHIFT_DATABASE:-dev}"
LAMBDA_FUNCTION_NAME="${LAMBDA_FUNCTION_NAME:-${PROJECT_NAME}-processor}"

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
log()    { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
ok()     { printf '\033[32m[ OK ]\033[0m %s\n' "$*" >&2; }
warn()   { printf '\033[33m[WARN]\033[0m %s\n' "$*" >&2; }
err()    { printf '\033[31m[ERR ]\033[0m %s\n' "$*" >&2; exit 1; }

# Look up a stack output by key. Errors out if the stack or output is missing.
stack_output() {
    local key="$1"
    aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='${key}'].OutputValue" \
        --output text 2>/dev/null \
        | tr -d '[:space:]'
}

# Require a command to be on PATH.
require() {
    command -v "$1" >/dev/null 2>&1 || err "$1 is not installed (required by this script)"
}

# Sanity check AWS credentials are configured.
check_aws_creds() {
    aws sts get-caller-identity --region "${AWS_REGION}" >/dev/null 2>&1 \
        || err "AWS credentials not configured. Run 'aws configure' or export AWS_PROFILE."
}
