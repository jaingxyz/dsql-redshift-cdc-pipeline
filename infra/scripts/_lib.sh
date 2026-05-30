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

# Same as stack_output but takes the stack name as the first arg. Used by
# scripts that touch multiple stacks (e.g. 05-deploy-simulator.sh reads
# from the simulator stack while STACK_NAME points at the base stack).
stack_output_from() {
    local stack="$1"
    local key="$2"
    aws cloudformation describe-stacks \
        --stack-name "${stack}" \
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

# Submit a SQL statement via the Redshift Data API and block until it
# finishes. Args: $1=sql, $2=optional secret ARN (for admin operations).
# Without the secret, the call uses the caller's IAM identity (federated
# user) — typically only suitable for read paths the caller has GRANTs on.
redshift_data_run_and_wait() {
    local sql="$1"
    local secret_arn="${2:-}"
    local args=(
        --workgroup-name "${REDSHIFT_WORKGROUP}"
        --database "${REDSHIFT_DATABASE}"
        --region "${AWS_REGION}"
        --sql "${sql}"
    )
    [ -n "${secret_arn}" ] && args+=(--secret-arn "${secret_arn}")

    local sid
    sid=$(aws redshift-data execute-statement "${args[@]}" --query 'Id' --output text)
    log "Submitted statement ${sid}"
    for _ in $(seq 1 60); do
        local status
        status=$(aws redshift-data describe-statement \
            --id "${sid}" --region "${AWS_REGION}" \
            --query 'Status' --output text)
        # AWS CLI's --output text returns the literal string "None" when
        # a queried field is null (eventual-consistency window right
        # after submit). Treat it the same as in-progress.
        case "${status}" in
            FINISHED) return 0 ;;
            FAILED|ABORTED)
                local reason
                reason=$(aws redshift-data describe-statement \
                    --id "${sid}" --region "${AWS_REGION}" \
                    --query 'Error' --output text)
                err "Redshift statement ${status}: ${reason}"
                ;;
            SUBMITTED|PICKED|STARTED|None|"") printf '.' >&2; sleep 3 ;;
            *) warn "Unexpected statement status '${status}'; continuing to poll"
               sleep 3 ;;
        esac
    done
    err "Timed out waiting for statement ${sid}"
}

# Like redshift_data_run_and_wait, but tolerates statements that fail
# with an Error message matching a regex. Returns 0 on FINISHED *or*
# ignored failure; calls err() on any other failure or timeout. Use for
# idempotent operations like CREATE USER where re-running is normal.
# Args: $1=sql, $2=secret ARN, $3=ignore-error regex (case-insensitive).
redshift_data_run_or_ignore() {
    local sql="$1"
    local secret_arn="$2"
    local ignore_re="$3"
    local sid
    sid=$(aws redshift-data execute-statement \
        --workgroup-name "${REDSHIFT_WORKGROUP}" \
        --database "${REDSHIFT_DATABASE}" \
        --region "${AWS_REGION}" \
        --secret-arn "${secret_arn}" \
        --sql "${sql}" \
        --query 'Id' --output text)
    log "Submitted statement ${sid}"
    for _ in $(seq 1 60); do
        local status
        status=$(aws redshift-data describe-statement \
            --id "${sid}" --region "${AWS_REGION}" \
            --query 'Status' --output text)
        case "${status}" in
            FINISHED) return 0 ;;
            FAILED|ABORTED)
                local reason
                reason=$(aws redshift-data describe-statement \
                    --id "${sid}" --region "${AWS_REGION}" \
                    --query 'Error' --output text)
                if printf '%s' "${reason}" | grep -Eqi -- "${ignore_re}"; then
                    log "Ignored expected error: ${reason}"
                    return 0
                fi
                err "Redshift statement ${status}: ${reason}"
                ;;
            SUBMITTED|PICKED|STARTED|None|"") printf '.' >&2; sleep 3 ;;
            *) warn "Unexpected statement status '${status}'; continuing to poll"
               sleep 3 ;;
        esac
    done
    err "Timed out waiting for statement ${sid}"
}

# Yes/no prompt for interactive bootstrap flows. Usage:
#   if confirm "Deploy SageMaker access stack?" DEPLOY_SAGEMAKER; then ... fi
# - First arg: prompt text.
# - Second arg: env var name. If set to 0/no/false, skip prompt and return
#   non-zero. If set to 1/yes/true, skip prompt and return zero. Else ask.
# Default (no answer / non-interactive shell): no.
confirm() {
    local prompt="$1"
    local var="${2:-}"
    if [ -n "${var}" ] && [ -n "${!var:-}" ]; then
        case "${!var}" in
            1|yes|true|YES|TRUE|Y|y) return 0 ;;
            *) return 1 ;;
        esac
    fi
    if [ ! -t 0 ]; then
        # Non-interactive (CI, piped input). Default no; the env var
        # is the override knob.
        return 1
    fi
    local reply
    read -r -p "${prompt} [y/N] " reply
    case "${reply}" in
        y|Y|yes|YES) return 0 ;;
        *) return 1 ;;
    esac
}
