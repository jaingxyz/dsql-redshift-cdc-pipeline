#!/usr/bin/env bash
# Tear down everything created by bootstrap.sh, in reverse order.
#
# Order: optional add-on stacks (SageMaker, simulator) -> DSQL CDC stream
# -> base CloudFormation stack (which removes the DSQL cluster, Kinesis,
# Redshift, Lambda, and IAM roles) -> local env file.
#
# Add-on stacks are deleted BEFORE the base stack because the simulator
# stack imports values via Fn::ImportValue from the base — CloudFormation
# refuses to delete an export that's still in use by another stack.
#
# Safety: this script DELETES resources. It prompts before each destructive
# action unless YES=1 is set in the environment (for CI / scripted use).
#
# Note on DSQL deletion protection: by default the CFN template sets
# DeletionProtectionEnabled=true on the cluster. CloudFormation cannot
# delete a protected cluster, so this script will disable protection
# (with explicit confirmation) before requesting stack deletion.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_lib.sh"
[ -f "${SCRIPT_DIR}/../.env.bootstrap" ] && source "${SCRIPT_DIR}/../.env.bootstrap"

require aws
check_aws_creds

confirm() {
    [ "${YES:-0}" = "1" ] && return 0
    read -r -p "$1 [y/N] " resp
    [[ "${resp}" =~ ^[Yy]$ ]]
}

# Helper: delete a CFN stack if it exists, prompt first, wait for completion.
delete_stack_if_exists() {
    local stack="$1"
    local label="$2"
    if ! aws cloudformation describe-stacks \
            --stack-name "${stack}" --region "${AWS_REGION}" \
            >/dev/null 2>&1; then
        return 0
    fi
    if confirm "Delete ${label} stack ${stack}?"; then
        aws cloudformation delete-stack \
            --stack-name "${stack}" --region "${AWS_REGION}"
        log "Waiting for ${stack} deletion..."
        aws cloudformation wait stack-delete-complete \
            --stack-name "${stack}" --region "${AWS_REGION}" \
            || warn "${stack} delete wait failed (check console)"
        ok "${label} stack deleted"
    else
        warn "Keeping ${label} stack. Base-stack delete will fail if it"
        warn "still imports values from this stack."
    fi
}

# 0. Optional add-on stacks first — they import from the base stack
# and have to go before its exports can be removed.
delete_stack_if_exists "${PROJECT_NAME}-sagemaker" "SageMaker access"
delete_stack_if_exists "${PROJECT_NAME}-simulator" "always-on simulator"

# 1. Delete the DSQL CDC stream
if [ -n "${DSQL_STREAM_ID:-}" ] && [ -n "${DSQL_CLUSTER_ID:-}" ]; then
    if confirm "Delete DSQL CDC stream ${DSQL_STREAM_ID}?"; then
        aws dsql delete-stream \
            --cluster-identifier "${DSQL_CLUSTER_ID}" \
            --stream-identifier "${DSQL_STREAM_ID}" \
            --region "${AWS_REGION}" || warn "Stream delete failed (may already be gone)"
        ok "CDC stream deletion requested"
    fi
fi

# 2. Disable deletion protection on the DSQL cluster (CFN can't delete protected resources)
if [ -n "${DSQL_CLUSTER_ID:-}" ]; then
    warn "About to delete the DSQL cluster ${DSQL_CLUSTER_ID} via CloudFormation."
    warn "This will PERMANENTLY DESTROY all data in the cluster."
    if confirm "Disable deletion protection on cluster ${DSQL_CLUSTER_ID}?"; then
        # Distinguish "No updates" (benign no-op when deletion protection
        # was already false) from real failures. If we don't, the
        # stack-update-complete waiter below polls up to an hour for an
        # UPDATE_COMPLETE state that will never arrive.
        update_stderr=$(aws cloudformation update-stack \
            --stack-name "${STACK_NAME}" \
            --use-previous-template \
            --region "${AWS_REGION}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --parameters \
                ParameterKey=ProjectName,UsePreviousValue=true \
                ParameterKey=RedshiftBaseCapacity,UsePreviousValue=true \
                ParameterKey=DsqlDeletionProtection,ParameterValue=false \
            2>&1 >/dev/null) && update_rc=0 || update_rc=$?
        if [ "${update_rc}" = "0" ]; then
            log "Waiting for stack update..."
            aws cloudformation wait stack-update-complete \
                --stack-name "${STACK_NAME}" \
                --region "${AWS_REGION}"
            ok "Deletion protection disabled"
        elif echo "${update_stderr}" | grep -qi "No updates are to be performed"; then
            ok "Deletion protection already disabled (no stack update needed)"
        else
            err "update-stack failed: ${update_stderr}"
        fi
    else
        warn "Skipping. CloudFormation delete-stack will fail on the protected cluster."
        exit 1
    fi
fi

# 3. Delete the CloudFormation stack (removes DSQL cluster, Kinesis, Redshift, Lambda, IAM)
# The local env file is removed only if the stack delete succeeds; otherwise
# we keep it so a re-run still has DSQL_STREAM_ID, DSQL_CLUSTER_ID, STACK_NAME
# to act on.
if confirm "Delete CloudFormation stack ${STACK_NAME}?"; then
    aws cloudformation delete-stack \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}"
    log "Waiting for stack deletion (this can take 5-10 minutes)..."
    aws cloudformation wait stack-delete-complete \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" || warn "Stack delete wait failed (check console)"
    ok "Stack deleted"

    if [ -f "${SCRIPT_DIR}/../.env.bootstrap" ]; then
        rm -f "${SCRIPT_DIR}/../.env.bootstrap"
        ok "Removed local .env.bootstrap"
    fi
fi

ok "Teardown complete"
