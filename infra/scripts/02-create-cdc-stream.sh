#!/usr/bin/env bash
# Create the Aurora DSQL CDC stream that links the cluster to the Kinesis stream.
# DSQL CDC is in public preview and does not yet have a CloudFormation
# resource type, so we create it via the AWS CLI here.
# Idempotent: reuses an existing CDC stream on the cluster if one exists.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_lib.sh"
[ -f "${SCRIPT_DIR}/../.env.bootstrap" ] && source "${SCRIPT_DIR}/../.env.bootstrap"

require aws

[ -n "${DSQL_CLUSTER_ID:-}" ]    || err "DSQL_CLUSTER_ID not set. Run 01-deploy-cfn.sh first."
[ -n "${KINESIS_STREAM_ARN:-}" ] || err "KINESIS_STREAM_ARN not set. Run 01-deploy-cfn.sh first."
[ -n "${DSQL_CDC_ROLE_ARN:-}" ]  || err "DSQL_CDC_ROLE_ARN not set. Run 01-deploy-cfn.sh first."

# Reuse existing CDC stream if one is already attached to the cluster
existing=$(aws dsql list-streams \
    --cluster-identifier "${DSQL_CLUSTER_ID}" \
    --region "${AWS_REGION}" \
    --query 'streams[0].streamIdentifier' \
    --output text 2>/dev/null || true)

if [ -n "${existing}" ] && [ "${existing}" != "None" ]; then
    DSQL_STREAM_ID="${existing}"
    log "Reusing existing CDC stream: ${DSQL_STREAM_ID}"
else
    log "Creating CDC stream linking cluster ${DSQL_CLUSTER_ID} to Kinesis ${KINESIS_STREAM_NAME}..."
    target_def=$(printf '{"kinesis":{"streamArn":"%s","roleArn":"%s"}}' \
        "${KINESIS_STREAM_ARN}" "${DSQL_CDC_ROLE_ARN}")

    DSQL_STREAM_ID=$(aws dsql create-stream \
        --cluster-identifier "${DSQL_CLUSTER_ID}" \
        --target-definition "${target_def}" \
        --ordering UNORDERED \
        --format JSON \
        --region "${AWS_REGION}" \
        --query 'streamIdentifier' \
        --output text)
    ok "Created CDC stream ${DSQL_STREAM_ID}"
fi

# Wait for ACTIVE
log "Waiting for CDC stream ${DSQL_STREAM_ID} to become ACTIVE..."
for i in $(seq 1 60); do
    status=$(aws dsql get-stream \
        --cluster-identifier "${DSQL_CLUSTER_ID}" \
        --stream-identifier "${DSQL_STREAM_ID}" \
        --region "${AWS_REGION}" \
        --query 'status' --output text)
    case "${status}" in
        ACTIVE) ok "CDC stream is ACTIVE"; break ;;
        FAILED|DELETING) err "CDC stream entered ${status} state" ;;
        *) printf '.' >&2; sleep 5 ;;
    esac
    [ "$i" -eq 60 ] && err "Timed out waiting for CDC stream"
done

echo "export DSQL_STREAM_ID=${DSQL_STREAM_ID}" >> "${SCRIPT_DIR}/../.env.bootstrap"
