#!/usr/bin/env bash
# Deploy the CloudFormation stack: DSQL cluster, Kinesis, IAM, Redshift Serverless,
# and the Lambda function (with placeholder code).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_lib.sh"

require aws
check_aws_creds

TEMPLATE="${SCRIPT_DIR}/../cloudformation.yaml"
[ -f "${TEMPLATE}" ] || err "Template not found at ${TEMPLATE}"

log "Deploying stack ${STACK_NAME} from ${TEMPLATE}..."
log "(This takes 3-5 minutes - DSQL cluster creation is the longest step.)"

aws cloudformation deploy \
    --template-file "${TEMPLATE}" \
    --stack-name "${STACK_NAME}" \
    --region "${AWS_REGION}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        ProjectName="${PROJECT_NAME}" \
        RedshiftBaseCapacity="${REDSHIFT_BASE_CAPACITY}" \
        DsqlDeletionProtection="${DSQL_DELETION_PROTECTION:-true}" \
    --no-fail-on-empty-changeset

ok "Stack ${STACK_NAME} deployed"

# Capture outputs into the env file so downstream scripts can read them
ENV_FILE="${SCRIPT_DIR}/../.env.bootstrap"
{
    echo "export AWS_REGION=${AWS_REGION}"
    echo "export PROJECT_NAME=${PROJECT_NAME}"
    echo "export STACK_NAME=${STACK_NAME}"
    echo "export DSQL_CLUSTER_ID=$(stack_output DsqlClusterIdentifier)"
    echo "export DSQL_CLUSTER_ARN=$(stack_output DsqlClusterArn)"
    echo "export DSQL_CLUSTER_ENDPOINT=$(stack_output DsqlClusterEndpoint)"
    echo "export KINESIS_STREAM_NAME=$(stack_output KinesisStreamName)"
    echo "export KINESIS_STREAM_ARN=$(stack_output KinesisStreamArn)"
    echo "export DSQL_CDC_ROLE_ARN=$(stack_output DsqlCdcRoleArn)"
    echo "export REDSHIFT_WORKGROUP=$(stack_output RedshiftWorkgroupName)"
    echo "export REDSHIFT_DATABASE=$(stack_output RedshiftDatabase)"
    echo "export LAMBDA_FUNCTION_NAME=$(stack_output LambdaFunctionName)"
} > "${ENV_FILE}"

ok "Stack outputs written to ${ENV_FILE}"
