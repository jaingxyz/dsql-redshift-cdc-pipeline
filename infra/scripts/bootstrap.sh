#!/usr/bin/env bash
# One-shot bootstrap: runs every script in order. Safe to re-run.
#
# Required tools: aws, psql, zip
# Required env (or accept defaults):
#   PROJECT_NAME              short prefix for resource names         (default: dsql-cdc)
#   AWS_REGION                AWS region                              (default: us-east-1)
#   STACK_NAME                CloudFormation stack name               (default: ${PROJECT_NAME}-stack)
#   REDSHIFT_BASE_CAPACITY    Redshift Serverless base capacity (RPUs) (default: 8)
#   DSQL_DELETION_PROTECTION  Cluster deletion protection             (default: true)
#
# Usage:
#   ./bootstrap.sh
#   PROJECT_NAME=mycdc AWS_REGION=us-west-2 ./bootstrap.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_lib.sh"

require aws
require psql
require zip
check_aws_creds

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
log "Bootstrapping into account ${ACCOUNT_ID}, region ${AWS_REGION}"
log "Project name: ${PROJECT_NAME}"
log "Stack name:   ${STACK_NAME}"
echo

bash "${SCRIPT_DIR}/01-deploy-cfn.sh"
bash "${SCRIPT_DIR}/02-create-cdc-stream.sh"
bash "${SCRIPT_DIR}/03-load-schemas.sh"
bash "${SCRIPT_DIR}/04-deploy-lambda-code.sh"

echo
ok "Bootstrap complete. Source environment with:"
echo "    source ${SCRIPT_DIR}/../.env.bootstrap"
echo
echo "Drive activity with the order simulator:"
echo "    cd ${SCRIPT_DIR}/../../app"
echo "    python3 order_simulator.py --duration 300 --rate 5"
echo
echo "Query Redshift via the Data API:"
echo "    aws redshift-data execute-statement \\"
echo "        --workgroup-name \${REDSHIFT_WORKGROUP} \\"
echo "        --database \${REDSHIFT_DATABASE} \\"
echo "        --sql \"SELECT COUNT(*) FROM cdc_events\""
