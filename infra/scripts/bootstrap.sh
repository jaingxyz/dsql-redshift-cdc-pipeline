#!/usr/bin/env bash
# One-shot bootstrap: runs every script in order. Safe to re-run.
#
# Always deploys the base pipeline (DSQL → Kinesis → Lambda → Redshift).
# Two optional add-on stacks are offered interactively after the base
# pipeline succeeds:
#   - Always-on order simulator (Fargate)        — for continuous traffic
#   - SageMaker access role + Redshift grants    — for notebook analytics
#
# Required tools: aws, psql, zip
# Required env (or accept defaults):
#   PROJECT_NAME              short prefix for resource names         (default: dsql-cdc)
#   AWS_REGION                AWS region                              (default: us-east-1)
#   STACK_NAME                CloudFormation stack name               (default: ${PROJECT_NAME}-stack)
#   REDSHIFT_BASE_CAPACITY    Redshift Serverless base capacity (RPUs) (default: 8)
#   DSQL_DELETION_PROTECTION  Cluster deletion protection             (default: true)
#
# Optional add-ons (override the prompts for non-interactive runs):
#   DEPLOY_SIMULATOR=1    skip the prompt and deploy the simulator stack
#   DEPLOY_SIMULATOR=0    skip the prompt and DON'T deploy
#   DEPLOY_SAGEMAKER=1    skip the prompt and deploy the SageMaker stack
#   DEPLOY_SAGEMAKER=0    skip the prompt and DON'T deploy
#   DEPLOY_TIERING=1      skip the prompt and deploy the tiering stack
#   DEPLOY_TIERING=0      skip the prompt and DON'T deploy
#
# Usage:
#   ./bootstrap.sh
#   PROJECT_NAME=mycdc AWS_REGION=us-west-2 ./bootstrap.sh
#   DEPLOY_SAGEMAKER=1 ./bootstrap.sh    # CI: yes to SageMaker, no prompt
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

# -------- Base pipeline (required) --------
bash "${SCRIPT_DIR}/01-deploy-cfn.sh"
bash "${SCRIPT_DIR}/02-create-cdc-stream.sh"
bash "${SCRIPT_DIR}/03-load-schemas.sh"
bash "${SCRIPT_DIR}/04-deploy-lambda-code.sh"

ok "Base pipeline deployed."
echo

# -------- Optional add-ons --------
# Each prompt has an env-var override (DEPLOY_SIMULATOR / DEPLOY_SAGEMAKER)
# so CI runs and re-runs can be deterministic. See `confirm` in _lib.sh.
echo
echo "Optional add-ons:"
echo "  - Simulator stack:  always-on Fargate task driving ~1 order/sec."
echo "                      Adds ~\$80-200/mo (mostly Redshift RPU-hours)."
echo "  - SageMaker stack:  IAM role + Redshift GRANTs for notebook access."
echo "                      Free; just IAM. Run later with 06-deploy-sagemaker.sh."
echo "  - Tiering stack:    Step Functions + EventBridge prune of cdc_events"
echo "                      older than 24h. Requires the Iceberg cold path"
echo "                      (07-deploy-iceberg.sh). Schedule deploys DISABLED;"
echo "                      operator triggers a manual run first to verify."
echo
# Add-ons are intentionally NOT failure-fatal: the base pipeline is
# already up by this point, and we want the user to see the success
# summary even if a transient issue (eventual consistency on a
# just-created secret, IAM propagation) trips up the add-on.
if confirm "Deploy the always-on simulator stack now?" DEPLOY_SIMULATOR; then
    bash "${SCRIPT_DIR}/05-deploy-simulator.sh" \
        || warn "05-deploy-simulator.sh failed; base pipeline is unaffected. Re-run the script when ready."
fi

if confirm "Deploy the SageMaker access stack now?" DEPLOY_SAGEMAKER; then
    bash "${SCRIPT_DIR}/06-deploy-sagemaker.sh" \
        || warn "06-deploy-sagemaker.sh failed; base pipeline is unaffected. Re-run the script when ready."
fi

if confirm "Deploy the tiering automation stack now? (requires Iceberg cold path)" DEPLOY_TIERING; then
    bash "${SCRIPT_DIR}/08-deploy-tiering.sh" \
        || warn "08-deploy-tiering.sh failed; base pipeline is unaffected. Re-run the script when ready."
fi

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
