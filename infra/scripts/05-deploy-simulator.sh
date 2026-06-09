#!/usr/bin/env bash
# Deploy the simulator infrastructure (ECR + VPC + Fargate cluster + service +
# Budget + GitHub OIDC role). The container IMAGE itself is NOT built here -
# that's done by .github/workflows/build-simulator.yml on push to main.
#
# Why split the build out? Building containers on a developer laptop creates
# a hidden dependency on Docker Desktop being installed and running. The
# CFN-managed OIDC role lets GitHub Actions push to ECR with no shared
# secrets, and the ECS service auto-redeploys on each new image. Net effect:
# this script provisions, GHA does the rest.
#
# Idempotent: re-running just updates the CFN stack.
#
# Required tools: aws
# Required env: AWS credentials configured for the same account where
#   01-deploy-cfn.sh deployed the base stack.
#
# Optional env (defaults match cloudformation-simulator.yaml):
#   PROJECT_NAME              must match the base stack's project name
#   AWS_REGION                must match the base stack's region
#   SIMULATOR_STACK_NAME      ${PROJECT_NAME}-simulator
#   IMAGE_REPO_NAME           dsql-cdc-simulator
#   IMAGE_TAG                 latest
#   TARGET_RATE               1     (orders/sec; > 1 raises Redshift bill)
#   MONTHLY_BUDGET_USD        200
#   GITHUB_REPO               jaingxyz/dsql-redshift-cdc-pipeline
#                             (must match the repo running the workflow)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

: "${SIMULATOR_STACK_NAME:=${PROJECT_NAME}-simulator}"
: "${IMAGE_REPO_NAME:=dsql-cdc-simulator}"
: "${IMAGE_TAG:=latest}"
: "${TARGET_RATE:=1}"
: "${MONTHLY_BUDGET_USD:=200}"
: "${GITHUB_REPO:=jaingxyz/dsql-redshift-cdc-pipeline}"

require aws
check_aws_creds

# -----------------------------------------------------------------------------
# Deploy / update the simulator infrastructure stack.
# Until the GHA workflow pushes its first image, the ECS service will fail to
# pull (the task references :latest in an empty ECR repo) and Fargate will
# keep retrying. That's expected and self-resolves once the workflow runs.
# -----------------------------------------------------------------------------
log "Deploying CloudFormation stack ${SIMULATOR_STACK_NAME}..."
aws cloudformation deploy \
    --stack-name "${SIMULATOR_STACK_NAME}" \
    --template-file "${SCRIPT_DIR}/../cloudformation-simulator.yaml" \
    --parameter-overrides \
        "ProjectName=${PROJECT_NAME}" \
        "ImageRepositoryName=${IMAGE_REPO_NAME}" \
        "ImageTag=${IMAGE_TAG}" \
        "TargetRate=${TARGET_RATE}" \
        "MonthlyBudgetUsd=${MONTHLY_BUDGET_USD}" \
        "GitHubRepo=${GITHUB_REPO}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
ok "Simulator stack deployed"

CLUSTER_NAME=$(stack_output_from "${SIMULATOR_STACK_NAME}" EcsClusterName)
SERVICE_NAME=$(stack_output_from "${SIMULATOR_STACK_NAME}" ServiceName)
LOG_GROUP=$(stack_output_from "${SIMULATOR_STACK_NAME}" LogGroupName)
ROLE_ARN=$(stack_output_from "${SIMULATOR_STACK_NAME}" GitHubDeployRoleArn)

cat <<EOF

Simulator infra is up. The container image is built by GitHub Actions:

  Workflow: .github/workflows/build-simulator.yml
  Trigger:  push to main touching app/Dockerfile or app/order_simulator.py,
            or via the Actions tab "Run workflow" button.
  Auth:     short-lived OIDC token assuming ${ROLE_ARN}

To kick off the first build, commit + push these files (or click
"Run workflow" once the workflow file exists on main).

Watch the simulator's stdout once the image is up:
  aws logs tail "${LOG_GROUP}" --follow --region "${AWS_REGION}"

Pause the simulator (without removing infrastructure):
  aws ecs update-service --cluster "${CLUSTER_NAME}" \\
    --service "${SERVICE_NAME}" --desired-count 0 --region "${AWS_REGION}"

Tear down the simulator (ECR + VPC + ECS + Budget; keeps base stack):
  aws cloudformation delete-stack \\
    --stack-name "${SIMULATOR_STACK_NAME}" --region "${AWS_REGION}"

Cost note: with TargetRate=${TARGET_RATE}, expect ~\$80-200/mo dominated
by Redshift Serverless RPU-hours. Monthly budget is \$${MONTHLY_BUDGET_USD}
(view the AWS Budgets console for current spend).
EOF
