#!/usr/bin/env bash
# Deploy the always-on order simulator stack:
#   1. Deploy cloudformation-simulator.yaml to provision ECR + VPC + ECS
#   2. Build the simulator container image (linux/arm64)
#   3. Push it to the just-created ECR repo
#   4. Force a new ECS deployment so the service picks up the image
#
# Idempotent: re-running rebuilds + redeploys; the service rolls forward.
#
# Required tools: aws, docker (with buildx)
# Required env: AWS credentials configured for the same account where
#   01-deploy-cfn.sh deployed the base stack.
#
# Optional env (defaults match cloudformation-simulator.yaml):
#   PROJECT_NAME              must match the base stack's project name
#   AWS_REGION                must match the base stack's region
#   SIMULATOR_STACK_NAME      ${PROJECT_NAME}-simulator
#   IMAGE_REPO_NAME           dsql-cdc-simulator
#   IMAGE_TAG                 latest (override for blue/green)
#   TARGET_RATE               1     (orders/sec; > 1 raises Redshift bill)
#   MONTHLY_BUDGET_USD        200
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/_lib.sh"

: "${SIMULATOR_STACK_NAME:=${PROJECT_NAME}-simulator}"
: "${IMAGE_REPO_NAME:=dsql-cdc-simulator}"
: "${IMAGE_TAG:=latest}"
: "${TARGET_RATE:=1}"
: "${MONTHLY_BUDGET_USD:=200}"

require aws
require docker
check_aws_creds

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URI="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${IMAGE_REPO_NAME}"

# -----------------------------------------------------------------------------
# 1. Deploy / update the simulator stack.
# We deploy BEFORE building the image because the stack creates the ECR repo.
# The ECS task references ${IMAGE_TAG}; until we push, the service will fail
# to pull and ECS will keep retrying — that's fine, the next steps fix it.
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
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${AWS_REGION}" \
    --no-fail-on-empty-changeset
ok "Simulator stack deployed"

# -----------------------------------------------------------------------------
# 2. Build the container image for arm64 (Graviton Fargate).
# -----------------------------------------------------------------------------
APP_DIR="$(cd "${SCRIPT_DIR}/../../app" && pwd)"
log "Building simulator image at ${APP_DIR} for linux/arm64..."

# Buildx setup is idempotent; this creates the builder if it doesn't exist.
docker buildx inspect dsql-cdc-builder >/dev/null 2>&1 \
    || docker buildx create --name dsql-cdc-builder --use >/dev/null

# -----------------------------------------------------------------------------
# 3. Authenticate Docker against ECR and push the image.
# -----------------------------------------------------------------------------
log "Logging Docker into ECR..."
aws ecr get-login-password --region "${AWS_REGION}" \
    | docker login --username AWS --password-stdin \
        "${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ok "ECR login OK"

log "Building and pushing ${ECR_URI}:${IMAGE_TAG}..."
docker buildx build \
    --platform linux/arm64 \
    --tag "${ECR_URI}:${IMAGE_TAG}" \
    --push \
    "${APP_DIR}"
ok "Image pushed"

# -----------------------------------------------------------------------------
# 4. Force the ECS service to pick up the new image.
# Without this, ECS would only redeploy on a task-definition change. Since we
# reused the :latest tag, the task definition is unchanged but the image
# behind it isn't — force-new-deployment makes ECS pull again.
# -----------------------------------------------------------------------------
CLUSTER_NAME=$(stack_output_from "${SIMULATOR_STACK_NAME}" EcsClusterName)
SERVICE_NAME=$(stack_output_from "${SIMULATOR_STACK_NAME}" ServiceName)
LOG_GROUP=$(stack_output_from "${SIMULATOR_STACK_NAME}" LogGroupName)

log "Forcing new ECS deployment on ${CLUSTER_NAME}/${SERVICE_NAME}..."
aws ecs update-service \
    --cluster "${CLUSTER_NAME}" \
    --service "${SERVICE_NAME}" \
    --force-new-deployment \
    --region "${AWS_REGION}" \
    --no-cli-pager \
    --query 'service.{status: status, desiredCount: desiredCount, runningCount: runningCount}' \
    --output table
ok "ECS deployment triggered"

cat <<EOF

Simulator deploy done.

Watch the simulator's stdout (it logs throughput every 10s):
  aws logs tail "${LOG_GROUP}" --follow --region "${AWS_REGION}"

Stop the simulator (without removing infrastructure):
  aws ecs update-service --cluster "${CLUSTER_NAME}" \\
    --service "${SERVICE_NAME}" --desired-count 0 --region "${AWS_REGION}"

Tear down the simulator (ECR + VPC + ECS + Budget; keeps base stack):
  aws cloudformation delete-stack \\
    --stack-name "${SIMULATOR_STACK_NAME}" --region "${AWS_REGION}"

Cost note: with TargetRate=${TARGET_RATE}, expect ~\$80-200/mo dominated
by Redshift Serverless RPU-hours. Monthly budget is \$${MONTHLY_BUDGET_USD}
(view the AWS Budgets console for current spend).
EOF
