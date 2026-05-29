#!/usr/bin/env bash
# Package the real cdc_processor.py and replace the placeholder Lambda code.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_lib.sh"
[ -f "${SCRIPT_DIR}/../.env.bootstrap" ] && source "${SCRIPT_DIR}/../.env.bootstrap"

require aws
require zip

[ -n "${LAMBDA_FUNCTION_NAME:-}" ] || err "LAMBDA_FUNCTION_NAME not set. Run 01-deploy-cfn.sh first."

REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SRC="${REPO_ROOT}/app/cdc_processor.py"
[ -f "${SRC}" ] || err "Lambda source not found at ${SRC}"

BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "${BUILD_DIR}"' EXIT

cp "${SRC}" "${BUILD_DIR}/cdc_processor.py"
( cd "${BUILD_DIR}" && zip -q cdc_processor.zip cdc_processor.py )

log "Updating Lambda function ${LAMBDA_FUNCTION_NAME}..."
aws lambda update-function-code \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --zip-file "fileb://${BUILD_DIR}/cdc_processor.zip" \
    --region "${AWS_REGION}" \
    --query '{Function:FunctionName,Version:Version,Sha:CodeSha256}' \
    --output table

ok "Lambda code deployed"
