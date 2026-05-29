#!/usr/bin/env bash
# Load the source schema into DSQL and the target schema + views into Redshift.
# Requires:
#   * psql on PATH (DSQL connection)
#   * AWS credentials with redshift-data permissions
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_lib.sh"
[ -f "${SCRIPT_DIR}/../.env.bootstrap" ] && source "${SCRIPT_DIR}/../.env.bootstrap"

require aws
require psql

[ -n "${DSQL_CLUSTER_ENDPOINT:-}" ] || err "DSQL_CLUSTER_ENDPOINT not set. Run 01-deploy-cfn.sh first."
[ -n "${REDSHIFT_WORKGROUP:-}" ]    || err "REDSHIFT_WORKGROUP not set. Run 01-deploy-cfn.sh first."
[ -n "${REDSHIFT_DATABASE:-}" ]     || err "REDSHIFT_DATABASE not set. Run 01-deploy-cfn.sh first."

REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DSQL_SCHEMA="${REPO_ROOT}/schema/dsql_schema.sql"
REDSHIFT_SCHEMA="${REPO_ROOT}/schema/redshift_schema.sql"

[ -f "${DSQL_SCHEMA}" ]     || err "DSQL schema not found at ${DSQL_SCHEMA}"
[ -f "${REDSHIFT_SCHEMA}" ] || err "Redshift schema not found at ${REDSHIFT_SCHEMA}"

# -------- Load DSQL schema --------
log "Loading DSQL schema..."
PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token \
    --hostname "${DSQL_CLUSTER_ENDPOINT}" \
    --region "${AWS_REGION}") \
PGSSLMODE=require \
psql -h "${DSQL_CLUSTER_ENDPOINT}" -U admin -d postgres -p 5432 \
    -v ON_ERROR_STOP=1 \
    -f "${DSQL_SCHEMA}"
ok "DSQL schema loaded"

# -------- Load Redshift schema --------
log "Loading Redshift schema (this issues an async statement and waits for completion)..."
sql_text="$(cat "${REDSHIFT_SCHEMA}")"

statement_id=$(aws redshift-data execute-statement \
    --workgroup-name "${REDSHIFT_WORKGROUP}" \
    --database "${REDSHIFT_DATABASE}" \
    --sql "${sql_text}" \
    --region "${AWS_REGION}" \
    --query 'Id' --output text)
log "Submitted statement ${statement_id}"

for i in $(seq 1 60); do
    status=$(aws redshift-data describe-statement \
        --id "${statement_id}" \
        --region "${AWS_REGION}" \
        --query 'Status' --output text)
    case "${status}" in
        FINISHED) ok "Redshift schema loaded"; exit 0 ;;
        FAILED|ABORTED)
            reason=$(aws redshift-data describe-statement \
                --id "${statement_id}" \
                --region "${AWS_REGION}" \
                --query 'Error' --output text)
            err "Redshift statement ${status}: ${reason}"
            ;;
        *) printf '.' >&2; sleep 3 ;;
    esac
    [ "$i" -eq 60 ] && err "Timed out waiting for Redshift schema load"
done
