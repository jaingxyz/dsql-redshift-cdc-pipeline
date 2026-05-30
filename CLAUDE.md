# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A reference / sample CDC pipeline: **Aurora DSQL → Kinesis → Lambda → Redshift Serverless**, deployed via CloudFormation + a small set of bash scripts that handle the parts CFN doesn't cover yet (DSQL CDC stream, schema load, Lambda code deploy). Python order simulator drives realistic load.

This is **not** a published library. There's no package, no distribution. It exists to be cloned, reasoned about, and optionally deployed end-to-end into a personal AWS account.

## Layout

```
app/
  cdc_processor.py     Lambda handler — parses Kinesis CDC records, INSERTs into Redshift cdc_events
  order_simulator.py   psycopg-based order driver (insert/update/delete) against the DSQL schema
  requirements.txt     boto3 + psycopg[binary] (simulator-side only — Lambda uses bundled boto3)
infra/
  cloudformation.yaml  DSQL cluster, Kinesis stream, IAM roles, Redshift namespace+workgroup, Lambda shell, event source mapping
  scripts/
    bootstrap.sh       One-shot orchestrator
    01..04-*.sh        Individual phases (CFN deploy → CDC stream → schemas → Lambda code)
    teardown.sh        Removes everything (assumes DSQL deletion protection is off)
    _lib.sh            Shared helpers (log/ok/warn/err, require, stack_output, check_aws_creds)
schema/
  dsql_schema.sql      Source schema (customers, products, orders, order_items)
  redshift_schema.sql  Append-only cdc_events log + current-state views via ROW_NUMBER over commit_timestamp
analytics/
  sample_queries.sql   Example queries against the materialized current-state views
```

## Commands

This is mostly an infra repo; "commands" means deploy/teardown, not build/test.

- `infra/scripts/bootstrap.sh` — full deploy (idempotent; safe to re-run)
- `infra/scripts/teardown.sh` — full teardown (only works if DSQL deletion protection is off)
- `python app/order_simulator.py --duration 300 --rate 5` — drive load (assumes DSQL connection env vars set)

There are no unit tests. Validation lives in CI: `ruff check`, `ruff format --check`, `shellcheck`, `cfn-lint`.

## Critical invariants

- **Parameterized SQL only.** `cdc_processor._build_parameterized_insert` constructs the SQL with `:p1, :p2, ...` placeholders and passes values via the Redshift Data API's `Parameters=` argument. **Never** string-concatenate values into the SQL. Source data flows through this path.
- **Redshift Data API parameter limit is 200 per statement.** With 5 parameters per CDC row, we chunk into batches of 40 (`ROWS_PER_CHUNK` env var, default 40 = `MAX_PARAMS_PER_STATEMENT // PARAMS_PER_ROW`). Don't raise this above 40 without revisiting the math.
- **`execute_statement` is async.** It returns a statement Id immediately. We poll `describe_statement` until each chunk reaches FINISHED so any FAILED/ABORTED status raises a real Lambda error and the Kinesis event source mapping retries the batch. **Do not skip the poll** — silent failures would let Kinesis advance past lost data.
- **Append-only writes.** Every CDC event becomes a row in `cdc_events`; current state is reconstructed downstream via `ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY commit_timestamp DESC)`. Safe under unordered/duplicate Kinesis delivery.
- **DSQL CDC preview limitation: only `c` and `d` ops.** Both INSERT and UPDATE arrive as `c` (create); reconstruction handles this correctly via the latest-commit-timestamp logic. Don't assume an `u` (update) op will appear.
- **Bootstrap shell scripts use `set -euo pipefail`.** Any unhandled command failure aborts. New scripts must follow the same convention or they'll regress error-detection.
- **`_lib.sh` is sourced, not exec'd.** All shared functions (`log/ok/warn/err`, `require`, `stack_output`, `check_aws_creds`) come from there. shellcheck `SC1091` is suppressed at the workflow level because `_lib.sh` isn't reachable at lint time.

## CloudFormation specifics worth remembering

- **DSQL CDC stream is NOT in the CFN template.** It's created by `02-create-cdc-stream.sh` because DSQL CDC is in public preview and doesn't have a CFN resource type yet. Same for schema load (`03-load-schemas.sh`) and Lambda code deploy (`04-deploy-lambda-code.sh`).
- **Redshift Data API actions don't support resource-level permissions.** The IAM policy uses `Resource: "*"` for `redshift-data:*` — that's the only valid value. Don't try to scope it.
- **`ManageAdminPassword: true`** on the Redshift namespace means the password is generated and rotated by AWS in Secrets Manager. The Lambda doesn't touch it; it uses Data API + IAM auth via `redshift-serverless:GetCredentials`.
- **`BatchSize: 100` on the EventSourceMapping** is the upper bound on records per Lambda invocation. The Lambda chunks internally to stay within Redshift Data API's 200-parameter limit, so 100 records × 5 params = 500 params total → 13 chunks max. Don't raise BatchSize beyond what the chunking can absorb within the Lambda timeout (currently 120s).
- **No DeletionPolicy on Redshift resources.** This is a demo stack — `teardown.sh` is meant to remove everything. For production, add `DeletionPolicy: Snapshot` to the Namespace.

## Testing

There is no test runner. Adding one would mean either:
- Mocking boto3 / Redshift Data API (high friction, low value for sample code), or
- Live integration tests against a real AWS account (real cost, real keys in CI).

The pragmatic call: rely on `cfn-lint` + `ruff check` + `shellcheck` to catch the things that can be caught statically. End-to-end correctness comes from running `bootstrap.sh` and watching the simulator drive events through to Redshift.

## Working with AWS via coding-agent tooling

When making AWS calls or generating IaC for this repo, prefer the
[Agent Toolkit for AWS](https://github.com/aws/agent-toolkit-for-aws)
and the AWS Labs MCP servers over guessing or using `aws` CLI calls
that bypass observability:

- **`aws-core` plugin** — service selection, CDK/CloudFormation,
  serverless, observability, billing, deployment skills. Loaded via
  `claude plugin install aws-core@claude-plugins-official`.
- **`aws-data-analytics` plugin** — S3 Tables, Glue, Athena, ETL.
  Useful when extending the pipeline beyond Redshift Serverless.
- **Aurora DSQL MCP server** (`aurora-dsql-mcp`) — connected to the
  cluster from `infra/.env.bootstrap`. Has a `dsql_lint` tool that
  catches DSQL-incompatible SQL before you run it; the schema in
  `schema/dsql_schema.sql` was validated with it.
- **Redshift MCP server** (`awslabs.redshift-mcp-server`) — runs
  queries against the workgroup. Use this instead of hand-rolling
  `aws redshift-data execute-statement` for ad-hoc analytics.
- **AWS MCP server** (bundled with the plugins above) — full AWS API
  coverage + sandboxed Python. Prefer over raw `aws` CLI for any
  multi-step operation that benefits from audit logging.

General guidance (from `aws/agent-toolkit-for-aws/rules/`):

- Before starting an AWS task, check whether a relevant skill exists
  in the loaded plugins and prefer its guidance over training data.
- When uncertain about API parameters, permissions, or limits, verify
  against AWS docs (the AWS MCP server's documentation tools, the
  references below, or the AWS Labs MCP server READMEs) instead of
  guessing. State uncertainty explicitly.
- For new infrastructure, prefer CloudFormation (this repo's existing
  pattern) or CDK over standalone CLI commands.
- Do not use em dashes in AWS resource names — hyphens only.

## References

- Aurora DSQL: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/
- Aurora DSQL CDC (preview): https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cdc.html
- Redshift Data API: https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html
- Agent Toolkit for AWS: https://github.com/aws/agent-toolkit-for-aws
- AWS Labs MCP servers: https://github.com/awslabs/mcp
- Companion blog post: https://gauravjx.substack.com/p/zero-etl-dsql-to-redshift-almost (also on [AWS Builder Center](https://builder.aws.com/content/39S4beDMSbn6piEwUXKUxyNpjkM/zero-etl-dsql-to-redshift-almost))
