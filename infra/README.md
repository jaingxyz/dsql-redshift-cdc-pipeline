# Infrastructure

Bootstrap, manage, and tear down the AWS resources for the CDC pipeline.

## What gets created

| Resource | How | Notes |
|---|---|---|
| Aurora DSQL cluster | CloudFormation (`AWS::DSQL::Cluster`) | Single-region. Deletion protection on by default. |
| Kinesis Data Stream (on-demand) | CloudFormation | Receives CDC events from DSQL. |
| IAM role for DSQL CDC → Kinesis | CloudFormation | Trust policy scoped to this cluster's ARN (least privilege). |
| Redshift Serverless namespace + workgroup | CloudFormation | Target data warehouse. Admin password is AWS-managed in Secrets Manager. |
| Lambda execution role | CloudFormation | Kinesis read + Redshift Data API + basic logs. |
| Lambda function (CDC processor) | CloudFormation | Created with placeholder code. Real code deployed by `scripts/04`. |
| Kinesis → Lambda event source mapping | CloudFormation | Batch size 100, 5-second batching window. |
| **DSQL CDC stream → Kinesis** | AWS CLI (`scripts/02`) | DSQL CDC is in public preview and not yet covered by CloudFormation. |
| Source + target schemas | psql + Redshift Data API (`scripts/03`) | DSQL: `customers`, `products`, `orders`, `order_items`. Redshift: `cdc_events` log + per-table current-state views. |
| **Optional**: always-on order simulator | CloudFormation (`cloudformation-simulator.yaml`) | ECR + minimal VPC + Fargate + AWS Budget + GitHub OIDC role. Deployed via `scripts/05`. |
| **Optional**: SageMaker access | CloudFormation (`cloudformation-sagemaker.yaml`) | IAM role + managed policy granting Secrets Manager read, `GetCredentials` on the workgroup, and `redshift-data:*`. |

## Prerequisites

- AWS CLI v2 with credentials configured (`aws configure` or `AWS_PROFILE`)
- `psql` (PostgreSQL client) for loading the DSQL schema
- `zip` for packaging the Lambda
- AWS account permissions for: DSQL, Kinesis, IAM, CloudFormation, Lambda, Redshift Serverless

## Quick start

```bash
cd infra/scripts
./bootstrap.sh
```

That's it. The script orchestrates all four stages and saves intermediate state to `infra/.env.bootstrap` for resumability.

### Customize via environment

```bash
PROJECT_NAME=my-cdc \
AWS_REGION=us-west-2 \
REDSHIFT_BASE_CAPACITY=16 \
DSQL_DELETION_PROTECTION=false \
./bootstrap.sh
```

| Variable | Default | What it controls |
|---|---|---|
| `PROJECT_NAME` | `dsql-cdc` | Prefix for all resource names |
| `AWS_REGION` | `us-east-1` | Region for everything |
| `STACK_NAME` | `${PROJECT_NAME}-stack` | CloudFormation stack name |
| `REDSHIFT_BASE_CAPACITY` | `8` | Redshift Serverless RPUs (min 8) |
| `DSQL_DELETION_PROTECTION` | `true` | Cluster deletion protection. Set `false` for ephemeral test stacks. |

### Run individual stages

Each script is self-contained and idempotent:

```bash
./01-deploy-cfn.sh             # ~3-5 min (DSQL cluster creation is the longest part)
./02-create-cdc-stream.sh      # ~2 min
./03-load-schemas.sh           # ~30s
./04-deploy-lambda-code.sh     # ~10s
```

State flows through `infra/.env.bootstrap` — source it to inspect or use the resources from your shell:

```bash
source infra/.env.bootstrap
echo $DSQL_CLUSTER_ENDPOINT
echo $REDSHIFT_WORKGROUP
echo $LAMBDA_FUNCTION_NAME
```

## Verifying the pipeline

After bootstrap, drive activity and confirm events land in Redshift:

```bash
source infra/.env.bootstrap

# Generate realistic e-commerce traffic
cd ../app
pip install boto3 'psycopg[binary]'
DSQL_CLUSTER_ID="${DSQL_CLUSTER_ID}" python3 order_simulator.py --duration 60 --rate 5

# Wait ~10 seconds for events to flow through, then count rows in Redshift
aws redshift-data execute-statement \
    --workgroup-name "${REDSHIFT_WORKGROUP}" \
    --database "${REDSHIFT_DATABASE}" \
    --sql "SELECT source_table, COUNT(*) FROM cdc_events GROUP BY 1"
```

You should see counts climb across `customers`, `products`, `orders`, and `order_items` in real time.

## Optional add-on stacks

These are deployed independently from the base stack — they're additive
and can be torn down without touching the core pipeline.

### Always-on order simulator (`cloudformation-simulator.yaml`)

Runs `order_simulator.py` continuously on Fargate so the pipeline stays
warm. Provisions an ECR repo, minimal VPC (public subnets, no NAT), an
ECS service, an AWS Budget, and a GitHub OIDC role for image builds.

```bash
infra/scripts/05-deploy-simulator.sh
```

The container image is built and pushed by the GitHub Actions workflow
in `.github/workflows/build-simulator.yml` — the script just provisions
the infrastructure shell. Cost: ~$80–200/mo dominated by Redshift
Serverless.

Tear down: `aws cloudformation delete-stack --stack-name dsql-cdc-simulator`

### SageMaker access (`cloudformation-sagemaker.yaml`)

Creates (or augments) an IAM role with the two ways a SageMaker
notebook can authenticate to the workgroup:

1. **Secrets Manager** — read the auto-rotated admin password
2. **`redshift-serverless:GetCredentials`** — short-lived federated creds

…plus the `redshift-data:*` actions either path needs.

Deploy via the wrapper script (recommended — also runs the GRANTs the
federated path needs, idempotently):

```bash
infra/scripts/06-deploy-sagemaker.sh
```

Or with raw CFN:

```bash
aws cloudformation deploy \
    --stack-name dsql-cdc-sagemaker \
    --template-file infra/cloudformation-sagemaker.yaml \
    --parameter-overrides ProjectName=dsql-cdc \
    --capabilities CAPABILITY_NAMED_IAM \
    --region us-east-1
```

Parameters:

| Parameter | Default | What it does |
|---|---|---|
| `ProjectName` | `dsql-cdc` | Must match the base stack — used for Fn::ImportValue lookups. |
| `ExistingRoleName` | `""` | Empty = create a new exec role with `AmazonSageMakerFullAccess`. Set to a role NAME (not ARN) to attach the Redshift policy onto an existing role. |

**Output**: `ExecutionRoleArn` — paste this into the SageMaker domain
or notebook's "Execution role" field.

**Grants.** `schema/redshift_schema.sql` already grants `SELECT` on
`cdc_events` and the four `*_current` views to `PUBLIC`, so any
auto-created federated DB user can read them. If you want grants
scoped to the SageMaker role specifically, `06-deploy-sagemaker.sh`
also runs:

```sql
CREATE USER "IAMR:dsql-cdc-sagemaker-exec-role" WITH PASSWORD DISABLE;
GRANT SELECT ON ALL TABLES IN SCHEMA public
    TO "IAMR:dsql-cdc-sagemaker-exec-role";
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO "IAMR:dsql-cdc-sagemaker-exec-role";
```

`ALTER DEFAULT PRIVILEGES` keeps future tables/views grant-ed without
re-running. Use `SKIP_GRANTS=1` to deploy the stack only and skip this
step.

#### Connecting from SageMaker Unified Studio

Studio's left-sidebar **Data → Catalogs** tree shows objects from the
**connecting DB user's** perspective. Setup:

1. **Project → Data → Connections → Add → Amazon Redshift**
2. **Redshift compute**:
   `jdbc:redshift://dsql-cdc-wg.<account>.<region>.redshift-serverless.amazonaws.com:5439/dev`
3. **JDBC URL Parameters**: `groupFederation=True` (Studio default —
   enables IAM federation against Redshift Serverless).
4. **Authentication type = IAM**, **Access role ARN** =
   `arn:aws:iam::<account>:role/dsql-cdc-sagemaker-exec-role` (or
   leave empty to use the project's own identity, which also works
   thanks to the PUBLIC grants).
5. Refresh the catalog tree — `dev → public` should show
   **`tables(1)`** + **`views(4)`**.

If you see `views(0)`, the connecting DB user has SELECT on
`cdc_events` (from the PUBLIC grant) but not on the views. Re-run
`infra/scripts/03-load-schemas.sh` to apply the PUBLIC grant added by
this commit, or run `GRANT SELECT ON <view> TO PUBLIC` as admin.

Reference docs:

- [SageMaker Unified Studio overview](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/what-is-sagemaker-unified-studio.html)
- [Add an Amazon Redshift connection](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/connections.html)
- [Redshift JDBC option `groupFederation`](https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-configuration-options.html)
- [`redshift-serverless:GetCredentials` API](https://docs.aws.amazon.com/redshift-serverless/latest/APIReference/API_GetCredentials.html)
- [Redshift admin password in Secrets Manager (`ManageAdminPassword`)](https://docs.aws.amazon.com/redshift/latest/mgmt/redshift-secrets-manager-integration.html)

Tear down: `aws cloudformation delete-stack --stack-name dsql-cdc-sagemaker`

### Hot/cold tiering automation (`cloudformation-tiering.yaml`)

Periodically prunes `cdc_events` (the hot path) of rows older than a
retention horizon, gated on `cold.cdc_events_archive` having received
rows for the same window. Without this, hot grows forever and the
unified view's hot/cold UNION just gets slower over time.

**Prerequisite:** the Iceberg cold path (`07-deploy-iceberg.sh`) must
already be running and Firehose must have flushed at least once. The
safety check queries `cold.cdc_events_archive` directly — if Firehose
has never flushed, every prune aborts with the "cold archive empty"
SNS alert.

```bash
infra/scripts/08-deploy-tiering.sh
```

**What gets created:**

- A Step Functions state machine that submits the safety check, then
  the `DELETE`, then `VACUUM DELETE ONLY`, then `ANALYZE`. Each
  Redshift Data API call is async, so each step has a poll loop.
- An EventBridge Scheduler entry that invokes the state machine on a
  schedule. **Deploys `DISABLED` by default** — run a manual
  `start-execution` first to verify the safety check works against
  live data, then enable.
- An SNS topic for abort/failure alerts. Set `TIERING_ALERT_EMAIL=...`
  before running the script to subscribe an email at deploy time.

**Knobs:**

| Env var | Default | Production value |
|---|---|---|
| `TIERING_RETENTION_HOURS` | `24` (demo) | `720` (30d) |
| `TIERING_SCHEDULE` | `rate(1 day)` | `cron(0 6 1 * ? *)` (monthly, 06:00 UTC, 1st) |
| `TIERING_SCHEDULE_STATE` | `DISABLED` | `ENABLED` after manual verification |
| `TIERING_ALERT_EMAIL` | empty | required for production |

**Manual one-shot test:**

```bash
SM_ARN=$(aws cloudformation describe-stacks --stack-name dsql-cdc-tiering \
    --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" \
    --output text)
aws stepfunctions start-execution --state-machine-arn "$SM_ARN"
# Watch the execution graph in the Step Functions console.
```

Expected paths: `SafetyCheck → SubmitDelete → Vacuum → Analyze → Success`
when there's archived data, or `SafetyCheck → AbortNoArchive` (publishes
SNS, ends successfully) when the cold side has zero rows for the window.

Tear down: handled by `teardown.sh` (deletes the tiering stack before the
Iceberg stack so the schedule is gone before the cold archive disappears).

## Tearing it down

```bash
cd infra/scripts
./teardown.sh
```

The script prompts before each destructive action. Pass `YES=1` to skip prompts (use carefully):

```bash
YES=1 ./teardown.sh
```

Order: DSQL CDC stream → disable cluster deletion protection (with confirmation) → CloudFormation stack delete (which removes the DSQL cluster, Kinesis, Redshift, Lambda, and IAM roles) → local env file.

## Why CFN + a couple of scripts (and not all CFN, all CDK, or all bash)?

- **CloudFormation** for everything that has CFN coverage — Aurora DSQL clusters (`AWS::DSQL::Cluster`), Kinesis, IAM, Redshift Serverless, Lambda. Declarative, single-stack rollbacks, easy parameter overrides, free state management.
- **CLI script** only for the **DSQL CDC stream**, because DSQL CDC is in public preview and does not yet have a CloudFormation resource type. Once that's added (and CDK constructs follow), this script can be replaced with a CFN resource.
- **Schema loading** is a CLI script because schemas evolve faster than infrastructure and schema migration tools (Flyway, Liquibase, etc.) typically own this layer in real projects.
- **Lambda code deployment** is a CLI script because iterating on Lambda code shouldn't require redeploying the whole stack. CFN owns the function shell; the script owns the code.

If you'd prefer CDK, the `AWS::DSQL::Cluster` resource is also available as the L1 construct `aws_cdk.aws_dsql.CfnCluster` (and as a higher-level construct in newer CDK versions). The CloudFormation template here is the source of truth for what needs to exist; converting it to CDK or Terraform is straightforward.

## Cost expectations

- **Idle cost**: near-zero. Kinesis on-demand has no idle charge. Redshift Serverless auto-pauses. Lambda is per-invocation. DSQL has minimum compute charges.
- **Active cost during a 60-second simulator run**: typically under $0.50.
- **Long-running development**: ~$5–20/month depending on Redshift query frequency.

Always run `teardown.sh` when you're done experimenting.
