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
