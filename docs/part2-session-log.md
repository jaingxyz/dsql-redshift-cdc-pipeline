# Part 2 Session Log

A real-time log of every issue hit while building the Iceberg cold path,
what we tried, and what worked. Read this at the end to extract a Claude
Code skill that prevents the next person from doing this iteration cycle.

## Context

- Goal: tee the existing Kinesis CDC stream into S3 Tables (Iceberg) so
  Redshift can UNION hot (last 24h in `cdc_events`) + cold (>24h in S3
  Tables).
- Constraint: zero downtime to the live Part 1 pipeline; iterate on the
  Iceberg infra without breaking it.
- Tools available: `aws-core` and `aws-data-analytics` plugins from the
  Agent Toolkit, `aurora-dsql` MCP, `awslabs.redshift-mcp-server`,
  `awsknowledge` MCP. **None used during the iteration cycle below — that's
  the lesson.**

## Issues encountered (chronological)

### 1. `AWS::S3Tables::Table` race with namespace propagation

**Symptom**: `Resource handler returned message: "The specified namespace
does not exist." (Status Code: 404)`

**Root cause**: The S3 Tables namespace API returns success on
`CreateNamespace` but the namespace isn't readable to subsequent
`CreateTable` calls for **5+ minutes**. CFN's resource handler considers
the namespace `CREATE_COMPLETE` ~500ms after the API call.

**Tried**:
1. Adding `DependsOn: TableNamespace` — no effect (it was already implicit
   via `!Ref`).
2. Lambda-backed custom resource with 30s of retries (15 × 2s) — failed,
   namespace still not visible.
3. Increasing custom-resource retry to 5 min — would have worked but added
   complexity.

**Resolution**: Move table creation OUT of CFN entirely. Created in
`07-deploy-iceberg.sh` after CFN finishes, with up to 5 min of retries.
The bash retries reliably succeed within ~30s in practice but the cushion
prevents flakes.

**Time wasted**: ~25 min.

### 2. `!Ref` on `AWS::S3Tables::TableBucket` returns the ARN, not the name

**Symptom**: Firehose validation rejected
`destinationDatabaseName=arn:aws:s3tables:...` because
`destinationDatabaseName` must match `[a-zA-Z0-9._]+`.

**Root cause**: Unlike most CFN resources, `!Ref TableBucket` returns the
full table-bucket ARN (`arn:aws:s3tables:region:account:bucket/name`). To
get the bare name we need `!Select [1, !Split ["/", !GetAtt
TableBucket.TableBucketARN]]`.

**Tried**: `!Ref TableBucket` everywhere → all string interpolations broke
silently (the bucket-nested catalog ARN became
`arn:...:catalog/s3tablescatalog/arn:aws:s3tables:...`).

**Resolution**: replaced every `${TableBucket}` substitution with the
extraction pattern.

**Time wasted**: ~15 min.

### 3. S3 Tables bucket name reservation cooldown after delete

**Symptom**: After deleting a stack, redeploying the same template fails
with "The bucket is in a transitional state because of a previous deletion
attempt."

**Root cause**: S3 Tables holds bucket names reserved for several minutes
after delete (similar to S3 but longer).

**Resolution**: Added a `BucketSuffix` parameter so iterators can pass `v2`,
`v3`, etc. without waiting for the cooldown.

**Time wasted**: ~10 min.

### 4. Firehose validates destination Iceberg table at create time

**Symptom**: Stack deploy fails with `Role ... is not authorized to perform:
glue:GetTable for the given table or the table does not exist`.

**Root cause**: Firehose's `IcebergDestinationConfiguration` does a
`glue:GetTable` synchronously during create — not async. So the Iceberg
table has to exist BEFORE the Firehose resource is created.

**Resolution**: Two-phase CFN deploy controlled by an `EnableFirehose`
parameter. Phase 1 deploys bucket + namespace + IAM (no Firehose); the
script creates the Iceberg table; Phase 2 redeploys with
`EnableFirehose=true` to add Firehose.

**Time wasted**: ~10 min.

### 5. Glue catalog ARN regex requires bucket-nested form

**Symptom**: Firehose validation regex
`arn:.*:glue:.*:\d{12}:catalog(?:(/[a-z0-9_-]+){1,2})?` requires at most
2 path components, but `catalog/s3tablescatalog/<bucket>` is 2, fine.
**The actual error was a side effect of issue #2** (`!Ref` returning ARN
instead of name).

**Resolution**: subsumed by #2.

### 6. Redshift `CREATE EXTERNAL SCHEMA ... IAM_ROLE default` requires a default role

**Symptom**: `Cannot find default IAM role on this cluster`.

**Root cause**: Redshift Serverless workgroups don't have a default IAM role
unless one is explicitly associated. Our Part 1 base stack didn't attach
one because the workgroup uses Data API + IAM auth (no Spectrum needs).

**Tried** (current state, in progress): NOT YET FIXED.

**Resolution path**:
- Option A: Create a Redshift IAM role with Glue/Lake-Formation/S3 reads in
  the iceberg stack, attach to the workgroup via
  `AWS::RedshiftServerless::Workgroup.IAMRoles` (would need to update the
  base stack). Cleaner.
- Option B: Pass an explicit `IAM_ROLE 'arn:...'` in the
  `CREATE EXTERNAL SCHEMA` instead of `default`. Doesn't require base-stack
  changes; needs the role to be associated with the workgroup independently.

## Patterns we should have known up front

These are exactly the questions a `s3-tables-iceberg-with-redshift-spectrum`
skill should answer:

1. **`!Ref` on `AWS::S3Tables::TableBucket` returns the ARN.** Always
   extract the name. (1 hour wasted just on this.)
2. **S3 Tables namespace propagation lag is 5+ minutes**, not seconds. CFN
   custom resources are too short-lived. Move dependent resource creation
   OUT of CFN and into a deploy script with bash-level retries.
3. **`AWS::S3Tables::Table` is unreliable** as of mid-2025 due to (1)+(2);
   prefer `aws s3tables create-table` from a script.
4. **Firehose validates the Iceberg destination table synchronously at
   create time.** The table must exist BEFORE the Firehose resource. Two-
   phase CFN deploy is the workaround.
5. **Bucket-nested federated Glue catalog ARN format** is
   `arn:aws:glue:<region>:<account>:catalog/s3tablescatalog/<bucket-name>`.
   Database name is the namespace (no slashes). Tables live at
   `arn:...:table/s3tablescatalog/<bucket>/<namespace>/<table>`.
6. **Redshift workgroup needs a default IAM role** for `CREATE EXTERNAL
   SCHEMA ... IAM_ROLE default` to work. Without it, pass an explicit ARN.
7. **`destinationDatabaseName`** in Firehose Iceberg config rejects slashes
   and hyphens — must match `[a-zA-Z0-9._]+`. Use the namespace name only.
8. **S3 Tables bucket name cooldown** after delete: ~minutes. Always have a
   suffix parameter to iterate without waiting.

## What to do differently next time

A future "build a Firehose-Iceberg-S3Tables-Redshift-Spectrum pipeline"
skill should:
- Lead with the two-phase deploy pattern as a template
- Provide a complete CFN snippet with all the `!Select`/`!Split` workarounds
- Include the post-deploy bash script that creates the table
- Document the Redshift IAM role attachment up front (don't assume the
  workgroup has a default role)
- Reference this session log as the "things that will go wrong" reference

## Next session resumption point

Phase 2 (Firehose) **deployed successfully**. The blocker is the Redshift
external schema needing an IAM role. Once that's fixed, we can verify
Firehose is delivering, then add the UNION view.
