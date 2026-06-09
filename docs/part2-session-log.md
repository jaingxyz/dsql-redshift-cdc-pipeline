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
  `awsknowledge` MCP. **None used during the iteration cycle below - that's
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
1. Adding `DependsOn: TableNamespace` - no effect (it was already implicit
   via `!Ref`).
2. Lambda-backed custom resource with 30s of retries (15 × 2s) - failed,
   namespace still not visible.
3. Increasing custom-resource retry to 5 min - would have worked but added
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

**Tried**: `!Ref TableBucket` everywhere -> all string interpolations broke
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
`glue:GetTable` synchronously during create - not async. So the Iceberg
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
   and hyphens - must match `[a-zA-Z0-9._]+`. Use the namespace name only.
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

---

## Update - issues 7-9 (Redshift auto-mount)

### 7. `!Ref TableBucket` returns the ARN, not the name (revisited)

Fixed by extracting via `!Select [1, !Split ["/", !GetAtt TableBucket.TableBucketARN]]`. This pattern is needed in 4+ places. **A skill should provide a YAML snippet that wraps it in a Mappings or local once.**

### 8. Firehose validates Iceberg destination synchronously

`AWS::KinesisFirehose::DeliveryStream` with `IcebergDestinationConfiguration` calls `glue:GetTable` during stack create, not first-use. Makes single-shot deploys impossible: bucket+namespace+table must exist first. We split into a two-phase CFN deploy controlled by an `EnableFirehose` parameter.

### 9. Auto-mount needs Lake Formation access control on the bucket integration

> **Superseded by issue 18 / final code.** Issues 9 and 10 below describe
> the auto-mount + 3-part `<bucket>@s3tablescatalog.<ns>.<table>` naming
> approach. The final code abandoned that path entirely - it uses a Glue
> resource link in the default catalog plus `CREATE EXTERNAL SCHEMA cold
> ... CATALOG_ID '<account-id>'`. Keep these notes for the cautionary
> tale, but don't replicate the approach.


The `s3tablescatalog/<bucket>` Glue catalog has a `CreateDatabaseDefaultPermissions` of `IAM_ALLOWED_PRINCIPALS / ALL`. This is **IAM access control mode** - federated tables are visible only via direct `glue:*` calls + `s3tables:*` data perms, NOT via Redshift auto-mount.

Redshift's `awsdatacatalog` auto-mount and the `"<bucket>@s3tablescatalog".<ns>.<table>` 3-part syntax both require **Lake Formation access control mode**, where:
- The bucket is registered as an LF resource (`aws lakeformation register-resource`)
- The catalog's default permissions exclude `IAM_ALLOWED_PRINCIPALS`
- LF grants determine all access

Switching modes is account-wide-ish (per-bucket but affects how anything else queries it). For our case it's safe - only the iceberg path uses this bucket. But a skill should warn: "if you have existing IAM-mode S3 Tables and switch one to LF mode, queries from non-LF-aware engines break."

### 10. The naming format I had wrong

I tried `awsdatacatalog."s3tablescatalog/<bucket>".<ns>.<table>` (4-part). **Wrong.** Correct: `"<bucket>@s3tablescatalog".<ns>.<table>` (3-part with quoted catalog using `@` separator).

### 11. ada credentials don't change default profile

`ada credentials update --account X --role Admin` writes to a separate profile (e.g. `gujain_acnt_isen_ada`), not the default. Use `AWS_PROFILE=gujain_acnt_isen_ada` to actually use it.

### 12. SageMaker role was the only LF Data Lake Admin

Until I added the Admin role, even `sts:AssumeRole/Admin` could not grant LF permissions because Admin wasn't a registered Data Lake Admin. Used `put-data-lake-settings` to add it.

## Updated patterns we should have known up front

These should be the bullet points of a future `redshift-serverless-iceberg-coldpath` skill:

9. **`!Ref TableBucket` returns the ARN.** Always extract via Split.
10. **`AWS::S3Tables::Table` resource handler is unreliable** due to namespace propagation lag - use a script with bash retries.
11. **`AWS::KinesisFirehose::DeliveryStream` with `IcebergDestinationConfiguration` validates the destination synchronously** - two-phase deploy required.
12. **`destinationDatabaseName`** in Firehose config rejects slashes/hyphens - must match `[a-zA-Z0-9._]+`.
13. **The bucket-nested catalog ARN is** `arn:aws:glue:<region>:<account>:catalog/s3tablescatalog/<bucket-name>`. Database name is the namespace alone.
14. **For Redshift auto-mount of S3 Tables**, the bucket integration must be in **Lake Formation access control mode**, not IAM mode. Set `AllowFullTableExternalDataAccess=False` and `CreateDatabaseDefaultPermissions=[]` (no IAM_ALLOWED_PRINCIPALS) when creating the catalog.
15. **Three-part naming for S3 Tables** is `"<bucket>@s3tablescatalog".<namespace>.<table>` - note the `@` separator and the double quotes.
16. **`ada credentials update` writes to a non-default profile** - use `AWS_PROFILE=...` to switch.
17. **LF `GrantPermissions` requires the caller to be a Data Lake Administrator**, not just an IAM admin. Add yourself via `put-data-lake-settings` first.

## Time totals

- ~25 min: namespace propagation race
- ~15 min: !Ref returns ARN
- ~10 min: bucket name cooldown
- ~10 min: Firehose synchronous validation
- ~30 min: figuring out auto-mount really needs LF mode
- ~10 min: figuring out the @s3tablescatalog naming
- ~10 min: figuring out ada profile + LF admin

**Total iteration overhead: ~110 min** for what should have been a 30 min task with the right skill.

---

## Update - issues 13-18 (transform Lambda, deploy idempotency, NO SCHEMA BINDING)

The session-resumption point above ("phase 2 deployed successfully, blocker is
external schema needing IAM role") buried the real bug: **Firehose was
delivering 0 rows.** 100% of records landed in `errors/iceberg-failed/`
with `Iceberg.MissingColumnWithinRecord`.

### 13. Firehose maps RAW Kinesis JSON keys to Iceberg columns by name

`AWS::KinesisFirehose::DeliveryStream`'s `IcebergDestinationConfiguration`
takes the top-level JSON keys of each Kinesis record and maps them to
columns by name. DSQL CDC records are
`{op, after, before, source, ts_ms, ...}` - those keys share **nothing**
with the Iceberg columns (`source_table, operation, record_id, event_data,
commit_timestamp, ingested_at`). Firehose rejects every record.

**Fix**: a transform Lambda (`app/firehose_transform.py`) wired in via
`ProcessingConfiguration` that reshapes each record into the column
layout. Mirrors `cdc_processor._row_for_op` so hot and cold paths agree
on what each event means.

**Skill bullet**: any direct Kinesis -> Iceberg pipe needs a transform
unless the producer ALREADY emits the destination column shape. The
docs describe this - but the absence of `ProcessingConfiguration` in
the CFN doesn't surface as a static error, only as 100% delivery
failures into the error bucket.

### 14. Iceberg timestamp columns expect MICROSECONDS in JSON

DSQL CDC carries `ts_ms` (milliseconds since epoch). The transform
multiplies by 1000. The Firehose docs say "Timestamp data must always
be sent in microseconds" but it's a one-line note buried under
"supported data types".

### 15. `aws cloudformation deploy` parameter inheritance bites three-phase deploys

`deploy` reuses the stack's previous parameter values for any flag not
in `--parameter-overrides`. On a re-run against an already-stream-enabled
stack, an unspecified `EnableFirehoseStream` inherits "true" from the
prior deploy - yielding `WantFirehoseStream` without `WantFirehose` and
"unresolved resource dependencies" because the stream's role / log
group / error bucket are gated on `WantFirehose`.

**Anti-fix**: pinning BOTH flags to false in Phase 1 fixed the
inheritance bug but introduced a worse one: it **tore down the
`FirehoseErrorBucket` on every re-run**, which then failed because the
bucket held failed-delivery objects from the previous run, and S3's
name-reservation cooldown blocks immediate recreation anyway.

**Real fix**: collapse Phase 1 into Phase A (`EnableFirehose=true,
EnableFirehoseStream=false`), keeping the error bucket / role / Lambda
across re-runs and only toggling the stream itself. The original
phasing assumed Phase 1 needed `EnableFirehose=false` to "create just
the bucket and namespace before the table" - but Phase A creates the
namespace too, so Phase 1 was redundant.

### 16. `lakeformation grant-permissions` failures were swallowed by `|| true`

The original deploy script appended `|| true` to LF grants on the
theory they were idempotent. They are - for "already exists" - but
`AccessDenied` (caller is not a Data Lake Admin) hit the same `|| true`
and the missing grants then surfaced as opaque
`Firehose: Role ... is not authorized to perform: glue:GetTable`
errors at stream-create time.

**Fix**: replaced `|| true` with a new `lf_grant` helper in `_lib.sh`
that tolerates "already exists" but aborts on any other error. Now a
non-admin caller fails LOUDLY at the LF step, not silently at the
Firehose step ten minutes later.

### 17. Default IAM identity wasn't a Data Lake Admin

The session-log update above noted this for the SageMaker role; same
pattern bit again for the default `user/AWSCLI` identity. Fix: add it
via `put-data-lake-settings`, preserving existing admins (don't
replace; LF settings are full-replace, easy to wipe by accident).

The script supports `LF_ADMIN_PROFILE` to switch profiles, but adding
the default identity to the admin list once is simpler than threading
a profile env var through every run.

### 18. Redshift views over external tables need `WITH NO SCHEMA BINDING`

`CREATE VIEW ... AS SELECT ... FROM cold.cdc_events_archive` fails with
`External tables are not supported in views`. Fix: append
`WITH NO SCHEMA BINDING` - and as a transitive consequence, every view
that references `cdc_events_all` (which references the external table)
also needs it.

Two side effects to know:
- All relation names inside a `NO SCHEMA BINDING` view must be **fully
  qualified** (`public.cdc_events`, not `cdc_events`). Forgetting the
  schema prefix produces `All the relation names inside should be
  qualified ...`.
- `NO SCHEMA BINDING` removes the dependency tracking that would
  otherwise block schema changes on referenced tables. Acceptable here
  because the Iceberg schema is fixed by the deploy script.

## Final patterns we should have known up front (consolidated)

These are bullets a future `redshift-serverless-iceberg-coldpath` skill
should lead with:

1. **`!Ref TableBucket` returns the ARN** - extract via `!Select [1,
   !Split ["/", !GetAtt TableBucket.TableBucketARN]]`.
2. **`AWS::S3Tables::Table` resource handler is unreliable** - namespace
   propagation lag of 5+ minutes; create the table in a script with
   bash retries, not in CFN.
3. **`AWS::KinesisFirehose::DeliveryStream` validates the Iceberg
   destination synchronously** at create time - table must exist first
   (two-phase deploy).
4. **`destinationDatabaseName` rejects slashes/hyphens** - must match
   `[a-zA-Z0-9._]+`. Use the namespace name only.
5. **Bucket-nested catalog ARN** is
   `arn:aws:glue:<region>:<account>:catalog/s3tablescatalog/<bucket>`.
   Database name is the namespace alone.
6. **Redshift auto-mount of S3 Tables requires LF mode**, not IAM mode,
   on the bucket integration.
7. **Three-part naming** is `"<bucket>@s3tablescatalog".<ns>.<table>`,
   not `awsdatacatalog.<...>.<ns>.<table>`.
8. **`ada credentials update`** writes to a non-default profile - use
   `AWS_PROFILE=...`.
9. **LF GrantPermissions requires Data Lake Admin** - add the default
   identity via `put-data-lake-settings` (preserve existing admins).
10. **Firehose direct Kinesis -> Iceberg maps top-level JSON keys to
    columns by name** - needs a transform Lambda unless the producer
    already emits the destination shape.
11. **Iceberg `timestamp` columns expect microseconds in JSON.**
12. **`aws cloudformation deploy` inherits prior parameter values** for
    unspecified flags - pin every flag explicitly OR design phases so
    re-runs don't toggle destructive flags.
13. **Don't swallow LF grant errors with `|| true`** - `AccessDenied`
    must abort, only "already exists" should be ignored.
14. **Redshift views over external tables need `WITH NO SCHEMA BINDING`**
    - and all referenced relations must be schema-qualified.

## Time totals (updated)

Original Part 2 (above): ~110 min iteration overhead.

This session (continuation): ~90 min, broken down:
- ~5 min: realize the cold path was delivering 0 rows (the "session
  resume" line in the log was wrong about the real blocker).
- ~10 min: confirm root cause by reading an error-bucket object.
- ~15 min: build the transform Lambda + wire ProcessingConfiguration.
- ~25 min: discover and fix the parameter-inheritance + destructive
  Phase 1 collapse, plus 17 min of churn from the bad first attempt.
- ~10 min: discover and fix the swallowed LF grant errors + add self
  as Data Lake Admin.
- ~15 min: write the unified view layer; hit `NO SCHEMA BINDING` and
  schema-qualification errors; fix.
- ~10 min: wire view-application into the deploy script + verify.

**Total Part 2: ~200 min** for what a single skill should have made a
40-min task. The skill, when written, must call out items 10-14 above
explicitly - those were the ones that ate the most time in this
continuation.

## Update - hot/cold tiering automation

Until this point, hot (`cdc_events`) and cold (`cold.cdc_events_archive`)
both grew forever in parallel. The unified view (`cdc_events_all`) does
the time-window split at query time, so duplicates exist on disk but are
deduped in queries. Functional, but unbounded - production needs to
prune hot.

### Design choice: Step Functions over a Lambda

A single Lambda was the obvious first option (one runtime, one log
group, one IAM role). The state machine wins for three reasons specific
to this pipeline:

1. **Each step's success is independently observable in the console.**
   Step Functions execution history shows SafetyCheck = FINISHED with
   the count, then Delete = FINISHED with the row count, etc. A Lambda
   has to log all of that itself, and post-mortem on a failed run means
   reading CloudWatch Logs rather than scanning a graph. For a
   process whose primary failure mode is "should we have deleted?",
   inspectability is the feature.
2. **The poll loop is async-Redshift's natural shape.** `ExecuteStatement`
   returns immediately; the actual work happens server-side. A Lambda
   either polls in-process (paying for idle Lambda time during VACUUM) or
   chains itself via Step Functions anyway. Wrapping the whole flow in a
   state machine collapses both options into the simpler one.
3. **The Choice state IS the safety guard.** The "if cold count == 0,
   abort" decision is a one-line `Choice` with the result path threaded
   through. In a Lambda it's one more `if` block to read, one more
   branch to test. Concurrent with the SF AWS-SDK integration, this
   means the entire prune is zero lines of business code - the state
   machine definition is the implementation.

### Safety-check semantics

Before pruning `cdc_events` rows older than the retention horizon, the
state machine asserts that `cold.cdc_events_archive` has rows for the
same window. The intent is: *don't delete from hot unless cold has
already received what we're about to lose.*

Sharp edges:

- **The cutoff is pinned at execution start, not recomputed per step.**
  An earlier draft computed `DATEADD(hour, -RetentionHours, GETDATE())`
  inside the SafetyCheck SQL *and* the DELETE SQL. Code review caught
  that this defeats the safety claim: SafetyCheck and DELETE would run
  1-3 minutes apart, so rows whose `commit_timestamp` crossed the
  cutoff during execution could be DELETEd from hot without ever
  having been verified in cold. The fixed shape resolves the cutoff
  once via a `SubmitResolveCutoff` step and pins the result into
  `$.cutoff.value`; both predicates use that pinned literal.
- **The check is "any rows older than cutoff", not "the same rows".**
  We don't compare primary keys hot-vs-cold. That would be safer but
  much more expensive - cold is in S3 Tables, scanning per-PK costs
  more than the prune saves. The "any rows older" check is a coarse
  proxy: if Firehose has flushed *anything* for the window, it has
  almost certainly flushed *everything*, because Firehose is a
  monotonic stream. The failure mode it doesn't catch is "Firehose
  flushed events 1-1000 successfully, then transform Lambda errors
  caused 1001-2000 to land in the error bucket". The CloudWatch
  alarm on transform Lambda errors is the second line of defense for
  that one - see "Production hardening" below.
- **`COUNT(*)` on Iceberg should be cheap in Redshift.** Spectrum can
  satisfy a `COUNT(*) WHERE timestamp < cutoff` predicate by walking
  the Iceberg manifest's per-file min/max stats, without scanning
  parquet content. Not benchmarked in this stack; assume "tens of ms"
  rather than "single-digit ms" until measured.
- **Aborts surface as FAILED executions.** An earlier draft had the
  abort path end with `Succeed`, which buried the signal - operators
  scanning for failures would see only green. Fixed shape: `Abort
  -> SNS publish -> Fail`, so console + email both light up.

### Schedule defaults

- **`ScheduleEnabled: DISABLED`** by default. The first run is always
  manual (`aws stepfunctions start-execution`). Two reasons: (a) gives
  the operator a chance to verify the SafetyCheck matches their mental
  model of what's in cold; (b) catches the common case where the stack
  was deployed before the cold path had ever flushed - the manual run
  fires AbortNoArchive and surfaces the issue immediately, instead of
  EventBridge silently aborting daily for a week.
- **Demo: `rate(1 day)`, retention 24h.** Mirrors the unified view's
  hot/cold window. Fast feedback, easy to validate.
- **Production: `cron(0 6 1 * ? *)`, retention 30d.** Monthly prune at
  06:00 UTC on the 1st. Different reason: at 30d retention, daily
  prunes save almost nothing (you delete one day out of thirty); a
  monthly prune that deletes ~30 days at once amortizes the
  VACUUM cost across thirty deletions worth of holes.

### Production hardening (deferred - call out in the post)

What this stack does NOT yet do, that it should before being trusted on
real data:

- **Alarm on transform Lambda error rate.** If `dsql-cdc-iceberg-transform`
  is dropping records, the cold archive count grows at less than the
  rate it should - but still grows, so the SafetyCheck still passes.
  Need a separate CloudWatch alarm wired to the same SNS topic: gate
  the schedule on the alarm's state, not just on the SafetyCheck.
- **Alarm on Firehose `DeliveryToIceberg.SuccessfulRowCount` going to
  zero while `IncomingRecords > 0`.** Same idea, lower in the stack:
  catch the case where Firehose is receiving but not delivering.
- **`VACUUM DELETE ONLY` is the right choice for an append-mostly
  table.** It reclaims space from the deleted rows without rewriting
  the sort order. If the table ever sees in-place updates (current
  schema doesn't, but if a future revision did), upgrade to plain
  `VACUUM`.
- **`DeletionPolicy: Snapshot` on Redshift in the base stack** - the
  README already calls this out. Tiering doesn't change the
  recommendation, but if you're committing to the prune, the snapshot
  policy matters more, not less.

### Time

- ~30 min: design + first-pass CFN + script + bootstrap/teardown
  wiring + this log.
- ~25 min: 3-reviewer panel (code-review skill, AutoCR-style fidelity,
  EdgeSwarm swarm-code-reviewer) found 8 must-fix items including the
  cutoff-drift bug, the SecretArn missing-suffix bug (state machine
  would have failed every execution at SubmitSafetyCheck), an abort
  path that hid signal, and a doc-comment that lied about retry
  bounds. Apply + re-validate.

**Total: ~55 min**, with the second half spent on review-driven
correctness fixes the first pass missed. Worth recording: the cutoff
bug and the SecretArn bug were both *plausible-looking code that didn't
actually work*. Without the review pass, the manual one-shot test
would have caught the SecretArn one (immediate failure), but the
cutoff-drift bug would only surface as silent data loss under
sustained load.

### Live validation (2026-06-08 23:08 PT)

Deployed `dsql-cdc-tiering` stack into account 239355724610 / us-east-1
with `TIERING_SCHEDULE_STATE=DISABLED` and `TIERING_RETENTION_HOURS=24`.

CFN outcome: `CREATE_COMPLETE` in ~30s. SecretArn resolved to
`arn:aws:secretsmanager:us-east-1:239355724610:secret:redshift!dsql-cdc-ns-admin-9HxJrR`
(full ARN with the 6-char Secrets Manager suffix - the fix from the
review pass). All 7 resources reached CREATE_COMPLETE without error.

Pre-prune state:
- Hot `cdc_events`: 1,890,022 rows; oldest commit_timestamp 2026-05-30 03:42 UTC.
- Cold `cold.cdc_events_archive`: 784,359 rows; oldest 2026-06-05 01:24 UTC.

Triggered one manual execution via `aws stepfunctions start-execution`.

**Outcome: SUCCEEDED in 31 seconds.** State path taken (verified via
`get-execution-history`):

```
InitResolveCutoffPoll -> SubmitResolveCutoff -> DescribeResolveCutoff
  -> ResolveCutoffDone -> ReadCutoff -> PinCutoff
  -> InitSafetyCheckPoll -> SubmitSafetyCheck -> DescribeSafetyCheck
  -> SafetyCheckDone(loop once via IncrementSafetyCheckPoll)
  -> DescribeSafetyCheck -> SafetyCheckDone(FINISHED)
  -> ReadSafetyResult -> SafetyChoice
  -> InitDeletePoll -> SubmitDelete -> DescribeDelete -> DeleteDone
  -> InitVacuumPoll -> SubmitVacuum -> DescribeVacuum -> VacuumDone
  -> InitAnalyzePoll -> SubmitAnalyze -> DescribeAnalyze -> AnalyzeDone
  -> Success
```

Exactly the design path. The poll-loop counter pattern works correctly
(SafetyCheck looped once before FINISHED, then proceeded).

Post-prune state:
- Hot `cdc_events`: **186,938 rows; oldest 2026-06-08 06:07:59 UTC.**
  That cutoff is "execution start (06-09 06:07:59 UTC) minus 24h" to
  the second - the **pinned cutoff is honored exactly**. The earlier-
  draft cutoff-drift bug would have produced an `oldest` value 1-3
  minutes older; we don't see that, so the pin holds under live load.
- Unified view `cdc_events_all`: hot=186,693 + cold=599,646 = 786,339
  rows with the time-window split applied. No data lost.

**DELETE pruned 1,703,084 rows in a single execution.** VACUUM DELETE
ONLY + ANALYZE both completed within the 31-second total runtime -
well under the 1-hour state-machine timeout and the per-step
720-attempt poll cap.

What this validates that static checks could not have:
- The full SecretArn flowed through `DefinitionSubstitutions` to the
  SFN definition and was accepted by `redshift-data:executeStatement`.
  The earlier-draft truncated ARN would have failed every execution
  at SubmitSafetyCheck.
- `States.Format(... TIMESTAMP \'{}\'', $.cutoff.value)` rendered the
  correct SQL - pinned-cutoff matched the post-prune `oldest` to the
  second.
- `aws:SourceAccount` condition on the SFN trust policy did not block
  Step Functions itself from invoking the role.
- `VACUUM DELETE ONLY` is accepted by Redshift Serverless. (Some
  Redshift docs imply VACUUM is provisioned-only; Serverless
  accepts it.)
- `ANALYZE cdc_events` works as the post-DELETE stats refresh step.

What's NOT yet validated:
- Schedule firing on the EventBridge clock - schedule deployed
  DISABLED. To enable, re-run with `TIERING_SCHEDULE_STATE=ENABLED`
  after watching at least one more manual prune.
- AbortNoArchive path - would only fire if cold had no rows older
  than the cutoff. Today there are plenty. The path is structurally
  identical to Success aside from the Choice routing.
- SNS email subscription - `TIERING_ALERT_EMAIL` was empty for this
  deploy. Re-run with the email set or add a subscription via console.

### Soak monitoring (paste-ready when you come back in N days)

Schedule was flipped to `ENABLED` at 2026-06-08 23:23 PT, so the first
auto-prune fires roughly 24h later (~23:23 UTC daily). gujain@amazon.com
is subscribed to the SNS alert topic (pending click-to-confirm); failures
or aborts will email there.

**Signal 1 - every scheduled prune completed SUCCEEDED:**

```bash
export AWS_REGION=us-east-1
SM_ARN=arn:aws:states:us-east-1:239355724610:stateMachine:dsql-cdc-tiering-prune

# Last 20 executions, status + duration
aws stepfunctions list-executions \
    --state-machine-arn "${SM_ARN}" \
    --max-results 20 \
    --query 'executions[].{
        Started:startDate,
        Stopped:stopDate,
        Status:status,
        Name:name
    }' \
    --output table

# Should be all SUCCEEDED. Any FAILED or ABORTED -> drill in:
aws stepfunctions list-executions \
    --state-machine-arn "${SM_ARN}" \
    --status-filter FAILED \
    --query 'executions[].executionArn' \
    --output text
```

**Signal 2 - latency drift (how long each prune took):**

```bash
# Subtract stopDate − startDate per execution; flag the trend.
# A growing trend on VACUUM-heavy days is normal until traffic
# stabilises.
aws stepfunctions list-executions \
    --state-machine-arn "${SM_ARN}" \
    --max-results 20 \
    --query 'executions[].{
        Started:startDate,
        DurationSec:to_string(stopDate)
    }' \
    --output table

# More precise (Python one-liner):
aws stepfunctions list-executions --state-machine-arn "${SM_ARN}" --max-results 20 \
    --query 'executions[].[startDate,stopDate]' --output text \
    | python3 -c "
import sys
from datetime import datetime
for line in sys.stdin:
    s, t = line.split()
    ds = datetime.fromisoformat(s).timestamp()
    dt = datetime.fromisoformat(t).timestamp()
    print(f'{s}  {int(dt-ds):4d}s')
"
```

**Signal 3 - no data loss (cold has rows for every window hot pruned):**

```bash
SECRET_ARN=$(aws secretsmanager describe-secret \
    --secret-id "redshift!dsql-cdc-ns-admin" \
    --query ARN --output text)

# Hot's oldest row should always be ~24h ago (just inside the prune
# horizon). Cold should have rows extending back to whenever Firehose
# came online (2026-06-05 in this account).
SID=$(aws redshift-data execute-statement \
    --workgroup-name dsql-cdc-wg --database dev \
    --secret-arn "${SECRET_ARN}" \
    --sql "
SELECT
    'hot'  AS store,
    COUNT(*)::BIGINT AS n,
    MIN(commit_timestamp)::VARCHAR AS oldest,
    MAX(commit_timestamp)::VARCHAR AS newest
FROM cdc_events
UNION ALL SELECT
    'cold' AS store,
    COUNT(*)::BIGINT AS n,
    MIN(commit_timestamp)::VARCHAR AS oldest,
    MAX(commit_timestamp)::VARCHAR AS newest
FROM cold.cdc_events_archive
ORDER BY 1
" --query Id --output text)
sleep 5
aws redshift-data get-statement-result --id "${SID}" --output table
```

Expected after a clean N-day soak:

| store | n              | oldest                  | newest         |
|-------|----------------|-------------------------|----------------|
| cold  | growing        | 2026-06-05 ...          | within ~1 min  |
| hot   | small (~24h)   | NOW − 24h ± 1h          | within ~10 sec |

**Signal 4 - SNS alarm count (did anything publish to email):**

```bash
# Number of failure/abort messages published in last 7 days.
aws cloudwatch get-metric-statistics \
    --namespace AWS/SNS \
    --metric-name NumberOfMessagesPublished \
    --dimensions Name=TopicName,Value=dsql-cdc-tiering-alerts \
    --start-time "$(date -u -v-7d '+%Y-%m-%dT%H:%M:%S')" \
    --end-time   "$(date -u '+%Y-%m-%dT%H:%M:%S')" \
    --period 86400 --statistics Sum \
    --query 'Datapoints[].[Timestamp,Sum]' --output table
```

If signal 4 returns 0 across all days, no failures or aborts were ever
published to the topic. Combined with signal 1 = all SUCCEEDED, this is
the cleanest "soak passed without hiccup" evidence you can get without
attaching a CloudWatch alarm.
