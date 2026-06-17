-- Iceberg time-travel against the cold archive: reproducible training sets.
--
-- The cold leg of this pipeline (Firehose -> S3 Tables -> Apache Iceberg,
-- surfaced in Redshift as the external schema `cold.cdc_events_archive`) is
-- an append-only table where every Firehose commit is a new Iceberg snapshot.
-- That snapshot history is the raw material for reproducible analytics: pin a
-- snapshot, and you get the exact dataset as it existed at that instant -
-- forever, no matter how much the live table grows afterward.
--
-- IMPORTANT ENGINE NOTE (validated against a live deployment, 2026-06):
--   Redshift Spectrum does NOT support Iceberg time-travel syntax. Running
--   `... FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` against the external
--   `cold` schema returns a syntax error. Time-travel is an Athena / Trino /
--   Spark capability. So this file is split:
--     * Part A runs in ATHENA (against the S3 Tables catalog) - the snapshot
--       listing and the actual time-travel queries.
--     * Part B runs in REDSHIFT - the "what can I still do from Redshift"
--       fallback (point-in-time by commit_timestamp, no snapshot pinning).
--
-- Why two catalogs in Athena matter (also validated live):
--   The Glue *resource link* (`dsql-cdc_iceberg_link`) that Redshift reads
--   through allows `count(*)` but blocks column access and hides the Iceberg
--   metadata tables. Query the NATIVE S3 Tables catalog instead. Mind the two
--   different identifier forms for the same catalog:
--     * Athena/Glue API context (e.g. start-query-execution --query-execution-
--       context Catalog=...): the federated path  s3tablescatalog/<bucket>
--     * SQL identifier inside a query: the 3-part, double-quoted, @-separated
--       form  "<bucket>@s3tablescatalog".<namespace>.<table>
--       (the slash form is NOT a valid SQL identifier and is rejected).
--     database/namespace = <your-namespace>   (e.g. cdc)
--     table              = cdc_events_archive
--   The querying identity also needs a Lake Formation grant on the table
--   (table-level SELECT + DESCRIBE; the bucket-nested catalog and namespace
--   need DESCRIBE too). `count(*)` succeeding while `SELECT col` fails with
--   "Relation contains no accessible columns" is the tell-tale of a missing
--   table grant. Granting requires Data-Lake-Admin status on the account.
--   (See the `lakehouse-redshift` skill for the full grant sequence.)


-- ===========================================================================
-- PART A - ATHENA (S3 Tables catalog). Snapshot history + time-travel.
-- ===========================================================================

-- A1. List the snapshot history. Every Firehose commit is one append snapshot;
--     a continuously-fed pipeline accumulates thousands. `committed_at` is the
--     timestamp you can time-travel to; `snapshot_id` is the exact handle.
SELECT committed_at,
       snapshot_id,
       operation,
       summary['total-records'] AS total_records
FROM "cdc_events_archive$snapshots"
ORDER BY committed_at;

-- A2. Pin a "training set v1" by SNAPSHOT ID. This is bit-for-bit reproducible:
--     it returns the same rows today, next month, and after the live table has
--     doubled - as long as snapshot expiration hasn't pruned it.
SELECT count(*) AS rows_at_snapshot
FROM cdc_events_archive
FOR VERSION AS OF 1398433419463122261;   -- replace with a snapshot_id from A1

-- A3. Pin the same training set by WALL-CLOCK TIME. Athena resolves the
--     timestamp to the snapshot that was current at that instant. Use this when
--     you think in "the data as of 2pm Tuesday" rather than in snapshot IDs.
SELECT count(*) AS rows_at_timestamp
FROM cdc_events_archive
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-13 12:00:00 UTC';

-- A4. The reproducibility proof: a real feature query, computed AS OF the
--     training snapshot. Reconstructs current order state (dedupe by record_id,
--     keep latest commit) exactly as the live views do - but frozen in time.
--     Run this against the snapshot and against the live table (drop the
--     FOR VERSION clause); the snapshot answer never changes, the live one does.
SELECT ed.status                       AS status,
       count(*)                        AS events,
       count(DISTINCT t.record_id)     AS orders
FROM (
    SELECT record_id,
           operation,
           json_parse(event_data)      AS ed,
           row_number() OVER (PARTITION BY record_id
                              ORDER BY commit_timestamp DESC) AS rn
    FROM cdc_events_archive FOR VERSION AS OF 1398433419463122261
    WHERE source_table = 'orders'
) t
WHERE t.rn = 1 AND t.operation <> 'd'
GROUP BY ed.status
ORDER BY status;


-- ===========================================================================
-- PART B - REDSHIFT fallback. No snapshot pinning; point-in-time by timestamp.
-- ===========================================================================
--
-- Redshift can't pin an Iceberg snapshot, but the archive carries
-- `commit_timestamp` on every row, so you can still reconstruct state "as of"
-- a wall-clock instant by filtering. This is reproducible only to the extent
-- that the cold archive is append-only and rows are never rewritten (true for
-- this pipeline). It is NOT snapshot-isolation: a late-arriving row with an
-- older commit_timestamp would change a historical answer; snapshot pinning in
-- Part A would not. Use Part A when you need true reproducibility.

-- B1. Reconstruct order state as of a point in time, from the cold archive.
--     (event_data in the external table is a JSON string; JSON_PARSE recovers
--     SUPER, then navigate.)
SELECT ed.status::VARCHAR              AS status,
       COUNT(*)                        AS events,
       COUNT(DISTINCT record_id)       AS orders
FROM (
    SELECT record_id,
           operation,
           JSON_PARSE(event_data)      AS ed,
           ROW_NUMBER() OVER (PARTITION BY record_id
                              ORDER BY commit_timestamp DESC) AS rn
    FROM cold.cdc_events_archive
    WHERE source_table = 'orders'
      AND commit_timestamp <= TIMESTAMP '2026-06-13 12:00:00'   -- "as of"
) t
WHERE t.rn = 1 AND t.operation <> 'd'
GROUP BY ed.status::VARCHAR
ORDER BY status;
