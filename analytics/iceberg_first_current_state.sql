-- Current-state reconstruction, Iceberg-first: straight off the cold archive,
-- with NO hot Redshift table involved.
--
-- This is the query that makes the Part 3 thesis concrete. In the dual-write
-- design (Parts 2a/2b), "current state" came from a small, continuously-pruned
-- hot table (fast, but the write feed pinned Redshift at its base capacity
-- 24/7). Here there is no hot table: the same answer is reconstructed directly
-- from the append-only Iceberg archive via dedup-on-read, and the workgroup
-- suspends to zero between queries because nothing writes to it.
--
-- The cold `event_data` is stored as a JSON string, so we recover it to SUPER
-- with JSON_PARSE in a subquery, THEN navigate fields one level up. Redshift
-- will not navigate a JSON_PARSE() result inline, so the parse and the field
-- extraction MUST be split across query levels (the same rule the unified
-- views and the time-travel queries follow).
--
-- Run against the Iceberg-first workgroup's `cold` external schema, e.g.:
--   aws redshift-data execute-statement \
--       --workgroup-name <project>-iceberg-wg \
--       --database dev --secret-arn <iceberg-first-admin-secret-arn> \
--       --sql "$(cat analytics/iceberg_first_current_state.sql)"

-- Current state of every order: newest event per key wins; a trailing
-- delete tombstones the row out.
SELECT order_id,
       ed."status"::VARCHAR     AS status,
       ed."total_cents"::BIGINT AS total_cents,
       last_change_at
FROM (
  SELECT record_id             AS order_id,
         JSON_PARSE(event_data) AS ed,        -- cold event_data is a JSON string; recover SUPER
         commit_timestamp       AS last_change_at,
         operation,
         ROW_NUMBER() OVER (PARTITION BY record_id
                            ORDER BY commit_timestamp DESC) AS rn
  FROM cold.cdc_events_archive
  WHERE source_table = 'orders'
)
WHERE rn = 1            -- newest event per order wins
  AND operation <> 'd' -- a delete as the latest event tombstones the row
ORDER BY last_change_at DESC
LIMIT 50;

-- Current order count by status, same dedup, as an aggregate sanity check.
-- Same two-level rule as above: JSON_PARSE to a SUPER alias in the innermost
-- subquery, dedup in the middle, navigate the field in the outer query - never
-- inline on the JSON_PARSE() result.
SELECT ed."status"::VARCHAR AS status, COUNT(*) AS orders
FROM (
  SELECT ed, rn, operation
  FROM (
    SELECT JSON_PARSE(event_data) AS ed,
           operation,
           ROW_NUMBER() OVER (PARTITION BY record_id
                              ORDER BY commit_timestamp DESC) AS rn
    FROM cold.cdc_events_archive
    WHERE source_table = 'orders'
  )
  WHERE rn = 1
    AND operation <> 'd'
)
GROUP BY ed."status"::VARCHAR
ORDER BY orders DESC;
