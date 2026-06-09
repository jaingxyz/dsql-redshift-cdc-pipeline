-- Hot + cold unified view layer for the CDC pipeline.
--
-- Two paths write the SAME Kinesis CDC stream into two stores:
--   * HOT  - the cdc_processor Lambda appends to local `cdc_events`
--            (SUPER event_data). Fast, full Redshift performance, but we
--            only keep it small / recent.
--   * COLD - Firehose appends to the S3 Tables Iceberg table, surfaced
--            in Redshift as the external schema `cold.cdc_events_archive`
--            (event_data is a JSON *string*; JSON_PARSE recovers SUPER).
--            Cheap, durable, unbounded retention, slower to scan.
--
-- Because both paths tee off the same stream, an event can appear in
-- BOTH stores. The append-only + latest-commit-timestamp reconstruction
-- (same pattern as redshift_schema.sql) is idempotent under that
-- duplication: deduping by record_id and keeping the row with the
-- greatest commit_timestamp yields the correct current state no matter
-- how many copies of an event exist or which store they came from.
--
-- Prereqs:
--   * schema/redshift_schema.sql loaded (cdc_events + *_current views).
--   * infra/scripts/07-deploy-iceberg.sh run (creates external schema
--     `cold` over the Iceberg table).
--
-- Run as the Redshift admin (the external schema and JSON_PARSE both
-- require it). The deploy script points you here as the final step.

-- ---------------------------------------------------------------------------
-- 1. Unified append-only event log across both stores.
--
-- Normalizes hot and cold into one shape (event_data as SUPER) and tags
-- each row with its origin so you can see the hot/cold split. This is
-- still an *event log* (one row per CDC event, duplicates included), not
-- current state - the *_unified views below collapse it.
--
-- The hot side prunes to the recent window and the cold side to the
-- older window so the two stores don't double-scan the overlap on every
-- query. HOT_WINDOW_HOURS is the retention horizon you intend to keep in
-- cdc_events; 24h is the default the pipeline is documented around. The
-- boundary is a scan-pruning optimization, not a correctness boundary:
-- if it is slightly off, deduplication still yields the right answer
-- because any event present in both stores is collapsed by record_id.
--
-- TODO(retention coupling): the 24-hour literal appears in two WHERE
-- clauses below and in cloudformation-tiering.yaml's RetentionHours
-- parameter. If you change the tiering retention you MUST update both
-- clauses below - Redshift doesn't support parameterized DDL for views.
-- Forgetting to update one direction silently widens or narrows the
-- visible window; correctness is preserved by dedup but cost and freshness
-- shift unexpectedly. A future pass could move this to a single source
-- of truth (e.g. CFN parameter -> stored procedure -> view body), but
-- for the demo it's loud-enough as a literal with this comment guarding it.
-- ---------------------------------------------------------------------------
-- WITH NO SCHEMA BINDING: required for any view that references an
-- external (Spectrum / federated catalog) table. Without it Redshift
-- rejects with "External tables are not supported in views". The cost
-- is that the view stops blocking schema changes on cdc_events_archive
-- - fine here because the Iceberg schema is fixed by the deploy script.
-- All downstream views (orders_unified etc.) reference cdc_events_all,
-- so they each need NO SCHEMA BINDING too.
CREATE OR REPLACE VIEW cdc_events_all AS
SELECT
    source_table,
    operation,
    record_id,
    event_data,                                  -- already SUPER
    commit_timestamp,
    ingested_at,
    'hot'::VARCHAR(4) AS source_store
FROM public.cdc_events
WHERE commit_timestamp >= DATEADD(hour, -24, GETDATE())
UNION ALL
SELECT
    source_table,
    operation,
    record_id,
    JSON_PARSE(event_data) AS event_data,        -- string -> SUPER
    commit_timestamp,
    ingested_at,
    'cold'::VARCHAR(4) AS source_store
FROM cold.cdc_events_archive
WHERE commit_timestamp < DATEADD(hour, -24, GETDATE())
WITH NO SCHEMA BINDING;

-- ---------------------------------------------------------------------------
-- 2. Current-state views over the unified log.
--
-- Identical reconstruction to redshift_schema.sql's *_current views, but
-- sourced from cdc_events_all so they reflect the full history (hot +
-- cold) instead of only what is still in cdc_events. Latest event per
-- record_id wins; an 'd' (delete) as the latest event tombstones the row.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW orders_unified AS
SELECT
    record_id                            AS order_id,
    event_data."customer_id"::VARCHAR    AS customer_id,
    event_data."total_cents"::BIGINT     AS total_cents,
    event_data."status"::VARCHAR         AS status,
    event_data."payment_method"::VARCHAR AS payment_method,
    event_data."ship_country"::VARCHAR   AS ship_country,
    commit_timestamp                     AS last_change_at,
    source_store
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM public.cdc_events_all
    WHERE source_table = 'orders'
)
WHERE rn = 1
  AND operation <> 'd'
WITH NO SCHEMA BINDING;

CREATE OR REPLACE VIEW customers_unified AS
SELECT
    record_id                          AS customer_id,
    event_data."email"::VARCHAR        AS email,
    event_data."full_name"::VARCHAR    AS full_name,
    event_data."country"::VARCHAR      AS country,
    commit_timestamp                   AS last_change_at,
    source_store
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM public.cdc_events_all
    WHERE source_table = 'customers'
)
WHERE rn = 1
  AND operation <> 'd'
WITH NO SCHEMA BINDING;

CREATE OR REPLACE VIEW products_unified AS
SELECT
    record_id                         AS product_id,
    event_data."sku"::VARCHAR         AS sku,
    event_data."name"::VARCHAR        AS name,
    event_data."category"::VARCHAR    AS category,
    event_data."price_cents"::BIGINT  AS price_cents,
    event_data."stock_qty"::INT       AS stock_qty,
    commit_timestamp                  AS last_change_at,
    source_store
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM public.cdc_events_all
    WHERE source_table = 'products'
)
WHERE rn = 1
  AND operation <> 'd'
WITH NO SCHEMA BINDING;

CREATE OR REPLACE VIEW order_items_unified AS
SELECT
    record_id                              AS order_item_id,
    event_data."order_id"::VARCHAR         AS order_id,
    event_data."product_id"::VARCHAR       AS product_id,
    event_data."quantity"::INT             AS quantity,
    event_data."unit_price_cents"::BIGINT  AS unit_price_cents,
    commit_timestamp                       AS last_change_at,
    source_store
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM public.cdc_events_all
    WHERE source_table = 'order_items'
)
WHERE rn = 1
  AND operation <> 'd'
WITH NO SCHEMA BINDING;

-- Mirror the PUBLIC grants from redshift_schema.sql so the same federated
-- users (Lambda, SageMaker, BI tools) can read the unified layer.
--
-- Demo trade-off: granting to PUBLIC means any current OR future
-- database user / federated identity in this Redshift namespace
-- automatically gets read on the full CDC dataset (operational PII
-- included). Acceptable here because the demo's user model is "one
-- admin + occasional federated reader". For production: replace
-- with a dedicated read-only role (e.g. cdc_reader), GRANT SELECT
-- to that role, and have BI tools assume it via redshift-serverless
-- federated auth. Don't widen this without thinking about who
-- inherits the access.
GRANT SELECT ON
    orders_unified,
    customers_unified,
    products_unified,
    order_items_unified
TO PUBLIC;
