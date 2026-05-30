-- Redshift target schema for the e-commerce CDC pipeline.
--
-- Pattern: append-only event log + materialized "current state" views.
-- Why this pattern?
--   * DSQL CDC delivers events UNORDERED. Appending is safe; in-place updates
--     are not.
--   * In public preview, both INSERT and UPDATE arrive as op="c", so we
--     cannot distinguish them at write time. Window functions on the event
--     log dedupe by primary key + commit timestamp.
--   * The SUPER type lets the same table absorb schema drift from any
--     source table without DDL changes.

-- 1. Append-only CDC event log.
-- Every Kinesis record produces one row here.
CREATE TABLE IF NOT EXISTS cdc_events (
    event_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_table      VARCHAR(100) NOT NULL,
    operation         VARCHAR(10)  NOT NULL,           -- "c" or "d"
    record_id         VARCHAR(50)  NOT NULL,           -- source row primary key
    event_data        SUPER,                            -- full row state for "c", PK only for "d"
    commit_timestamp  TIMESTAMP    NOT NULL,            -- DSQL commit time (root ts_ms)
    ingested_at       TIMESTAMP    NOT NULL DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (record_id)
SORTKEY (source_table, commit_timestamp);

-- Grant INSERT to PUBLIC so the Lambda's IAM-mapped database user (which
-- is auto-created on first redshift-serverless:GetCredentials call) can
-- write here. SELECT is also granted so any authenticated user can query
-- the event log and the *_current views below.
-- For tighter control in production, grant to a specific role created
-- to match the Lambda's IAM identity (typically "IAMR:<role-name>") and
-- drop the PUBLIC grant.
GRANT INSERT, SELECT ON cdc_events TO PUBLIC;

-- 2. Current-state views per source table.
-- Pick the latest event per record_id; treat "d" as a tombstone.

CREATE OR REPLACE VIEW orders_current AS
SELECT
    record_id                            AS order_id,
    event_data."customer_id"::VARCHAR    AS customer_id,
    event_data."total_cents"::BIGINT     AS total_cents,
    event_data."status"::VARCHAR         AS status,
    event_data."payment_method"::VARCHAR AS payment_method,
    event_data."ship_country"::VARCHAR   AS ship_country,
    commit_timestamp                     AS last_change_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM cdc_events
    WHERE source_table = 'orders'
)
WHERE rn = 1
  AND operation <> 'd';

CREATE OR REPLACE VIEW customers_current AS
SELECT
    record_id                          AS customer_id,
    event_data."email"::VARCHAR        AS email,
    event_data."full_name"::VARCHAR    AS full_name,
    event_data."country"::VARCHAR      AS country,
    commit_timestamp                   AS last_change_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM cdc_events
    WHERE source_table = 'customers'
)
WHERE rn = 1
  AND operation <> 'd';

CREATE OR REPLACE VIEW products_current AS
SELECT
    record_id                         AS product_id,
    event_data."sku"::VARCHAR         AS sku,
    event_data."name"::VARCHAR        AS name,
    event_data."category"::VARCHAR    AS category,
    event_data."price_cents"::BIGINT  AS price_cents,
    event_data."stock_qty"::INT       AS stock_qty,
    commit_timestamp                  AS last_change_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM cdc_events
    WHERE source_table = 'products'
)
WHERE rn = 1
  AND operation <> 'd';

CREATE OR REPLACE VIEW order_items_current AS
SELECT
    record_id                              AS order_item_id,
    event_data."order_id"::VARCHAR         AS order_id,
    event_data."product_id"::VARCHAR       AS product_id,
    event_data."quantity"::INT             AS quantity,
    event_data."unit_price_cents"::BIGINT  AS unit_price_cents,
    commit_timestamp                       AS last_change_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY record_id
               ORDER BY commit_timestamp DESC
           ) AS rn
    FROM cdc_events
    WHERE source_table = 'order_items'
)
WHERE rn = 1
  AND operation <> 'd';

-- Grant SELECT on the current-state views to PUBLIC, mirroring the
-- cdc_events grant above. This makes the views queryable by any
-- federated DB user a tool (Lambda, SageMaker Studio's project IAM
-- identity, BI tools) is auto-created as on first connection.
-- Tighten in production by granting to a specific role and dropping
-- the PUBLIC grant.
GRANT SELECT ON
    orders_current,
    customers_current,
    products_current,
    order_items_current
TO PUBLIC;
