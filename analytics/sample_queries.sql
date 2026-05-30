-- Sample analytical queries for the e-commerce CDC pipeline.
-- These run on Redshift against the cdc_events table and the *_current views.
-- Each section maps to a use case from the blog post.

-- =============================================================================
-- 1. REAL-TIME SALES DASHBOARD
-- =============================================================================

-- Sales velocity: orders per minute over the last hour.
SELECT
    DATE_TRUNC('minute', last_change_at) AS minute,
    COUNT(*)                              AS orders,
    SUM(total_cents) / 100.0              AS revenue_dollars
FROM orders_current
WHERE status IN ('paid', 'shipped', 'delivered')
  AND last_change_at >= GETDATE() - INTERVAL '1 hour'
GROUP BY 1
ORDER BY 1 DESC;

-- Top selling products today.
SELECT
    p.name,
    p.category,
    SUM(oi.quantity)                              AS units_sold,
    SUM(oi.quantity * oi.unit_price_cents) / 100.0 AS revenue_dollars
FROM order_items_current oi
JOIN products_current    p  ON p.product_id = oi.product_id
JOIN orders_current      o  ON o.order_id   = oi.order_id
WHERE o.status IN ('paid', 'shipped', 'delivered')
  AND o.last_change_at >= TRUNC(GETDATE())
GROUP BY p.name, p.category
ORDER BY units_sold DESC
LIMIT 20;

-- Conversion funnel: how do orders distribute across lifecycle states?
SELECT
    status,
    COUNT(*)                          AS orders,
    SUM(total_cents) / 100.0          AS dollars,
    ROUND(100.0 * COUNT(*) /
          SUM(COUNT(*)) OVER (), 2)   AS pct_of_total
FROM orders_current
WHERE last_change_at >= GETDATE() - INTERVAL '24 hours'
GROUP BY status
ORDER BY orders DESC;


-- =============================================================================
-- 2. FRAUD DETECTION SIGNALS
-- =============================================================================

-- High-value orders from new customers in the last hour.
-- Cross-checks orders_current against the customer creation timestamp.
WITH new_customers AS (
    SELECT customer_id, last_change_at AS signed_up_at
    FROM customers_current
    WHERE last_change_at >= GETDATE() - INTERVAL '1 day'
)
SELECT
    o.order_id,
    o.customer_id,
    o.total_cents / 100.0       AS dollars,
    o.ship_country,
    n.signed_up_at,
    o.last_change_at
FROM orders_current o
JOIN new_customers   n ON n.customer_id = o.customer_id
WHERE o.status = 'paid'
  AND o.total_cents >= 50000              -- > $500
  AND o.last_change_at >= GETDATE() - INTERVAL '1 hour'
ORDER BY o.total_cents DESC;

-- Customers with an unusual burst of orders (potential card testing).
SELECT
    customer_id,
    COUNT(*)                  AS orders_last_hour,
    SUM(total_cents) / 100.0  AS dollars,
    LISTAGG(DISTINCT ship_country, ',') WITHIN GROUP (ORDER BY ship_country)
                              AS countries
FROM orders_current
WHERE last_change_at >= GETDATE() - INTERVAL '1 hour'
GROUP BY customer_id
HAVING COUNT(*) >= 5
ORDER BY orders_last_hour DESC;


-- =============================================================================
-- 3. INVENTORY MANAGEMENT
-- =============================================================================

-- Products selling faster than usual: orders in last hour vs prior 24h baseline.
WITH last_hour AS (
    SELECT oi.product_id, SUM(oi.quantity) AS qty_last_hour
    FROM order_items_current oi
    JOIN orders_current      o  ON o.order_id = oi.order_id
    WHERE o.status IN ('paid', 'shipped', 'delivered')
      AND o.last_change_at >= GETDATE() - INTERVAL '1 hour'
    GROUP BY oi.product_id
),
baseline AS (
    SELECT oi.product_id,
           SUM(oi.quantity) / 24.0 AS avg_qty_per_hour
    FROM order_items_current oi
    JOIN orders_current      o  ON o.order_id = oi.order_id
    WHERE o.status IN ('paid', 'shipped', 'delivered')
      AND o.last_change_at >= GETDATE() - INTERVAL '25 hours'
      AND o.last_change_at <  GETDATE() - INTERVAL '1 hour'
    GROUP BY oi.product_id
)
SELECT
    p.sku,
    p.name,
    p.stock_qty                   AS current_stock,
    b.avg_qty_per_hour            AS baseline_per_hr,
    h.qty_last_hour               AS last_hour,
    ROUND(h.qty_last_hour / NULLIF(b.avg_qty_per_hour, 0), 2)
                                  AS surge_multiplier
FROM last_hour       h
JOIN baseline        b ON b.product_id = h.product_id
JOIN products_current p ON p.product_id = h.product_id
WHERE h.qty_last_hour >= 5
  AND h.qty_last_hour > 2 * b.avg_qty_per_hour
ORDER BY surge_multiplier DESC;

-- Low-stock alerts: products with less than 1 hour of inventory at current rate.
WITH velocity AS (
    SELECT oi.product_id, SUM(oi.quantity) AS qty_last_hour
    FROM order_items_current oi
    JOIN orders_current      o  ON o.order_id = oi.order_id
    WHERE o.status IN ('paid', 'shipped', 'delivered')
      AND o.last_change_at >= GETDATE() - INTERVAL '1 hour'
    GROUP BY oi.product_id
)
SELECT
    p.sku,
    p.name,
    p.stock_qty                                  AS current_stock,
    v.qty_last_hour                              AS hourly_rate,
    ROUND(p.stock_qty * 1.0 / NULLIF(v.qty_last_hour, 0), 2)
                                                 AS hours_of_inventory
FROM products_current p
JOIN velocity         v ON v.product_id = p.product_id
WHERE p.stock_qty < v.qty_last_hour
ORDER BY hours_of_inventory ASC;


-- =============================================================================
-- 4. CART ABANDONMENT RECOVERY
-- =============================================================================

-- Pending orders older than 30 minutes - eligible for recovery email.
SELECT
    o.order_id,
    c.email,
    c.full_name,
    o.total_cents / 100.0          AS dollars,
    o.last_change_at               AS placed_at,
    DATEDIFF(minute, o.last_change_at, GETDATE())
                                   AS minutes_pending
FROM orders_current     o
JOIN customers_current  c ON c.customer_id = o.customer_id
WHERE o.status = 'pending'
  AND o.last_change_at <= GETDATE() - INTERVAL '30 minutes'
  AND o.last_change_at >= GETDATE() - INTERVAL '24 hours'
ORDER BY o.total_cents DESC
LIMIT 100;


-- =============================================================================
-- 5. CUSTOMER LIFETIME VALUE (LTV) BY COUNTRY
-- =============================================================================

SELECT
    c.country,
    COUNT(DISTINCT c.customer_id)                  AS customers,
    COUNT(DISTINCT o.order_id)                     AS orders,
    SUM(o.total_cents) / 100.0                     AS gross_revenue_dollars,
    AVG(o.total_cents) / 100.0                     AS avg_order_value_dollars
FROM customers_current  c
LEFT JOIN orders_current o
    ON o.customer_id = c.customer_id
    AND o.status IN ('paid', 'shipped', 'delivered')
GROUP BY c.country
ORDER BY gross_revenue_dollars DESC NULLS LAST;


-- =============================================================================
-- 6. PIPELINE HEALTH (operational checks on the CDC pipeline itself)
-- =============================================================================

-- CDC propagation latency in the last 5 minutes (commit -> ingest).
SELECT
    source_table,
    COUNT(*)                                                 AS events,
    AVG(DATEDIFF(millisecond, commit_timestamp, ingested_at)) AS avg_latency_ms,
    MAX(DATEDIFF(millisecond, commit_timestamp, ingested_at)) AS max_latency_ms
FROM cdc_events
WHERE ingested_at >= GETDATE() - INTERVAL '5 minutes'
GROUP BY source_table
ORDER BY events DESC;

-- Event volume per minute - spot gaps that suggest pipeline stalls.
SELECT
    DATE_TRUNC('minute', ingested_at) AS minute,
    source_table,
    COUNT(*)                          AS events
FROM cdc_events
WHERE ingested_at >= GETDATE() - INTERVAL '1 hour'
GROUP BY 1, 2
ORDER BY 1 DESC, source_table;
