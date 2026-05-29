-- DSQL e-commerce schema
-- Source-of-truth operational database for an online store.
--
-- Design notes:
--   * UUIDs (gen_random_uuid) for all primary keys to distribute writes
--     across DSQL's storage nodes.
--   * No foreign keys: DSQL does not enforce them; application logic
--     maintains referential integrity.
--   * created_at / updated_at on every table for CDC ordering downstream.
--   * Async secondary indexes for common query paths.

-- Customers placing orders.
CREATE TABLE IF NOT EXISTS customers (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email       VARCHAR(255) NOT NULL,
    full_name   VARCHAR(200) NOT NULL,
    country     VARCHAR(2)   NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX ASYNC IF NOT EXISTS idx_customers_email
    ON customers (email);

-- Product catalog.
CREATE TABLE IF NOT EXISTS products (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku          VARCHAR(50)  NOT NULL,
    name         VARCHAR(200) NOT NULL,
    category     VARCHAR(100) NOT NULL,
    price_cents  BIGINT       NOT NULL,
    stock_qty    INT          NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX ASYNC IF NOT EXISTS idx_products_sku
    ON products (sku);

-- Order header (one row per checkout).
-- Lifecycle:
--   pending -> paid -> shipped -> delivered
--                 \-> cancelled
--   pending (abandoned, no further events)
CREATE TABLE IF NOT EXISTS orders (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id   UUID         NOT NULL,
    total_cents   BIGINT       NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'pending',
    payment_method VARCHAR(20),
    ship_country  VARCHAR(2)   NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX ASYNC IF NOT EXISTS idx_orders_customer
    ON orders (customer_id);

CREATE INDEX ASYNC IF NOT EXISTS idx_orders_status
    ON orders (status);

-- Order line items (one row per product in an order).
CREATE TABLE IF NOT EXISTS order_items (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID         NOT NULL,
    product_id      UUID         NOT NULL,
    quantity        INT          NOT NULL,
    unit_price_cents BIGINT      NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX ASYNC IF NOT EXISTS idx_order_items_order
    ON order_items (order_id);
