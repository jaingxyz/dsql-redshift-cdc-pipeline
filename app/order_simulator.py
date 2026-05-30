"""
E-commerce order simulator.

Generates realistic shopping activity against the DSQL schema:
  * Seeds a product catalog and customer base on first run.
  * Continuously creates orders with line items.
  * Updates orders through lifecycle states (pending -> paid -> shipped -> delivered).
  * Randomly abandons a small percentage of orders (stays "pending").
  * Occasionally cancels orders.

Every write produces a CDC event that flows through Kinesis -> Lambda -> Redshift.

Usage:
    python order_simulator.py --duration 300 --rate 5
        Run for 5 minutes at ~5 orders/sec.

    python order_simulator.py --duration 0 --rate 1
        Run indefinitely at ~1 order/sec, reconnecting on auth-token
        expiry. Used by the always-on Fargate deployment.

    python order_simulator.py --seed-only
        Just seed catalog and customers, then exit.

Requires:
    pip install boto3 psycopg[binary]
    aws-dsql connection details exported as env vars:
        DSQL_CLUSTER_ID, AWS_REGION (defaults: us-east-1)
"""

import argparse
import os
import random
import time
from contextlib import contextmanager
from dataclasses import dataclass

import boto3
import psycopg

REGION = os.environ.get("AWS_REGION", "us-east-1")
CLUSTER_ID = os.environ["DSQL_CLUSTER_ID"]

# Tunables
SEED_CUSTOMERS = 200
SEED_PRODUCTS = 50
ABANDON_RATE = 0.10  # 10% of orders never advance from pending
CANCEL_RATE = 0.05  # 5% of paid orders get cancelled
MAX_ITEMS_PER_ORDER = 4

CATEGORIES = ["apparel", "electronics", "home", "books", "toys", "beauty"]
COUNTRIES = ["US", "GB", "DE", "FR", "CA", "AU", "JP", "BR"]
PAYMENT_METHODS = ["card", "paypal", "applepay", "googlepay"]


@dataclass
class SimulatorState:
    customer_ids: list[str]
    product_catalog: list[dict]  # {id, price_cents, stock_qty}
    pending_orders: list[str]  # order ids awaiting payment
    paid_orders: list[str]  # order ids ready to ship
    shipped_orders: list[str]  # order ids in transit


# ---------------------------------------------------------------------------
# Connection management
#
# Aurora DSQL uses IAM auth tokens (not passwords) and enforces SSL. Per the
# DSQL operating limits:
#   - tokens expire after 15 minutes
#   - any single connection lives at most 60 minutes
#   - sslmode=verify-full is required to validate the server cert chain
#
# We mint a fresh token via boto3 on every (re)connect — no caching. The
# proactive-refresh window (CONN_REFRESH_S, default 14 min) is below the
# 15-min token cap so we cycle the connection BEFORE it errors out
# mid-statement. Reactive reconnect still exists in the outer loop in
# run_simulation() as a safety net for the 60-minute cap and any
# unexpected disconnect.
#
# Note: AWS publishes `aurora_dsql_psycopg` which automates this pattern.
# We do it manually here because this is sample code; the explicit
# token-mint + reconnect logic is the point.
# ---------------------------------------------------------------------------

# Refresh proactively at 14 min so a 15-min token never expires mid-query.
CONN_REFRESH_S = int(os.environ.get("CONN_REFRESH_S", 14 * 60))


def _generate_password() -> str:
    """Get a short-lived auth token from DSQL (admin role)."""
    client = boto3.client("dsql", region_name=REGION)
    return client.generate_db_connect_admin_auth_token(
        Hostname=f"{CLUSTER_ID}.dsql.{REGION}.on.aws",
        Region=REGION,
    )


@contextmanager
def dsql_connection():
    """
    Yield a psycopg connection to the DSQL cluster.

    sslmode=verify-full validates the server cert against a trust store.
    sslrootcert=system points libpq at the OS bundle (Amazon Root CA is
    in /etc/ssl/certs in python:3.11-slim). Without it, libpq looks for
    ~/.postgresql/root.crt and fails. `require` would skip verification.
    """
    conn = psycopg.connect(
        host=f"{CLUSTER_ID}.dsql.{REGION}.on.aws",
        port=5432,
        dbname="postgres",
        user="admin",
        password=_generate_password(),
        sslmode="verify-full",
        sslrootcert="system",
        autocommit=True,
    )
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


def seed_catalog(conn) -> list[dict]:
    """Insert product catalog if empty. Returns the catalog."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM products")
        if cur.fetchone()[0] >= SEED_PRODUCTS:
            cur.execute(
                "SELECT id, price_cents, stock_qty FROM products LIMIT %s",
                (SEED_PRODUCTS,),
            )
            return [
                {"id": str(r[0]), "price_cents": r[1], "stock_qty": r[2]}
                for r in cur.fetchall()
            ]

        catalog = []
        for i in range(SEED_PRODUCTS):
            sku = f"SKU-{i:05d}"
            name = f"Sample Product {i}"
            category = random.choice(CATEGORIES)
            price = random.randint(500, 50_000)  # $5 to $500
            stock = random.randint(50, 1000)
            cur.execute(
                """
                INSERT INTO products (sku, name, category, price_cents, stock_qty)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (sku, name, category, price, stock),
            )
            row_id = cur.fetchone()[0]
            catalog.append(
                {"id": str(row_id), "price_cents": price, "stock_qty": stock}
            )
        print(f"Seeded {SEED_PRODUCTS} products")
        return catalog


def seed_customers(conn) -> list[str]:
    """Insert customers if empty. Returns list of customer ids."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customers")
        if cur.fetchone()[0] >= SEED_CUSTOMERS:
            cur.execute("SELECT id FROM customers LIMIT %s", (SEED_CUSTOMERS,))
            return [str(r[0]) for r in cur.fetchall()]

        ids = []
        for i in range(SEED_CUSTOMERS):
            email = f"user{i}@example.com"
            name = f"User {i}"
            country = random.choice(COUNTRIES)
            cur.execute(
                """
                INSERT INTO customers (email, full_name, country)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (email, name, country),
            )
            ids.append(str(cur.fetchone()[0]))
        print(f"Seeded {SEED_CUSTOMERS} customers")
        return ids


# ---------------------------------------------------------------------------
# Order lifecycle actions
# ---------------------------------------------------------------------------


def create_order(conn, state: SimulatorState) -> None:
    """Place a new pending order with 1-4 items."""
    customer_id = random.choice(state.customer_ids)
    items = random.sample(
        state.product_catalog, k=random.randint(1, MAX_ITEMS_PER_ORDER)
    )

    total = sum(p["price_cents"] for p in items)
    country = random.choice(COUNTRIES)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO orders (customer_id, total_cents, status, ship_country)
            VALUES (%s, %s, 'pending', %s)
            RETURNING id
            """,
            (customer_id, total, country),
        )
        order_id = str(cur.fetchone()[0])

        for product in items:
            cur.execute(
                """
                INSERT INTO order_items
                  (order_id, product_id, quantity, unit_price_cents)
                VALUES (%s, %s, %s, %s)
                """,
                (order_id, product["id"], 1, product["price_cents"]),
            )

    state.pending_orders.append(order_id)


def pay_order(conn, state: SimulatorState) -> None:
    """Move a pending order to paid, unless it's an abandoned cart."""
    if not state.pending_orders:
        return
    order_id = state.pending_orders.pop(0)

    if random.random() < ABANDON_RATE:
        # Cart abandoned - leave in pending forever
        return

    method = random.choice(PAYMENT_METHODS)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE orders
               SET status = 'paid',
                   payment_method = %s,
                   updated_at = NOW()
             WHERE id = %s
            """,
            (method, order_id),
        )
    state.paid_orders.append(order_id)


def ship_order(conn, state: SimulatorState) -> None:
    """Move a paid order to shipped, or cancel a small fraction."""
    if not state.paid_orders:
        return
    order_id = state.paid_orders.pop(0)

    if random.random() < CANCEL_RATE:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = 'cancelled', updated_at = NOW() WHERE id = %s",
                (order_id,),
            )
        return

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE orders SET status = 'shipped', updated_at = NOW() WHERE id = %s",
            (order_id,),
        )
    state.shipped_orders.append(order_id)


def deliver_order(conn, state: SimulatorState) -> None:
    """Move a shipped order to delivered."""
    if not state.shipped_orders:
        return
    order_id = state.shipped_orders.pop(0)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE orders SET status = 'delivered', updated_at = NOW() WHERE id = %s",
            (order_id,),
        )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def run_simulation(duration_sec: int, target_rate: float) -> None:
    """
    Drive the simulator at `target_rate` ops/sec for `duration_sec` seconds.

    `duration_sec <= 0` means run indefinitely (intended for the always-on
    Fargate deployment in cloudformation-simulator.yaml). In that mode the
    loop reconnects to DSQL when its auth token expires (~15 min) instead
    of exiting.
    """
    sleep_interval = 1.0 / target_rate if target_rate > 0 else 1.0
    forever = duration_sec <= 0

    # Action mix - more creations than later-stage transitions so the funnel
    # narrows naturally.
    actions = (
        [create_order] * 50 + [pay_order] * 30 + [ship_order] * 12 + [deliver_order] * 8
    )

    state = SimulatorState(
        customer_ids=[],
        product_catalog=[],
        pending_orders=[],
        paid_orders=[],
        shipped_orders=[],
    )

    start = time.time()
    op_count = 0
    last_log = start

    # Outer loop = one DSQL connection's lifetime. We proactively cycle
    # the connection every CONN_REFRESH_S seconds (default 14 min) so the
    # 15-min token cap never expires mid-statement. The reactive
    # OperationalError handler at the bottom is a safety net for the
    # 60-min hard connection cap and any unexpected disconnect.
    while True:
        conn_opened_at = time.time()
        try:
            with dsql_connection() as conn:
                if not state.product_catalog:
                    state.product_catalog = seed_catalog(conn)
                if not state.customer_ids:
                    state.customer_ids = seed_customers(conn)

                while forever or time.time() - start < duration_sec:
                    # Proactive token refresh: cycle the connection before
                    # the 15-min token expires. This avoids in-flight
                    # OperationalErrors that the safety-net path below
                    # would handle reactively.
                    if time.time() - conn_opened_at >= CONN_REFRESH_S:
                        break

                    action = random.choice(actions)
                    try:
                        action(conn, state)
                        op_count += 1
                    except psycopg.OperationalError:
                        # Connection-level (60-min cap, network blip,
                        # unexpected disconnect). Re-raise to the outer
                        # except so we reconnect with a fresh token.
                        raise
                    except psycopg.Error as e:
                        # Statement-level (constraint violation, deadlock).
                        # Don't kill the run; log and continue.
                        print(f"DB error (continuing): {e}")

                    now = time.time()
                    if now - last_log >= 10:
                        elapsed = now - start
                        print(
                            f"[{elapsed:8.1f}s] ops={op_count:7d} "
                            f"pending={len(state.pending_orders):4d} "
                            f"paid={len(state.paid_orders):4d} "
                            f"shipped={len(state.shipped_orders):4d}"
                        )
                        last_log = now

                    time.sleep(sleep_interval)
        except psycopg.OperationalError as e:
            if not forever:
                print(f"DB error: {e} -- ending simulation")
                break
            print(f"DB connection lost ({e}); reconnecting in 2s...")
            time.sleep(2)
            continue

        # We get here either because:
        #   - bounded mode reached end of duration cleanly, or
        #   - forever mode hit the proactive-refresh window.
        # Bounded mode exits; forever mode loops to mint a fresh token.
        if not forever:
            break

    print(f"Done. Total operations: {op_count}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help=(
            "How long to run in seconds (default: 60). "
            "Pass 0 or negative to run indefinitely with auto-reconnect "
            "on DSQL auth-token expiry (used by the Fargate deployment)."
        ),
    )
    parser.add_argument(
        "--rate", type=float, default=5.0, help="Target ops/sec (default: 5)"
    )
    parser.add_argument(
        "--seed-only", action="store_true", help="Seed catalog and customers, then exit"
    )
    args = parser.parse_args()

    if args.seed_only:
        with dsql_connection() as conn:
            seed_catalog(conn)
            seed_customers(conn)
        return

    run_simulation(args.duration, args.rate)


if __name__ == "__main__":
    main()
