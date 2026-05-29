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
import uuid
from contextlib import contextmanager
from dataclasses import dataclass

import boto3
import psycopg
from psycopg import sql

REGION = os.environ.get("AWS_REGION", "us-east-1")
CLUSTER_ID = os.environ["DSQL_CLUSTER_ID"]

# Tunables
SEED_CUSTOMERS = 200
SEED_PRODUCTS = 50
ABANDON_RATE = 0.10        # 10% of orders never advance from pending
CANCEL_RATE = 0.05         # 5% of paid orders get cancelled
MAX_ITEMS_PER_ORDER = 4

CATEGORIES = ["apparel", "electronics", "home", "books", "toys", "beauty"]
COUNTRIES = ["US", "GB", "DE", "FR", "CA", "AU", "JP", "BR"]
PAYMENT_METHODS = ["card", "paypal", "applepay", "googlepay"]


@dataclass
class SimulatorState:
    customer_ids: list[str]
    product_catalog: list[dict]   # {id, price_cents, stock_qty}
    pending_orders: list[str]     # order ids awaiting payment
    paid_orders: list[str]        # order ids ready to ship
    shipped_orders: list[str]     # order ids in transit


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _generate_password() -> str:
    """Get a short-lived auth token from DSQL."""
    client = boto3.client("dsql", region_name=REGION)
    return client.generate_db_connect_admin_auth_token(
        Hostname=f"{CLUSTER_ID}.dsql.{REGION}.on.aws",
        Region=REGION,
    )


@contextmanager
def dsql_connection():
    """Yield a psycopg connection to the DSQL cluster."""
    conn = psycopg.connect(
        host=f"{CLUSTER_ID}.dsql.{REGION}.on.aws",
        port=5432,
        dbname="postgres",
        user="admin",
        password=_generate_password(),
        sslmode="require",
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
            cur.execute("SELECT id, price_cents, stock_qty FROM products LIMIT %s",
                        (SEED_PRODUCTS,))
            return [
                {"id": str(r[0]), "price_cents": r[1], "stock_qty": r[2]}
                for r in cur.fetchall()
            ]

        catalog = []
        for i in range(SEED_PRODUCTS):
            sku = f"SKU-{i:05d}"
            name = f"Sample Product {i}"
            category = random.choice(CATEGORIES)
            price = random.randint(500, 50_000)   # $5 to $500
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
            catalog.append({"id": str(row_id), "price_cents": price, "stock_qty": stock})
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
    items = random.sample(state.product_catalog,
                          k=random.randint(1, MAX_ITEMS_PER_ORDER))

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
    sleep_interval = 1.0 / target_rate if target_rate > 0 else 1.0

    with dsql_connection() as conn:
        catalog = seed_catalog(conn)
        customer_ids = seed_customers(conn)

        state = SimulatorState(
            customer_ids=customer_ids,
            product_catalog=catalog,
            pending_orders=[],
            paid_orders=[],
            shipped_orders=[],
        )

        # Action mix - more creations than later-stage transitions
        # so the funnel narrows naturally.
        actions = (
            [create_order] * 50
            + [pay_order] * 30
            + [ship_order] * 12
            + [deliver_order] * 8
        )

        start = time.time()
        op_count = 0
        last_log = start

        while time.time() - start < duration_sec:
            action = random.choice(actions)
            try:
                action(conn, state)
                op_count += 1
            except psycopg.Error as e:
                # DSQL auth tokens expire after ~15 minutes; a long-running
                # simulation will hit token expiry and end here. Re-run the
                # script (or wrap the main loop with a reconnect helper)
                # for runs longer than 15 minutes.
                print(f"DB error: {e} -- ending simulation")
                break

            now = time.time()
            if now - last_log >= 10:
                elapsed = now - start
                print(
                    f"[{elapsed:6.1f}s] ops={op_count:5d} "
                    f"pending={len(state.pending_orders):4d} "
                    f"paid={len(state.paid_orders):4d} "
                    f"shipped={len(state.shipped_orders):4d}"
                )
                last_log = now

            time.sleep(sleep_interval)

        print(f"Done. Total operations: {op_count}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=60,
                        help="How long to run in seconds (default: 60)")
    parser.add_argument("--rate", type=float, default=5.0,
                        help="Target ops/sec (default: 5)")
    parser.add_argument("--seed-only", action="store_true",
                        help="Seed catalog and customers, then exit")
    args = parser.parse_args()

    if args.seed_only:
        with dsql_connection() as conn:
            seed_catalog(conn)
            seed_customers(conn)
        return

    run_simulation(args.duration, args.rate)


if __name__ == "__main__":
    main()
