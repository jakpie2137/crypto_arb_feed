#!/usr/bin/env python3
"""Prune old market_data rows according to demo-config retention."""

from __future__ import annotations

import os

try:
    import psycopg
except ImportError:
    import psycopg2 as psycopg  # type: ignore


def main() -> None:
    dense_min = int(os.getenv("DENSE_SNAPSHOT_RETENTION_MINUTES", "15"))
    rolled_h = int(os.getenv("ROLLED_GAINS_RETENTION_HOURS", "24"))

    conn = psycopg.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "arb_demo"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASS", os.getenv("POSTGRES_PASSWORD", "postgres")),
    )
    with conn.cursor() as cur:
        cur.execute("SELECT market_data.prune_orderbook_snapshots(%s)", (dense_min,))
        d1 = cur.fetchone()[0]
        cur.execute("SELECT market_data.prune_rolled_gains(%s)", (rolled_h,))
        d2 = cur.fetchone()[0]
        cur.execute("SELECT market_data.prune_global_rankings(%s)", (48,))
        d3 = cur.fetchone()[0]
    conn.commit()
    conn.close()
    print(f"Pruned snapshots={d1}, rolled_gains={d2}, rankings={d3}")


if __name__ == "__main__":
    main()
