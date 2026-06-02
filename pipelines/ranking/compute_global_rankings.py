#!/usr/bin/env python3
"""Compute global gain rankings from rolled gains table."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

try:
    import psycopg
except ImportError:
    import psycopg2 as psycopg  # type: ignore


def connect():
    return psycopg.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "arb_demo"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASS", os.getenv("POSTGRES_PASSWORD", "postgres")),
    )


def rank_sell_gain_30s_plus(cur) -> list[dict]:
    cur.execute(
        """
        WITH pairs AS (
            SELECT symbol, src_exchange, ref_exchange,
                   COUNT(*) FILTER (WHERE side = 'ASK' AND avg_gain_pct_30s > 0) AS pos_ask,
                   MAX(avg_gain_pct_30s) FILTER (WHERE side = 'ASK') AS max_ask_gain
            FROM market_data.ob_arb_rolling_gain_30s
            WHERE ts >= NOW() - INTERVAL '2 minutes'
            GROUP BY symbol, src_exchange, ref_exchange
        )
        SELECT symbol, src_exchange, ref_exchange, max_ask_gain
        FROM pairs
        WHERE pos_ask >= 2
        ORDER BY max_ask_gain DESC NULLS LAST
        LIMIT 50
        """
    )
    return [
        {
            "symbol": r[0],
            "src_exchange": r[1],
            "ref_exchange": r[2],
            "max_ask_gain_pct": float(r[3]) if r[3] is not None else None,
        }
        for r in cur.fetchall()
    ]


def rank_max_single_gain_24h(cur) -> dict:
    cur.execute(
        """
        SELECT side, symbol, src_exchange, ref_exchange, avg_gain_pct_30s, ts
        FROM market_data.ob_arb_rolling_gain_30s
        WHERE ts >= NOW() - INTERVAL '24 hours'
        ORDER BY avg_gain_pct_30s DESC NULLS LAST
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        return {}
    return {
        "side": row[0],
        "symbol": row[1],
        "src_exchange": row[2],
        "ref_exchange": row[3],
        "gain_pct": float(row[4]) if row[4] is not None else None,
        "ts": row[5].isoformat() if row[5] else None,
    }


def rank_multi_exchange_coins(cur) -> list[dict]:
    cur.execute(
        """
        WITH latest AS (
            SELECT DISTINCT ON (symbol, src_exchange, side)
                symbol, src_exchange, side, avg_gain_pct_30s
            FROM market_data.ob_arb_rolling_gain_30s
            WHERE ts >= NOW() - INTERVAL '15 minutes'
            ORDER BY symbol, src_exchange, side, ts DESC
        ),
        coin_map AS (
            SELECT symbol,
                   COALESCE(base_curr, regexp_replace(symbol, '(USDT|USDC)$', '')) AS coin,
                   src_exchange,
                   MAX(avg_gain_pct_30s) AS max_gain
            FROM latest l
            JOIN manager.symbol s ON s.symbol = l.symbol AND s.exchange = l.src_exchange
            GROUP BY 1, 2, 3
        )
        SELECT coin,
               COUNT(DISTINCT src_exchange) AS exchange_count,
               json_agg(json_build_object('exchange', src_exchange, 'symbol', symbol, 'max_gain_pct', max_gain)
                        ORDER BY max_gain DESC) AS instruments
        FROM coin_map
        WHERE max_gain IS NOT NULL AND max_gain > 0
        GROUP BY coin
        HAVING COUNT(DISTINCT src_exchange) >= 2
        ORDER BY exchange_count DESC, MAX(max_gain) DESC
        LIMIT 30
        """
    )
    out = []
    for coin, cnt, instruments in cur.fetchall():
        out.append({"coin": coin, "exchange_count": cnt, "instruments": instruments})
    return out


def store_ranking(cur, rank_type: str, payload) -> None:
    cur.execute(
        """
        INSERT INTO market_data.global_gain_rankings (ts, rank_type, payload)
        VALUES (NOW(), %s, %s::jsonb)
        """,
        (rank_type, json.dumps(payload)),
    )


def main() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            store_ranking(cur, "sell_gain_30s_plus", rank_sell_gain_30s_plus(cur))
            store_ranking(cur, "max_single_gain_24h", rank_max_single_gain_24h(cur))
            store_ranking(cur, "multi_exchange_coin_spread", rank_multi_exchange_coins(cur))
            cur.execute("SELECT market_data.prune_global_rankings(%s)", (48,))
        conn.commit()
    print(f"[ranking] updated at {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
