#!/usr/bin/env python3
"""Bootstrap manager.symbol rows from conf/feeds/*.yaml (no API keys required)."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Install PyYAML: pip install pyyaml psycopg[binary]", file=sys.stderr)
    sys.exit(1)

try:
    import psycopg
except ImportError:
    import psycopg2 as psycopg  # type: ignore

DEMO_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
    "LTCUSDT", "BCHUSDT", "UNIUSDT", "ATOMUSDT", "APTUSDT",
    "ARBUSDT", "OPUSDT", "NEARUSDT", "INJUSDT", "SUIUSDT",
    "FILUSDT", "TIAUSDT", "WIFUSDT", "PEPEUSDT", "MATICUSDT",
]

FEED_TO_EXCHANGE = {
    "binancefut.yaml": "BINANCEFUT",
    "bybitfut.yaml": "BYBITFUT",
    "gatefut.yaml": "GATEFUT",
    "bitget.yaml": "BITGET",
    "kucoin.yaml": "KUCOIN",
    "mexc.yaml": "MEXC",
    "binance.yaml": "BINANCE",
    "gatespot.yaml": "GATE",
}

DEFAULT_FEE = 0.0004


def parse_base_quote(symbol: str) -> tuple[str, str]:
    if symbol.endswith("USDT"):
        return symbol[:-4], "USDT"
    if symbol.endswith("USDC"):
        return symbol[:-4], "USDC"
    if "-" in symbol:
        parts = symbol.split("-", 1)
        return parts[0], parts[1]
    m = re.match(r"^([A-Z0-9]+)(USDT|USDC|EUR|USD)$", symbol)
    if m:
        return m.group(1), m.group(2)
    return symbol, "USDT"


def load_feed_symbols(feeds_dir: Path, disabled: set[str]) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for fname, exchange in FEED_TO_EXCHANGE.items():
        stem = fname.replace(".yaml", "")
        if stem in disabled:
            continue
        path = feeds_dir / fname
        if not path.is_file():
            continue
        with path.open(encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        if not cfg.get("enabled", True):
            continue
        symbols = cfg.get("symbols") or []
        allow = set(DEMO_SYMBOLS)
        for sym in symbols:
            if sym in allow:
                base, quote = parse_base_quote(sym)
                ext = sym
                if exchange == "KUCOIN" and "-" not in sym:
                    ext = f"{base}-{quote}"
                rows.append((exchange, sym, ext, base, quote))
    return rows


def upsert(conn, exchange: str, symbol: str, external: str, base: str, quote: str) -> None:
    sql = """
    INSERT INTO manager.symbol (
        exchange, symbol, is_active, base_curr, quote_curr, external_symbol,
        fee_market_buy, fee_market_sell, price_precision, volume_precision,
        contract_size, symbol_type
    ) VALUES (
        %s, %s, true, %s, %s, %s,
        %s, %s, 8, 8,
        1.0, 'PERP'
    )
    ON CONFLICT (exchange, symbol) DO UPDATE SET
        is_active = true,
        external_symbol = EXCLUDED.external_symbol,
        base_curr = EXCLUDED.base_curr,
        quote_curr = EXCLUDED.quote_curr
    """
    with conn.cursor() as cur:
        cur.execute(sql, (exchange, symbol, base, quote, external, DEFAULT_FEE, DEFAULT_FEE))


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    feeds_dir = root / "conf" / "feeds"
    disabled = set(os.environ.get("DISABLED_FEEDS", "bitvavo,bitkub,kraken").split(","))

    host = os.environ.get("PG_HOST", "127.0.0.1")
    port = int(os.environ.get("PG_PORT", "5437"))
    db = os.environ.get("PG_DB", "arb_demo")
    user = os.environ.get("PG_USER", "postgres")
    password = os.environ.get("PG_PASS", os.environ.get("POSTGRES_PASSWORD", "postgres"))
    if os.environ.get("PG_PASS_FILE") and Path(os.environ["PG_PASS_FILE"]).is_file():
        password = Path(os.environ["PG_PASS_FILE"]).read_text(encoding="utf-8").strip()

    rows = load_feed_symbols(feeds_dir, disabled)
    if not rows:
        print("No symbols to bootstrap", file=sys.stderr)
        sys.exit(1)

    conninfo = f"host={host} port={port} dbname={db} user={user} password={password}"
    with psycopg.connect(conninfo) as conn:
        for exchange, symbol, external, base, quote in rows:
            upsert(conn, exchange, symbol, external, base, quote)
        conn.commit()
    print(f"Bootstrapped {len(rows)} symbol rows into manager.symbol")


if __name__ == "__main__":
    main()
