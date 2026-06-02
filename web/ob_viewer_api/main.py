"""FastAPI backend for web OB viewer (read-only, no API keys)."""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from analytics.ob_analytics import gain_calc

app = FastAPI(title="OB Viewer API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

FEED_PROXY_URL = os.getenv("FEED_PROXY_URL", "http://localhost:8082")
REF_PRIORITY = ["BINANCEFUT", "GATEFUT", "BYBITFUT", "BINANCE", "GATE", "KUCOIN", "BITGET", "MEXC"]
DISPLAY_DEPTH = int(os.getenv("OB_VIEWER_DISPLAY_DEPTH", "20"))
REFRESH_MS = int(os.getenv("OB_VIEWER_REFRESH_MS", "100"))
TARGET_VOLUME = float(os.getenv("OB_VIEWER_TARGET_VOLUME_USD", "5000"))
DEFAULT_FEE = 0.0004


def pg_connect():
    import psycopg

    return psycopg.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5437")),
        dbname=os.getenv("PG_DB", "arb_demo"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASS", os.getenv("POSTGRES_PASSWORD", "postgres")),
    )


class ViewerConfig(BaseModel):
    refresh_interval_ms: int
    display_depth: int
    default_target_volume_usd: float
    public_read_only: bool


class ObLevel(BaseModel):
    price: float
    size: float
    volume_quote: float
    gain_usd: Optional[float] = None
    gain_pct: Optional[float] = None
    heat: float = 0.0


class OrderBookView(BaseModel):
    exchange: str
    symbol: str
    has_data: bool
    ts: Optional[int] = None
    age_ms: Optional[int] = None
    funding_rate: Optional[float] = None
    funding_rate_norm_24h: Optional[float] = None
    bids: list[ObLevel]
    asks: list[ObLevel]


class CompareResponse(BaseModel):
    symbol: str
    source_exchange: str
    ref_exchange: str
    target_volume_usd: float
    fee_source: float
    fee_ref: float
    source: OrderBookView
    ref: OrderBookView
    buy_gain_pct: Optional[float] = None
    buy_gain_usd: Optional[float] = None
    sell_gain_pct: Optional[float] = None
    sell_gain_usd: Optional[float] = None


async def fetch_orderbook(client: httpx.AsyncClient, exchange: str, symbol: str, levels: int) -> dict:
    r = await client.get(
        f"{FEED_PROXY_URL}/orderbook",
        params={"ex": exchange, "sym": symbol, "levels": levels},
        timeout=5.0,
    )
    r.raise_for_status()
    return r.json()


def load_symbols() -> list[str]:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT symbol FROM manager.symbol
            WHERE is_active = true ORDER BY symbol
            """
        )
        return [row[0] for row in cur.fetchall()]


def load_exchanges() -> list[str]:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT exchange FROM manager.symbol
            WHERE is_active = true ORDER BY exchange
            """
        )
        return [row[0] for row in cur.fetchall()]


def load_fees(exchange: str, symbol: str) -> tuple[float, float]:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT fee_market_buy, fee_market_sell
            FROM manager.symbol
            WHERE exchange = %s AND symbol = %s AND is_active = true
            LIMIT 1
            """,
            (exchange, symbol),
        )
        row = cur.fetchone()
    if not row:
        return DEFAULT_FEE, DEFAULT_FEE
    buy = float(row[0]) if row[0] is not None else DEFAULT_FEE
    sell = float(row[1]) if row[1] is not None else DEFAULT_FEE
    return buy, sell


def resolve_ref_exchange(symbol: str) -> Optional[str]:
    with pg_connect() as conn, conn.cursor() as cur:
        for ex in REF_PRIORITY:
            cur.execute(
                """
                SELECT 1 FROM manager.symbol
                WHERE symbol = %s AND exchange = %s AND is_active = true LIMIT 1
                """,
                (symbol, ex),
            )
            if cur.fetchone():
                return ex
    return None


def _levels(raw: list[dict], side: str, ref_best_bid: float, ref_best_ask: float,
            fee_sell: float, fee_ref_buy: float, fee_buy: float, fee_ref_sell: float,
            is_source: bool) -> list[ObLevel]:
    out: list[ObLevel] = []
    max_vol = 1.0
    parsed = []
    for lv in raw or []:
        p = float(lv.get("p", 0))
        s = float(lv.get("s", 0))
        if p <= 0 or s <= 0:
            continue
        vol = p * s
        parsed.append((p, s, vol))
        max_vol = max(max_vol, vol)

    for p, s, vol in parsed[:DISPLAY_DEPTH]:
        gain_usd = None
        gain_pct = None
        if is_source:
            if side == "BID" and ref_best_ask > 0:
                eff_sell = p * (1.0 - fee_sell)
                eff_ref_buy = ref_best_ask * (1.0 + fee_ref_buy)
                per_unit = eff_sell - eff_ref_buy
                gain_usd = per_unit * s
                gain_pct = (per_unit / eff_ref_buy * 100.0) if eff_ref_buy else None
            elif side == "ASK" and ref_best_bid > 0:
                eff_buy = p * (1.0 + fee_buy)
                eff_ref_sell = ref_best_bid * (1.0 - fee_ref_sell)
                per_unit = eff_ref_sell - eff_buy
                gain_usd = per_unit * s
                gain_pct = (per_unit / eff_buy * 100.0) if eff_buy else None
        out.append(
            ObLevel(
                price=p,
                size=s,
                volume_quote=vol,
                gain_usd=gain_usd,
                gain_pct=gain_pct,
                heat=vol / max_vol,
            )
        )
    return out


def build_view(data: dict, exchange: str, symbol: str, is_source: bool,
               ref_data: Optional[dict], fee_src: tuple[float, float], fee_ref: tuple[float, float]) -> OrderBookView:
    if not data.get("hasData"):
        return OrderBookView(exchange=exchange, symbol=symbol, has_data=False, bids=[], asks=[])

    ref_best_bid = ref_best_ask = 0.0
    if ref_data and ref_data.get("hasData"):
        bids = ref_data.get("bids") or []
        asks = ref_data.get("asks") or []
        if bids:
            ref_best_bid = float(bids[0]["p"])
        if asks:
            ref_best_ask = float(asks[0]["p"])

    fee_buy, fee_sell = fee_src
    ref_buy, ref_sell = fee_ref

    bids = _levels(data.get("bids"), "BID", ref_best_bid, ref_best_ask, fee_sell, ref_buy, fee_buy, ref_sell, is_source)
    asks = _levels(data.get("asks"), "ASK", ref_best_bid, ref_best_ask, fee_sell, ref_buy, fee_buy, ref_sell, is_source)

    return OrderBookView(
        exchange=exchange,
        symbol=symbol,
        has_data=True,
        ts=data.get("ts"),
        age_ms=data.get("ageMs"),
        funding_rate=data.get("fundingRate"),
        funding_rate_norm_24h=data.get("fundingRateNorm24h"),
        bids=bids,
        asks=asks,
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/config", response_model=ViewerConfig)
def viewer_config() -> ViewerConfig:
    return ViewerConfig(
        refresh_interval_ms=REFRESH_MS,
        display_depth=DISPLAY_DEPTH,
        default_target_volume_usd=TARGET_VOLUME,
        public_read_only=os.getenv("PUBLIC_READ_ONLY", "true").lower() == "true",
    )


@app.get("/symbols")
def symbols() -> dict[str, list[str]]:
    return {"symbols": load_symbols()}


@app.get("/exchanges")
def exchanges() -> dict[str, list[str]]:
    return {"exchanges": load_exchanges()}


@app.get("/ref-exchange")
def ref_exchange(symbol: str = Query(...)) -> dict[str, Optional[str]]:
    return {"symbol": symbol, "ref_exchange": resolve_ref_exchange(symbol)}


@app.get("/compare", response_model=CompareResponse)
async def compare(
    symbol: str = Query(...),
    source_exchange: str = Query(...),
    ref_exchange: Optional[str] = Query(None),
    target_volume_usd: float = Query(TARGET_VOLUME),
) -> CompareResponse:
    ref_ex = ref_exchange or resolve_ref_exchange(symbol)
    if not ref_ex:
        raise HTTPException(400, "No reference exchange for symbol")

    async with httpx.AsyncClient() as client:
        src_raw = await fetch_orderbook(client, source_exchange, symbol, DISPLAY_DEPTH)
        ref_raw = await fetch_orderbook(client, ref_ex, symbol, DISPLAY_DEPTH)

    fee_src = load_fees(source_exchange, symbol)
    fee_ref = load_fees(ref_ex, symbol)
    total_fees = fee_src[0] + fee_src[1] + fee_ref[0] + fee_ref[1]

    source = build_view(src_raw, source_exchange, symbol, True, ref_raw, fee_src, fee_ref)
    ref = build_view(ref_raw, ref_ex, symbol, False, None, fee_ref, fee_ref)

    buy_pct = buy_usd = sell_pct = sell_usd = None
    if src_raw.get("hasData") and ref_raw.get("hasData"):
        src_asks = [{"p": lv["p"], "s": lv["s"]} for lv in src_raw.get("asks") or []]
        src_bids = [{"p": lv["p"], "s": lv["s"]} for lv in src_raw.get("bids") or []]
        ref_bids = ref_raw.get("bids") or []
        ref_asks = ref_raw.get("asks") or []
        ref_best_bid = float(ref_bids[0]["p"]) if ref_bids else 0.0
        ref_best_ask = float(ref_asks[0]["p"]) if ref_asks else 0.0
        if ref_best_bid > 0:
            sell_pct, sell_usd = gain_calc.compute_vwap_gain(
                src_asks, ref_best_bid, "ASK", target_volume_usd, total_fees / 2
            )
        if ref_best_ask > 0:
            buy_pct, buy_usd = gain_calc.compute_vwap_gain(
                src_bids, ref_best_ask, "BID", target_volume_usd, total_fees / 2
            )

    return CompareResponse(
        symbol=symbol,
        source_exchange=source_exchange,
        ref_exchange=ref_ex,
        target_volume_usd=target_volume_usd,
        fee_source=fee_src[0],
        fee_ref=fee_ref[0],
        source=source,
        ref=ref,
        buy_gain_pct=buy_pct,
        buy_gain_usd=buy_usd,
        sell_gain_pct=sell_pct,
        sell_gain_usd=sell_usd,
    )
