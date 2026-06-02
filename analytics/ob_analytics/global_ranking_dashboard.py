"""Global gain rankings dashboard (refreshed ~15 min)."""

from __future__ import annotations

import json
import os

import pandas as pd
import psycopg2
from dash import Dash, dcc, html, Input, Output
from dash import dash_table

DB_HOST = os.getenv("PG_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("PG_PORT", "5437"))
DB_NAME = os.getenv("PG_DB", "arb_demo")
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASS = os.getenv("PG_PASS", "postgres")
REFRESH_SEC = max(60, int(os.getenv("GLOBAL_RANKING_REFRESH_SEC", "900")))
PORT = int(os.getenv("DASH_PORT", "8051"))


def conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)


def latest_payload(rank_type: str):
    q = """
    SELECT payload, ts FROM market_data.global_gain_rankings
    WHERE rank_type = %s ORDER BY ts DESC LIMIT 1
    """
    with conn() as c:
        df = pd.read_sql(q, c, params=(rank_type,))
    if df.empty:
        return [], None
    payload = df.iloc[0]["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return payload, df.iloc[0]["ts"]


app = Dash(__name__)
app.layout = html.Div(
    style={"backgroundColor": "#1c1c1c", "color": "white", "padding": "16px", "fontFamily": "Arial"},
    children=[
        html.H2("Global gain rankings"),
        html.Div(id="updated-at", style={"color": "#aaa", "marginBottom": "12px"}),
        dcc.Interval(id="tick", interval=REFRESH_SEC * 1000, n_intervals=0),
        html.H3("Sell gain ≥30s (ASK)"),
        dash_table.DataTable(id="tbl-sell", page_size=15),
        html.H3("Max single gain (24h)"),
        html.Pre(id="max-gain", style={"background": "#111", "padding": "8px"}),
        html.H3("Multi-exchange coins"),
        dash_table.DataTable(id="tbl-coins", page_size=15),
    ],
)


@app.callback(
    Output("tbl-sell", "data"),
    Output("tbl-sell", "columns"),
    Output("max-gain", "children"),
    Output("tbl-coins", "data"),
    Output("tbl-coins", "columns"),
    Output("updated-at", "children"),
    Input("tick", "n_intervals"),
)
def refresh(_):
    sell, ts1 = latest_payload("sell_gain_30s_plus")
    mx, ts2 = latest_payload("max_single_gain_24h")
    coins, ts3 = latest_payload("multi_exchange_coin_spread")
    ts = ts1 or ts2 or ts3
    updated = f"Last ranking run: {ts}" if ts is not None else "No data yet — start ranking worker / Kestra"

    sell_cols = [{"name": c, "id": c} for c in (sell[0].keys() if sell else [])]
    coin_rows = []
    for row in coins or []:
        coin_rows.append({
            "coin": row.get("coin"),
            "exchange_count": row.get("exchange_count"),
            "instruments": json.dumps(row.get("instruments", []))[:200],
        })
    coin_cols = [{"name": c, "id": c} for c in (coin_rows[0].keys() if coin_rows else ["coin", "exchange_count", "instruments"])]

    return sell, sell_cols, json.dumps(mx or {}, indent=2), coin_rows, coin_cols, updated


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
