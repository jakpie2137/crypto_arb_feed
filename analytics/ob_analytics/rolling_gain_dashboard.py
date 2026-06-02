# rolling_gain_dashboard.py
import os
from pathlib import Path

import psycopg2
import pandas as pd

from dash import Dash, dcc, html, Input, Output, State
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go


# ============================================
#  CONFIG
# ============================================

DB_HOST = os.getenv("PG_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("PG_PORT", "5437"))
DB_NAME = os.getenv("PG_DB", "arb_demo")
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASS = os.getenv("PG_PASS", "postgres")

TABLE_NAME = os.getenv("PG_TABLE", "market_data.ob_arb_rolling_gain_30s")
DEFAULT_LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "8"))
DASH_REFRESH_SEC = max(60, int(os.getenv("ROLLING_DASHBOARD_REFRESH_SEC", "60")))
DASH_PORT = int(os.getenv("DASH_PORT", "8050"))

BACKGROUND = "#1c1c1c"
TEXT_COLOR = "white"

MAX_INSTRUMENTS = 10


# ============================================
#  ASSETS / CSS – DARK MODE
# ============================================

BASE_DIR = Path(__file__).resolve().parent
ASSETS_DIR = BASE_DIR / "assets"
ASSETS_DIR.mkdir(exist_ok=True)

DARK_CSS = f"""
html, body {{
    margin: 0;
    background-color: {BACKGROUND};
    color: {TEXT_COLOR};
    font-family: Arial, sans-serif;
}}
.Select-control {{
    background-color: {BACKGROUND} !important;
    color: {TEXT_COLOR} !important;
}}
.Select-menu-outer {{
    background-color: {BACKGROUND} !important;
    color: {TEXT_COLOR} !important;
}}
"""

(ASSETS_DIR / "dark.css").write_text(DARK_CSS, encoding="utf-8")


# ============================================
#  DB HELPERS
# ============================================

def get_pg_connection():
    return psycopg2.connect(
        host=str(DB_HOST),
        port=str(DB_PORT),
        dbname=str(DB_NAME),
        user=str(DB_USER),
        password=str(DB_PASS),
    )


def load_meta():
    """Light query for dropdowns: DISTINCT src_exchange, symbol."""
    query = f"""
        SELECT DISTINCT
            src_exchange,
            symbol
        FROM {TABLE_NAME}
        ORDER BY src_exchange, symbol;
    """
    with get_pg_connection() as conn:
        df = pd.read_sql(query, conn)
    if df.empty:
        df = pd.DataFrame(columns=["src_exchange", "symbol"])
    df["instrument"] = df["src_exchange"].astype(str) + "-" + df["symbol"].astype(str)
    return df


def _normalize_instruments(instruments):
    if instruments is None:
        return []
    if isinstance(instruments, str):
        instruments = [instruments]
    # drop empties, enforce limit
    out = [x for x in instruments if isinstance(x, str) and x.strip()]
    return out[:MAX_INSTRUMENTS]


def load_timeseries(
    instruments,
    time_mode="last_x_hours",
    last_x_hours=DEFAULT_LOOKBACK_HOURS,
    start_date=None,
    end_date=None,
):
    """
    Fetch timeseries for instruments (max 10).

    time_mode:
      - 'last_x_hours' (default): ts >= NOW() - X hours
      - 'range': ts between start_date and end_date (inclusive end-day), dates are 'YYYY-MM-DD'
    """
    instruments = _normalize_instruments(instruments)
    if not instruments:
        return pd.DataFrame(
            columns=[
                "ts",
                "symbol",
                "src_exchange",
                "ref_exchange",
                "side",
                "target_volume_quote",
                "avg_gain_pct_30s",
                "instrument",
            ]
        )

    # Build OR clauses for (src_exchange, symbol)
    where_parts = []
    params = []

    for inst in instruments:
        # Robust split: only first '-'
        if "-" not in inst:
            # if bad format, skip instead of crashing
            continue
        src, sym = inst.split("-", 1)
        src = src.strip()
        sym = sym.strip()
        if not src or not sym:
            continue
        where_parts.append("(src_exchange = %s AND symbol = %s)")
        params.extend([src, sym])

    if not where_parts:
        return pd.DataFrame(
            columns=[
                "ts",
                "symbol",
                "src_exchange",
                "ref_exchange",
                "side",
                "target_volume_quote",
                "avg_gain_pct_30s",
                "instrument",
            ]
        )

    where_sql = " OR ".join(where_parts)

    # Time filter
    time_mode = (time_mode or "last_x_hours").strip().lower()
    if time_mode not in ("last_x_hours", "range"):
        time_mode = "last_x_hours"

    last_x_hours = int(last_x_hours) if (last_x_hours is not None and str(last_x_hours).isdigit()) else DEFAULT_LOOKBACK_HOURS
    last_x_hours = max(1, last_x_hours)

    start_dt = None
    end_dt = None
    if time_mode == "range":
        # DatePickerRange returns strings 'YYYY-MM-DD'
        if start_date and end_date:
            start_dt = pd.to_datetime(start_date).tz_localize("UTC")
            # inclusive end day
            end_dt = (pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).tz_localize("UTC")
        else:
            # missing boundaries -> fallback
            time_mode = "last_x_hours"

    query = f"""
        SELECT
            ts,
            symbol,
            src_exchange,
            ref_exchange,
            side,
            target_volume_quote,
            avg_gain_pct_30s
        FROM {TABLE_NAME}
        WHERE (
            (%s = 'last_x_hours' AND ts >= NOW() - (%s * INTERVAL '1 hour'))
            OR (%s = 'range' AND ts >= %s AND ts <= %s)
        )
        AND ({where_sql})
        ORDER BY ts ASC;
    """

    # time params must match placeholders always
    time_params = [time_mode, float(last_x_hours), time_mode, start_dt, end_dt]

    with get_pg_connection() as conn:
        df = pd.read_sql(query, conn, params=time_params + params)

    if df.empty:
        df = pd.DataFrame(
            columns=[
                "ts",
                "symbol",
                "src_exchange",
                "ref_exchange",
                "side",
                "target_volume_quote",
                "avg_gain_pct_30s",
            ]
        )

    # Add instrument, normalize ts
    if "src_exchange" in df.columns and "symbol" in df.columns:
        df["instrument"] = df["src_exchange"].astype(str) + "-" + df["symbol"].astype(str)
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")

    return df


# ============================================
#  PLOT HELPERS
# ============================================

def create_base_figure():
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=("ASK", "BID"),
    )
    fig.update_layout(
        height=900,
        paper_bgcolor=BACKGROUND,
        plot_bgcolor=BACKGROUND,
        font=dict(color=TEXT_COLOR),
        hovermode="x unified",
        legend=dict(orientation="h", y=1.05),
        margin=dict(l=40, r=20, t=60, b=40),
    )
    fig.update_xaxes(showgrid=False, color=TEXT_COLOR)
    fig.update_yaxes(showgrid=True, gridcolor="#333", color=TEXT_COLOR)
    return fig


def compute_yrange(series: pd.Series):
    if series is None or series.empty:
        return (-1, 1)
    try:
        minv = float(series.min())
        maxv = float(series.max())
    except Exception:
        return (-1, 1)
    if minv == maxv:
        pad = abs(minv) * 0.1 + 1e-6
        return (minv - pad, maxv + pad)
    pad = (maxv - minv) * 0.1
    return (minv - pad, maxv + pad)


# ============================================
#  STARTUP META (DO NOT CRASH APP)
# ============================================

STARTUP_ERROR = None
try:
    meta_initial = load_meta()
except Exception as e:
    STARTUP_ERROR = (
        f"DB meta load failed. Check PG_* env vars and PG_TABLE. "
        f"PG_TABLE='{TABLE_NAME}'. Error: {type(e).__name__}: {e}"
    )
    meta_initial = pd.DataFrame(columns=["src_exchange", "symbol", "instrument"])


# ============================================
#  DASH APP
# ============================================

app = Dash(__name__, assets_folder=str(ASSETS_DIR))
server = app.server


# Dark UI + remove default browser margins
app.index_string = '<!DOCTYPE html>\n<html>\n  <head>\n    {%metas%}\n    <title>{%title%}</title>\n    {%favicon%}\n    {%css%}\n    <style>\n/* Kill default browser whitespace */\nhtml, body {\n  margin: 0 !important;\n  padding: 0 !important;\n  background: #0e0e0e !important;\n  color: #e6e6e6 !important;\n}\n\n/* Dash root */\n#react-entry-point, #_dash-app-content {\n  margin: 0 !important;\n  padding: 0 !important;\n  background: #0e0e0e !important;\n}\n\n/* General text */\n* { box-sizing: border-box; }\n\n/* Inputs */\ninput, textarea {\n  background: #111 !important;\n  color: #e6e6e6 !important;\n  border: 1px solid #555 !important;\n}\n\n/* Dropdown (react-select v1 used by Dash) */\n.Select-control {\n  background-color: #111 !important;\n  border: 1px solid #555 !important;\n}\n.Select-placeholder, .Select--single > .Select-control .Select-value, .Select-value-label {\n  color: #e6e6e6 !important;\n}\n.Select-input > input { color: #e6e6e6 !important; }\n.Select-menu-outer {\n  background-color: #111 !important;\n  border: 1px solid #555 !important;\n  color: #e6e6e6 !important;\n}\n.Select-option {\n  background-color: #111 !important;\n  color: #e6e6e6 !important;\n}\n.Select-option.is-focused {\n  background-color: #222 !important;\n}\n.Select-option.is-selected {\n  background-color: #333 !important;\n}\n\n/* DatePicker (react-dates) */\n.DateRangePickerInput, .DateRangePickerInput__withBorder {\n  background: #111 !important;\n  border: 1px solid #555 !important;\n}\n.DateInput {\n  background: #111 !important;\n}\n.DateInput_input {\n  background: #111 !important;\n  color: #e6e6e6 !important;\n  border: 0 !important;\n}\n.DateInput_input__focused {\n  background: #111 !important;\n  border-bottom: 2px solid #888 !important;\n}\n.DateRangePickerInput_arrow_svg {\n  fill: #e6e6e6 !important;\n}\n.CalendarDay {\n  background: #111 !important;\n  color: #e6e6e6 !important;\n  border: 1px solid #222 !important;\n}\n.CalendarDay__hovered_span, .CalendarDay__hovered_span:hover {\n  background: #222 !important;\n  color: #e6e6e6 !important;\n  border: 1px solid #333 !important;\n}\n.CalendarDay__selected, .CalendarDay__selected:hover {\n  background: #333 !important;\n  color: #ffffff !important;\n  border: 1px solid #555 !important;\n}\n.CalendarDay__selected_span, .CalendarDay__selected_span:hover {\n  background: #222 !important;\n  color: #ffffff !important;\n  border: 1px solid #444 !important;\n}\n.DayPicker, .DayPicker__withBorder {\n  background: #0e0e0e !important;\n}\n.DayPickerNavigation_button {\n  background: #111 !important;\n  border: 1px solid #444 !important;\n}\n.DayPickerNavigation_svg__horizontal {\n  fill: #e6e6e6 !important;\n}\n.DayPickerKeyboardShortcuts_show__bottomRight {\n  border-right: 33px solid #444 !important;\n}\n\n/* Plotly modebar tweaks (optional) */\n.modebar {\n  background: rgba(0,0,0,0.2) !important;\n}\n</style>\n  </head>\n  <body>\n    {%app_entry%}\n    <footer>\n      {%config%}\n      {%scripts%}\n      {%renderer%}\n    </footer>\n  </body>\n</html>\n'

app.layout = html.Div(
    style={
        "display": "flex",
        "height": "100vh",
        "fontFamily": "Arial",
        "backgroundColor": BACKGROUND,
        "margin": "0",
        "padding": "0",
        "color": TEXT_COLOR,
    },
    children=[
        dcc.Store(id="meta-store", data=meta_initial.to_dict("records")),
        dcc.Store(id="ts-store", data=[]),
        dcc.Interval(id="auto-refresh", interval=DASH_REFRESH_SEC * 1000, n_intervals=0),

        # LEFT PANEL
        html.Div(
            style={
                "width": "22%",
                "padding": "10px",
                "borderRight": "1px solid #333",
                "backgroundColor": BACKGROUND,
                "color": TEXT_COLOR,
                "boxSizing": "border-box",
            },
            children=[
                html.H3("Filters", style={"color": TEXT_COLOR}),

                html.Div(
                    STARTUP_ERROR if STARTUP_ERROR else "",
                    id="startup-error",
                    style={
                        "whiteSpace": "pre-wrap",
                        "backgroundColor": "#3a0f0f",
                        "border": "1px solid #7a2a2a",
                        "padding": "8px",
                        "marginBottom": "10px",
                        "display": "block" if STARTUP_ERROR else "none",
                    },
                ),

                html.Button(
                    "Refresh data",
                    id="refresh-button",
                    n_clicks=0,
                    style={
                        "width": "100%",
                        "marginBottom": "12px",
                        "backgroundColor": "#333",
                        "color": TEXT_COLOR,
                        "border": "1px solid #555",
                        "padding": "8px",
                        "cursor": "pointer",
                    },
                ),

                html.Hr(style={"borderColor": "#444"}),

                html.Label("Time filter mode", style={"color": TEXT_COLOR}),
                dcc.RadioItems(
                    id="time-mode",
                    options=[
                        {"label": "Last X hours", "value": "last_x_hours"},
                        {"label": "Start / End date", "value": "range"},
                    ],
                    value="last_x_hours",
                    style={"color": TEXT_COLOR, "marginBottom": "8px"},
                    inputStyle={"marginRight": "6px"},
                ),

                html.Label("Last X hours", style={"color": TEXT_COLOR}),
                dcc.Input(
                    id="last-x-hours",
                    type="number",
                    min=1,
                    step=1,
                    value=DEFAULT_LOOKBACK_HOURS,
                    style={
                        "width": "100%",
                        "marginBottom": "12px",
                        "backgroundColor": "#111",
                        "color": TEXT_COLOR,
                        "border": "1px solid #555",
                        "padding": "6px",
                        "boxSizing": "border-box",
                    },
                ),

                html.Label("Start / End date (UTC)", style={"color": TEXT_COLOR}),
                dcc.DatePickerRange(
                    id="date-range",
                    start_date_placeholder_text="Start date",
                    end_date_placeholder_text="End date",
                    minimum_nights=0,
                    display_format="YYYY-MM-DD",
                    style={"marginBottom": "12px"},
                ),

                html.Hr(style={"borderColor": "#444"}),

                html.Label("Source exchange", style={"color": TEXT_COLOR}),
                dcc.Dropdown(
                    id="src-exchange-dropdown",
                    multi=True,
                    placeholder="Select exchanges...",
                    style={"backgroundColor": BACKGROUND, "color": TEXT_COLOR},
                ),
                html.Br(),

                html.Label("Symbol", style={"color": TEXT_COLOR}),
                dcc.Dropdown(
                    id="symbol-dropdown",
                    multi=True,
                    placeholder="Select symbols...",
                    style={"backgroundColor": BACKGROUND, "color": TEXT_COLOR},
                ),
                html.Br(),

                html.Label(f"Instrument (max {MAX_INSTRUMENTS})", style={"color": TEXT_COLOR}),
                dcc.Dropdown(
                    id="instrument-dropdown",
                    multi=True,
                    placeholder="Select instruments...",
                    style={"backgroundColor": BACKGROUND, "color": TEXT_COLOR},
                ),
            ],
        ),

        # RIGHT PANEL – CHART
        html.Div(
            style={
                "flex": 1,
                "padding": "10px",
                "backgroundColor": BACKGROUND,
                "boxSizing": "border-box",
            },
            children=[
                html.H3("Rolling 30s avg gain pct", style={"color": TEXT_COLOR}),
                dcc.Graph(id="gain-graph", style={"height": "95%"}),
            ],
        ),
    ],
)


# ============================================
#  CALLBACKS
# ============================================

@app.callback(
    Output("meta-store", "data"),
    Input("refresh-button", "n_clicks"),
    Input("auto-refresh", "n_intervals"),
    prevent_initial_call=True,
)
def refresh_meta(_n_clicks):
    try:
        df = load_meta()
        return df.to_dict("records")
    except Exception as e:
        # Keep current meta; surface error in startup-error div via its own callback if desired
        return meta_initial.to_dict("records")


@app.callback(
    Output("ts-store", "data"),
    Input("refresh-button", "n_clicks"),
    Input("auto-refresh", "n_intervals"),
    Input("instrument-dropdown", "value"),
    Input("time-mode", "value"),
    Input("last-x-hours", "value"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    prevent_initial_call=False,
)
def refresh_timeseries(_n_clicks, selected_instruments, time_mode, last_x_hours, start_date, end_date):
    instruments = _normalize_instruments(selected_instruments)
    if not instruments:
        return []
    try:
        df = load_timeseries(
            instruments,
            time_mode=time_mode,
            last_x_hours=last_x_hours,
            start_date=start_date,
            end_date=end_date,
        )
        return df.to_dict("records")
    except Exception:
        return []


@app.callback(
    Output("src-exchange-dropdown", "options"),
    Output("symbol-dropdown", "options"),
    Input("meta-store", "data"),
)
def update_src_symbol_options(meta_records):
    df = pd.DataFrame(meta_records)
    if df.empty:
        return [], []
    src_options = sorted(df["src_exchange"].dropna().unique())
    sym_options = sorted(df["symbol"].dropna().unique())
    return (
        [{"label": x, "value": x} for x in src_options],
        [{"label": x, "value": x} for x in sym_options],
    )


@app.callback(
    Output("instrument-dropdown", "options"),
    Input("meta-store", "data"),
    Input("src-exchange-dropdown", "value"),
    Input("symbol-dropdown", "value"),
)
def update_instrument_options(meta_records, src_exchanges, symbols):
    df = pd.DataFrame(meta_records)
    if df.empty:
        return []
    if src_exchanges:
        df = df[df["src_exchange"].isin(src_exchanges)]
    if symbols:
        df = df[df["symbol"].isin(symbols)]
    instruments = sorted(df["instrument"].dropna().unique())
    return [{"label": x, "value": x} for x in instruments]


@app.callback(
    Output("gain-graph", "figure"),
    Input("ts-store", "data"),
    Input("instrument-dropdown", "value"),
)
def update_graph(ts_records, selected_instruments):
    fig = create_base_figure()
    df = pd.DataFrame(ts_records)

    instruments = _normalize_instruments(selected_instruments)
    if df.empty or not instruments:
        # consistent axes
        yrange = compute_yrange(pd.Series(dtype=float))
        fig.update_yaxes(range=list(yrange), zeroline=True, zerolinecolor="white", zerolinewidth=2, row=1, col=1)
        fig.update_yaxes(range=list(yrange), zeroline=True, zerolinecolor="white", zerolinewidth=2, row=2, col=1)
        fig.update_yaxes(title_text="ASK gain %", row=1, col=1)
        fig.update_yaxes(title_text="BID gain %", row=2, col=1)
        fig.update_xaxes(title_text="Time", row=2, col=1)
        return fig

    df = df[df.get("instrument", "").isin(instruments)]
    if df.empty:
        return fig

    palette = getattr(px.colors.qualitative, "Plotly", None) or []
    if not palette:
        palette = ["#1f77b4"]  # fallback to avoid any list issues

    color_map = {inst: palette[i % len(palette)] for i, inst in enumerate(instruments)}

    ask_df = df[df["side"] == "ASK"]
    for inst in instruments:
        tmp = ask_df[ask_df["instrument"] == inst]
        if not tmp.empty:
            fig.add_trace(
                go.Scatter(
                    x=tmp["ts"],
                    y=tmp["avg_gain_pct_30s"],
                    mode="lines",
                    name=inst,
                    line=dict(color=color_map[inst]),
                ),
                row=1,
                col=1,
            )

    bid_df = df[df["side"] == "BID"]
    for inst in instruments:
        tmp = bid_df[bid_df["instrument"] == inst]
        if not tmp.empty:
            fig.add_trace(
                go.Scatter(
                    x=tmp["ts"],
                    y=tmp["avg_gain_pct_30s"],
                    mode="lines",
                    name=inst,
                    line=dict(color=color_map[inst]),
                    showlegend=False,
                ),
                row=2,
                col=1,
            )

    yr1 = compute_yrange(ask_df["avg_gain_pct_30s"]) if not ask_df.empty else None
    yr2 = compute_yrange(bid_df["avg_gain_pct_30s"]) if not bid_df.empty else None
    if yr1:
        fig.update_yaxes(range=list(yr1), row=1, col=1)
    if yr2:
        fig.update_yaxes(range=list(yr2), row=2, col=1)

    fig.update_yaxes(title_text="ASK gain %", row=1, col=1)
    fig.update_yaxes(title_text="BID gain %", row=2, col=1)
    fig.update_xaxes(title_text="Time", row=2, col=1)

    return fig


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=DASH_PORT)
