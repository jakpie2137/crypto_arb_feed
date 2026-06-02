import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any

import requests
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

API_BASE = os.getenv("OB_API_BASE", "http://localhost:8000")

# ===== Dashboard prefill defaults =====
# You can change these values to set default filters in the UI.

# Default symbol shown in all pages
DEFAULT_SYMBOL = "MOODENGUSDT"

# Default top-level page selection in sidebar
# One of: "VWAP Gains (current)", "Nominal OB Gains", "Histograms"
DEFAULT_VIEW = "VWAP Gains (current)"

# Default grouping dimension for pages that use 'Group by'
# One of: "instrument", "symbol", "exchange"
DEFAULT_GROUP_BY = "symbol"

# Default histogram page settings (page 3)
# Histogram type: "First level gain (%)", "VWAP gain", "Total OB positive gain ($)"
DEFAULT_HISTOGRAM_TYPE = "First level gain (%)"
# Side: "BID", "ASK", "BOTH"
DEFAULT_HISTOGRAM_SIDE = "BOTH"

# Default time range for all pages (UTC).
# Format: "YYYY-MM-DD HH:MM:SS"
DEFAULT_START_DATETIME_STR = "2025-12-08 06:30:00"
DEFAULT_END_DATETIME_STR = "2025-12-08 09:30:00"

def _parse_prefill_dt(s: str) -> datetime:
    """Helper to parse ISO-like datetime strings into aware UTC datetimes."""
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

DEFAULT_FROM_DT = _parse_prefill_dt(DEFAULT_START_DATETIME_STR)
DEFAULT_TO_DT = _parse_prefill_dt(DEFAULT_END_DATETIME_STR)



def fetch_symbols():
    """Zwraca listę stringów z /symbols."""
    url = f"{API_BASE}/symbols"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "symbols" in data:
        return data["symbols"]
    if isinstance(data, list):
        return data
    raise ValueError(f"Unexpected /symbols response: {data!r}")


def fetch_exchanges(symbol: str):
    """Zwraca listę giełd dla danego symbolu."""
    url = f"{API_BASE}/exchanges"
    resp = requests.get(url, params={"symbol": symbol}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "exchanges" in data:
        return data["exchanges"]
    if isinstance(data, list):
        return data
    raise ValueError(f"Unexpected /exchanges response: {data!r}")


def fetch_gains(
    symbol: str,
    ref_exchange: str,
    exchanges: List[str],
    volume: float,
    fee_source: float,
    fee_ref: float,
    time_from: datetime,
    time_to: datetime,
) -> Any:
    params: Dict[str, List[str] | float | str] = {
        "symbol": symbol,
        "ref_exchange": ref_exchange,
        "volume": volume,
        "fee_source": fee_source,
        "fee_ref": fee_ref,
        "time_from": time_from.isoformat(),
        "time_to": time_to.isoformat(),
    }
    for ex in exchanges:
        params.setdefault("exchanges", []).append(ex)

    url = f"{API_BASE}/gains"
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()




def fetch_nominal_gains(
    symbol: str,
    volume: float,
    group_by: str,
    side: str,
    time_from: datetime,
    time_to: datetime,
    exchange: str | None = None,
    instrument: str | None = None,
) -> Any:
    """
    Woła endpoint /nominal_gains i zwraca surową odpowiedź JSON.

    Parametry:
      - symbol: np. "DOGEUSDT"
      - volume: wolumen w QUOTE; musi odpowiadać bucketowi z ETL
      - group_by: "instrument" / "symbol" / "exchange"
      - side: "BID" / "ASK" / "BOTH"
      - time_from, time_to: zakres czasu (UTC)
      - exchange, instrument: opcjonalne filtry
    """
    params: Dict[str, Any] = {
        "symbol": symbol,
        "volume": float(volume),
        "group_by": group_by,
        "side": side,
        "time_from": time_from.isoformat(),
        "time_to": time_to.isoformat(),
    }
    if exchange:
        params["exchange"] = exchange
    if instrument:
        params["instrument"] = instrument

    url = f"{API_BASE}/nominal_gains"
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_histogram(
    metric: str,
    symbol: str,
    group_by: str,
    side: str,
    time_from: datetime,
    time_to: datetime,
    volume: float,
    exchange: str | None = None,
    instrument: str | None = None,
) -> Any:
    """
    Woła endpoint /histogram i zwraca surową odpowiedź JSON.

    metric:
      - 'first_level_gain_pct'
      - 'vwap_gain_pct'
      - 'vwap_gain_usd'
      - 'total_positive_gain_usd'
    """
    params: Dict[str, Any] = {
        "metric": metric,
        "symbol": symbol,
        "group_by": group_by,
        "side": side,
        "volume": float(volume),
        "time_from": time_from.isoformat(),
        "time_to": time_to.isoformat(),
    }
    if exchange:
        params["exchange"] = exchange
    if instrument:
        params["instrument"] = instrument

    url = f"{API_BASE}/histogram"
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def normalize_gains_response(
    raw: Any,
    symbol: str,
    ref_exchange: str,
    volume: float,
    fee_source: float,
    fee_ref: float,
) -> Dict[str, Any]:
    """
    Ujednolica odpowiedź API do postaci:

    {
        "symbol": str,
        "ref_exchange": str,
        "volume_bucket": float,
        "fee_source": float,
        "fee_ref": float,
        "bid": { exchange: [{ "t": iso, "gain_pct": float }, ...], ... },
        "ask": { exchange: [{ "t": iso, "gain_pct": float }, ...], ... },
    }
    """

    # 1) Jeśli już jest w "ładnym" formacie – nic nie rób.
    if isinstance(raw, dict) and "bid" in raw and "ask" in raw:
        return raw

    # 2) Nowy format z API: lista GainSeries z polami 'points'
    if isinstance(raw, list) and raw and isinstance(raw[0], dict) and "points" in raw[0]:
        first_series = raw[0]
        symbol_val = first_series.get("symbol", symbol)
        ref_exchange_val = first_series.get("ref_exchange", ref_exchange)
        try:
            volume_bucket = float(first_series.get("volume", volume))
        except (TypeError, ValueError):
            volume_bucket = volume

        bid: Dict[str, List[Dict[str, Any]]] = {}
        ask: Dict[str, List[Dict[str, Any]]] = {}

        for series in raw:
            if not isinstance(series, dict):
                continue
            points = series.get("points") or []
            for pt in points:
                if not isinstance(pt, dict):
                    continue
                exch = pt.get("exchange")
                if not exch:
                    continue

                ts_val = pt.get("snapshot_time")
                if isinstance(ts_val, str):
                    t_iso = ts_val
                else:
                    try:
                        if isinstance(ts_val, datetime):
                            t_iso = ts_val.isoformat()
                        else:
                            ts_float = float(ts_val)
                            # traktujemy jako timestamp w sekundach
                            t_iso = datetime.fromtimestamp(
                                ts_float, tz=timezone.utc
                            ).isoformat()
                    except Exception:
                        continue

                gain_bid = pt.get("gain_bid_pct")
                gain_ask = pt.get("gain_ask_pct")

                if gain_bid is not None:
                    try:
                        g_bid = float(gain_bid)
                    except (TypeError, ValueError):
                        pass
                    else:
                        bid.setdefault(exch, []).append(
                            {"t": t_iso, "gain_pct": g_bid}
                        )

                if gain_ask is not None:
                    try:
                        g_ask = float(gain_ask)
                    except (TypeError, ValueError):
                        pass
                    else:
                        ask.setdefault(exch, []).append(
                            {"t": t_iso, "gain_pct": g_ask}
                        )

        return {
            "symbol": symbol_val,
            "ref_exchange": ref_exchange_val,
            "volume_bucket": volume_bucket,
            "fee_source": fee_source,
            "fee_ref": fee_ref,
            "bid": bid,
            "ask": ask,
        }

    # 3) Legacy format – lista wierszy z polami side/gain_pct/t
    if not isinstance(raw, list):
        raise ValueError(f"Unexpected /gains response type: {type(raw)}")

    # 3) Pusta lista – zwracamy tylko meta.
    if not raw:
        return {
            "symbol": symbol,
            "ref_exchange": ref_exchange,
            "volume_bucket": volume,
            "fee_source": fee_source,
            "fee_ref": fee_ref,
            "bid": {},
            "ask": {},
        }

    first = raw[0] if isinstance(raw[0], dict) else {}

    volume_bucket = first.get("volume_bucket", volume)
    fee_src = float(first.get("fee_source", fee_source))
    fee_r = float(first.get("fee_ref", fee_ref))

    bid: Dict[str, List[Dict[str, Any]]] = {}
    ask: Dict[str, List[Dict[str, Any]]] = {}

    for row in raw:
        if not isinstance(row, dict):
            continue

        exch = (
            row.get("exchange")
            or row.get("source_exchange")
            or row.get("exch")
        )
        if not exch:
            continue

        side_raw = (row.get("side") or row.get("direction") or "").lower()
        if side_raw.startswith("bid"):
            target = bid
        elif side_raw.startswith("ask"):
            target = ask
        else:
            continue

        ts_val = row.get("t") or row.get("ts") or row.get("timestamp")

        if isinstance(ts_val, str):
            t_iso = ts_val
        else:
            try:
                ts_float = float(ts_val)
            except (TypeError, ValueError):
                continue

            # jeśli bardzo duże – traktuj jako ms
            if ts_float > 10**11:
                ts_float /= 1000.0
            t_iso = datetime.utcfromtimestamp(ts_float).isoformat()

        gain_pct = row.get("gain_pct")
        if gain_pct is None:
            continue

        try:
            g = float(gain_pct)
        except (TypeError, ValueError):
            continue

        target.setdefault(exch, []).append({"t": t_iso, "gain_pct": g})

    return {
        "symbol": first.get("symbol", symbol),
        "ref_exchange": first.get("ref_exchange", ref_exchange),
        "volume_bucket": volume_bucket,
        "fee_source": fee_src,
        "fee_ref": fee_r,
        "bid": bid,
        "ask": ask,
    }


def render_vwap_gains_page():

        st.title("OrderBook Gain Comparator")

        st.sidebar.header("Parameters")

        # --- SYMBOLS ---
        try:
            symbols = fetch_symbols()
        except Exception as e:
            st.error(f"Cannot load symbols from API: {e}")
            st.stop()

        default_symbol_idx = 0
        if DEFAULT_SYMBOL in symbols:
            default_symbol_idx = symbols.index(DEFAULT_SYMBOL)
        symbol = st.sidebar.selectbox("Symbol", options=symbols, index=default_symbol_idx)

        # --- EXCHANGES ---
        try:
            exchanges = fetch_exchanges(symbol)
        except Exception as e:
            st.error(f"Cannot load exchanges from API: {e}")
            st.stop()

        if not exchanges:
            st.warning("No exchanges found for this symbol.")
            st.stop()

        ref_exchange = st.sidebar.selectbox(
            "Reference exchange",
            options=exchanges,
            index=0,
        )

        src_exchanges = [
            ex for ex in exchanges if ex != ref_exchange
        ]

        if not src_exchanges:
            st.warning("No source exchanges (only reference).")
            st.stop()

        st.sidebar.write("Source exchanges:")
        for ex in src_exchanges:
            st.sidebar.write(f"- {ex}")

        # --- VOLUME / FEES ---
        volume = st.sidebar.number_input(
            "VWAP volume (quote)",
            min_value=1.0,
            max_value=1_000_000.0,
            value=5_000.0,
            step=100.0,
        )

        fee_source = st.sidebar.number_input(
            "Source fee (fraction)",
            min_value=0.0,
            max_value=0.01,
            value=0.0002,
            step=0.0001,
            format="%.4f",
        )

        fee_ref = st.sidebar.number_input(
            "Ref fee (fraction)",
            min_value=0.0,
            max_value=0.01,
            value=0.00045,
            step=0.0001,
            format="%.4f",
        )

        # --- TIME RANGE ---
        col1, col2 = st.columns(2)
        default_from = DEFAULT_FROM_DT
        default_to = DEFAULT_TO_DT

        date_from = col1.date_input(
            "From date",
            value=default_from.date(),
            key="from_date",
        )
        time_from = col2.time_input(
            "From time",
            value=default_from.time().replace(microsecond=0),
            key="from_time",
        )

        col3, col4 = st.columns(2)
        date_to = col3.date_input(
            "To date",
            value=default_to.date(),
            key="to_date",
        )
        time_to = col4.time_input(
            "To time",
            value=default_to.time().replace(microsecond=0),
            key="to_time",
        )

        dt_from = datetime.combine(date_from, time_from).replace(tzinfo=timezone.utc)
        dt_to = datetime.combine(date_to, time_to).replace(tzinfo=timezone.utc)

        if dt_to <= dt_from:
            st.error("`To` must be > `From`.")
            st.stop()

        if st.button("Load gains"):
            try:
                raw = fetch_gains(
                    symbol=symbol,
                    ref_exchange=ref_exchange,
                    exchanges=src_exchanges,
                    volume=volume,
                    fee_source=fee_source,
                    fee_ref=fee_ref,
                    time_from=dt_from,
                    time_to=dt_to,
                )
            except Exception as e:
                st.error(f"Cannot load gains from API: {e}")
                st.stop()

            try:
                data = normalize_gains_response(
                    raw=raw,
                    symbol=symbol,
                    ref_exchange=ref_exchange,
                    volume=volume,
                    fee_source=fee_source,
                    fee_ref=fee_ref,
                )
            except Exception as e:
                st.error(f"Error normalizing /gains response: {e}")
                st.json(raw)
                st.stop()

            st.subheader(
                f"Gains for {symbol} vs {ref_exchange} "
                f"(volume={data['volume_bucket']}, "
                f"fee_source={data['fee_source']}, fee_ref={data['fee_ref']})"
            )

            bid_data = data.get("bid", {})
            ask_data = data.get("ask", {})
            all_exchanges = sorted(set(list(bid_data.keys()) + list(ask_data.keys())))
            base_colors = [
                "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
                "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
            ]
            color_map = {
                ex: base_colors[i % len(base_colors)]
                for i, ex in enumerate(all_exchanges)
            }


            if not bid_data and not ask_data:
                st.info("No BID/ASK data for given parameters.")
                return

            # --- JEDNA FIGURA, 2 RZĘDY, WSPÓLNY X ---
            fig = make_subplots(
                rows=2,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.08,
                subplot_titles=(
                    "BID gain % vs reference",
                    "ASK gain % vs reference",
                ),
            )

            # BID – rząd 1
            if bid_data:
                for ex, points in bid_data.items():
                    fig.add_trace(
                        go.Scatter(
                            x=[p["t"] for p in points],
                            y=[p["gain_pct"] for p in points],
                            mode="lines+markers",
                            name=f"{ex} BID",
                            marker=dict(size=3, color=color_map.get(ex)),
                            line=dict(color=color_map.get(ex)),
                            legendgroup=ex,
                        ),
                        row=1,
                        col=1,
                    )
            else:
                # żeby było widać, że brak danych
                fig.add_annotation(
                    text="No BID data",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.8,
                    showarrow=False,
                    row=1,
                    col=1,
                )

            # ASK – rząd 2
            if ask_data:
                for ex, points in ask_data.items():
                    fig.add_trace(
                        go.Scatter(
                            x=[p["t"] for p in points],
                            y=[p["gain_pct"] for p in points],
                            mode="lines+markers",
                            name=f"{ex} ASK",
                            marker=dict(size=3, color=color_map.get(ex)),
                            line=dict(color=color_map.get(ex), dash="dot"),
                            legendgroup=ex,
                            showlegend=not bid_data,  # jeśli są BID-y, legenda i tak już jest
                        ),
                        row=2,
                        col=1,
                    )
            else:
                fig.add_annotation(
                    text="No ASK data",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.2,
                    showarrow=False,
                    row=2,
                    col=1,
                )

            # Wyraźna linia y=0 na obu subplotach
            fig.add_hline(
                y=0,
                line_width=2,
                line_dash="dash",
                line_color="rgba(200,200,200,0.8)",
                row=1,
                col=1,
            )
            fig.add_hline(
                y=0,
                line_width=2,
                line_dash="dash",
                line_color="rgba(200,200,200,0.8)",
                row=2,
                col=1,
            )

            # Opisy osi
            fig.update_yaxes(title_text="Gain [%] (BID)", row=1, col=1)
            fig.update_yaxes(title_text="Gain [%] (ASK)", row=2, col=1)
            fig.update_xaxes(title_text="Time", row=2, col=1)

            fig.update_layout(
                height=900,
                legend_title_text="Exchange / side",
            )

            st.plotly_chart(fig, use_container_width=True)


def render_nominal_ob_gains_page():
    """
    Page 2: Nominal_gains_in_OB_over_time.

    Wykorzystuje endpoint /nominal_gains i rysuje w czasie:
      - total_positive_gain_usd (nominalny zysk w QUOTE) z całego OB,
      - osobno dla BID i ASK,
      - z możliwością grupowania po instrument / symbol / exchange.

    Uwaga: gainy_$ i total_positive_volume_quote są liczone z całego OB
    niezależnie od bucketu, ale tabela w DB ma kolumnę volume_bucket_quote,
    więc technicznie wybieramy JEDEN bucket tylko po to, żeby odczytać wartości.
    Użytkownik tego nie widzi.
    """
    st.title("Nominal gains in OrderBook over time")

    st.sidebar.header("Parameters")

    # --- SYMBOLS ---
    try:
        symbols = fetch_symbols()
    except Exception as e:
        st.error(f"Cannot load symbols from API: {e}")
        st.stop()

    default_symbol_idx = 0
    if DEFAULT_SYMBOL in symbols:
        default_symbol_idx = symbols.index(DEFAULT_SYMBOL)
    symbol = st.sidebar.selectbox("Symbol", options=symbols, index=default_symbol_idx)

    # --- EXCHANGES (na razie tylko info / future filters) ---
    try:
        exchanges = fetch_exchanges(symbol)
    except Exception as e:
        st.error(f"Cannot load exchanges from API: {e}")
        st.stop()

    if not exchanges:
        st.warning("No exchanges found for this symbol.")
        st.stop()

    # Grouping dimension
    group_by = st.sidebar.selectbox(
        "Group by",
        options=["instrument", "symbol", "exchange"],
        format_func=lambda x: {
            "instrument": "Instrument (EXCHANGE_SYMBOL)",
            "symbol": "Symbol (aggregated across exchanges)",
            "exchange": "Exchange (aggregated across symbols)",
        }[x],
    )

    side_mode = st.sidebar.selectbox(
        "Sides to show",
        options=["BID & ASK", "BID only", "ASK only"],
    )
    if side_mode == "BID & ASK":
        side_param = "BOTH"
    elif side_mode == "BID only":
        side_param = "BID"
    else:
        side_param = "ASK"

    # --- TECHNICZNY bucket volume, NIE pokazujemy w UI ---
    # Musi odpowiadać jednemu z VOLUME_BUCKETS z ETL, np. [100, 500, ..., 5000, ...]
    # Gainy_$ z całego OB i tak są takie same dla każdego bucketu.
    NOMINAL_VOLUME_BUCKET = 5000.0

    # --- TIME RANGE ---
    col1, col2 = st.columns(2)
    default_from = DEFAULT_FROM_DT
    default_to = DEFAULT_TO_DT

    date_from = col1.date_input(
        "From date",
        value=default_from.date(),
        key="nominal_from_date",
    )
    time_from_val = col2.time_input(
        "From time",
        value=default_from.time().replace(microsecond=0),
        key="nominal_from_time",
    )

    col3, col4 = st.columns(2)
    date_to = col3.date_input(
        "To date",
        value=default_to.date(),
        key="nominal_to_date",
    )
    time_to_val = col4.time_input(
        "To time",
        value=default_to.time().replace(microsecond=0),
        key="nominal_to_time",
    )

    dt_from = datetime.combine(date_from, time_from_val).replace(tzinfo=timezone.utc)
    dt_to = datetime.combine(date_to, time_to_val).replace(tzinfo=timezone.utc)

    if dt_to <= dt_from:
        st.error("`To` must be > `From`.")
        st.stop()

    if st.button("Load nominal OB gains"):
        try:
            raw = fetch_nominal_gains(
                symbol=symbol,
                volume=NOMINAL_VOLUME_BUCKET,
                group_by=group_by,
                side=side_param,
                time_from=dt_from,
                time_to=dt_to,
            )
        except Exception as e:
            st.error(f"Cannot load nominal gains from API: {e}")
            st.stop()

        if not raw:
            st.info("No nominal gains data for given parameters.")
            return

        # Strukturujemy odpowiedź: per side, per group_key -> lista punktów
        bid_series: Dict[str, List[Dict[str, Any]]] = {}
        ask_series: Dict[str, List[Dict[str, Any]]] = {}

        for item in raw:
            if not isinstance(item, dict):
                continue

            # snapshot_time
            ts_val = item.get("snapshot_time")
            if isinstance(ts_val, str):
                t_iso = ts_val
            elif isinstance(ts_val, datetime):
                t_iso = ts_val.isoformat()
            else:
                try:
                    t_iso = datetime.fromtimestamp(float(ts_val), tz=timezone.utc).isoformat()
                except Exception:
                    continue

            group_key = str(item.get("group_key"))
            side_val = str(item.get("side", "")).upper()

            gain_usd = float(item.get("total_positive_gain_usd") or 0.0)
            avg_gain_pct = float(item.get("avg_gain_pct_total") or 0.0)
            vol_quote = float(item.get("total_positive_volume_quote") or 0.0)

            point = {
                "t": t_iso,
                "gain_usd": gain_usd,
                "avg_gain_pct": avg_gain_pct,
                "vol_quote": vol_quote,
            }

            if side_val == "BID":
                bid_series.setdefault(group_key, []).append(point)
            elif side_val == "ASK":
                ask_series.setdefault(group_key, []).append(point)

        show_bid = side_mode in ("BID & ASK", "BID only")
        show_ask = side_mode in ("BID & ASK", "ASK only")

        if (not show_bid or not bid_series) and (not show_ask or not ask_series):
            st.info("No data for selected sides / parameters.")
            return

        # Sortujemy po czasie
        for series in (bid_series, ask_series):
            for key, pts in series.items():
                pts.sort(key=lambda p: p["t"])

        all_group_keys = sorted(set(list(bid_series.keys()) + list(ask_series.keys())))
        base_colors = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        ]
        color_map = {
            key: base_colors[i % len(base_colors)]
            for i, key in enumerate(all_group_keys)
        }

        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.5, 0.5],
            subplot_titles=("BID nominal gains (USD)", "ASK nominal gains (USD)"),
        )

        # BID
        if show_bid and bid_series:
            for key, pts in bid_series.items():
                x_vals = [p["t"] for p in pts]
                y_vals = [p["gain_usd"] for p in pts]
                custom = [
                    [p["vol_quote"], p["avg_gain_pct"]]
                    for p in pts
                ]
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=y_vals,
                        mode="lines+markers",
                        name=f"{key} BID",
                        marker=dict(size=3, color=color_map.get(key)),
                        line=dict(color=color_map.get(key)),
                        legendgroup=key,
                        showlegend=True,
                        customdata=custom,
                        hovertemplate=(
                            "Time=%{x}<br>"
                            "Gain=$%{y:.2f}<br>"
                            "Volume=%{customdata[0]:,.0f} quote<br>"
                            "Avg gain=%{customdata[1]:.3%}"
                            "<extra></extra>"
                        ),
                    ),
                    row=1,
                    col=1,
                )

        # ASK
        if show_ask and ask_series:
            for key, pts in ask_series.items():
                x_vals = [p["t"] for p in pts]
                y_vals = [p["gain_usd"] for p in pts]
                custom = [
                    [p["vol_quote"], p["avg_gain_pct"]]
                    for p in pts
                ]
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=y_vals,
                        mode="lines+markers",
                        name=f"{key} ASK",
                        marker=dict(size=3, color=color_map.get(key)),
                        line=dict(color=color_map.get(key), dash="dot"),
                        legendgroup=key,
                        showlegend=not show_bid,
                        customdata=custom,
                        hovertemplate=(
                            "Time=%{x}<br>"
                            "Gain=$%{y:.2f}<br>"
                            "Volume=%{customdata[0]:,.0f} quote<br>"
                            "Avg gain=%{customdata[1]:.3%}"
                            "<extra></extra>"
                        ),
                    ),
                    row=2,
                    col=1,
                )

        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_yaxes(title_text="Total positive nominal gain (USD)", row=1, col=1)
        fig.update_yaxes(title_text="Total positive nominal gain (USD)", row=2, col=1)

        fig.update_layout(
            height=900,
            legend_title_text={
                "instrument": "Instrument",
                "symbol": "Symbol",
                "exchange": "Exchange",
            }[group_by],
        )

        st.plotly_chart(fig, use_container_width=True)



def render_histograms_page():
    """
    Page 3: Histograms.

    3 typy histogramów:
      1) Hist 1 – first line gain (%).
      2) Hist 2 – VWAP gain (% lub $) dla danego volume bucketu.
      3) Hist 3 – total_positive_gain_usd z całego OB.

    Wszystkie:
      - grupowanie po: instrument / symbol / exchange,
      - przełącznik Y: linear / log.

    Dodatkowo:
      - dla SINGLE-grupy + Side=BOTH pokazujemy osobno rozkład BID (czerwony) i ASK (zielony),
      - dla multi-grup – kolory per grupa (instrument/exchange), side może być BOTH lub pojedynczy.
    """
    st.title("OrderBook gain histograms")

    st.sidebar.header("Parameters")

    # --- SYMBOL ---
    try:
        symbols = fetch_symbols()
    except Exception as e:
        st.error(f"Cannot load symbols from API: {e}")
        st.stop()

    default_symbol_idx = 0
    if DEFAULT_SYMBOL in symbols:
        default_symbol_idx = symbols.index(DEFAULT_SYMBOL)
    symbol = st.sidebar.selectbox("Symbol", options=symbols, index=default_symbol_idx)

    # --- EXCHANGES dla tego symbolu ---
    try:
        exchanges = fetch_exchanges(symbol)
    except Exception as e:
        st.error(f"Cannot load exchanges from API: {e}")
        st.stop()

    if not exchanges:
        st.warning("No exchanges found for this symbol.")
        st.stop()

    # Filtry: exchange / instrument
    exchange_filter_label = st.sidebar.selectbox(
        "Filter by source exchange (optional)",
        options=["ALL"] + exchanges,
    )
    exchange_filter = None if exchange_filter_label == "ALL" else exchange_filter_label

    instrument_options = ["ALL"] + [f"{ex}_{symbol}" for ex in exchanges]
    instrument_filter_label = st.sidebar.selectbox(
        "Filter by instrument (EXCHANGE_SYMBOL, optional)",
        options=instrument_options,
    )
    instrument_filter = None if instrument_filter_label == "ALL" else instrument_filter_label

    # group_by dimension
    group_by_options = ['instrument', 'symbol', 'exchange']
    default_group_by_idx = 0
    if DEFAULT_GROUP_BY in group_by_options:
        default_group_by_idx = group_by_options.index(DEFAULT_GROUP_BY)
    group_by = st.sidebar.selectbox(
        "Group by",
        options=group_by_options,
        index=default_group_by_idx,
        format_func=lambda x: {
            "instrument": "Instrument (EXCHANGE_SYMBOL)",
            "symbol": "Symbol (aggregated across exchanges)",
            "exchange": "Exchange (aggregated across symbols)",
        }[x],
    )

    # Histogram type
    hist_type_options = ['First level gain (%)', 'VWAP gain', 'Total OB positive gain ($)']
    default_hist_type_idx = 0
    if DEFAULT_HISTOGRAM_TYPE in hist_type_options:
        default_hist_type_idx = hist_type_options.index(DEFAULT_HISTOGRAM_TYPE)
    hist_type = st.sidebar.selectbox(
        "Histogram type",
        options=hist_type_options,
        index=default_hist_type_idx,
    )

    # Side
    side_options = ['BID', 'ASK', 'BOTH']
    default_side_idx = 0
    if DEFAULT_HISTOGRAM_SIDE in side_options:
        default_side_idx = side_options.index(DEFAULT_HISTOGRAM_SIDE)
    side_label = st.sidebar.selectbox(
        "Side",
        options=side_options,
        index=default_side_idx,
        help="BID/ASK albo BOTH. Dla BOTH przy jednej grupie pokazujemy osobno BID (czerwony) i ASK (zielony).",
    )

    # VWAP-specific options
    metric: str
    show_volume_input = False
    is_pct_metric = False

    if hist_type == "First level gain (%)":
        metric = "first_level_gain_pct"
        show_volume_input = False
        x_label = "First level gain (%)"
        is_pct_metric = True
    elif hist_type == "VWAP gain":
        show_volume_input = True
        vwap_value_mode = st.sidebar.selectbox(
            "VWAP value",
            options=["Percent (%)", "Nominal ($)"],
        )
        if vwap_value_mode == "Percent (%)":
            metric = "vwap_gain_pct"
            x_label = "VWAP gain (%)"
            is_pct_metric = True
        else:
            metric = "vwap_gain_usd"
            x_label = "VWAP gain ($)"
            is_pct_metric = False
    else:  # "Total OB positive gain ($)"
        metric = "total_positive_gain_usd"
        show_volume_input = False
        x_label = "Total positive OB gain ($)"
        is_pct_metric = False

    # Volume bucket
    if show_volume_input:
        volume = st.sidebar.number_input(
            "VWAP volume (quote)",
            min_value=1.0,
            max_value=1_000_000.0,
            value=5_000.0,
            step=100.0,
        )
    else:
        volume = 5_000.0  # techniczny bucket

    # Y-axis scale
    y_scale = st.sidebar.radio(
        "Y axis scale",
        options=["Linear", "Logarithmic"],
        index=0,
    )

    # --- TIME RANGE ---
    col1, col2 = st.columns(2)
    default_from = DEFAULT_FROM_DT
    default_to = DEFAULT_TO_DT

    date_from = col1.date_input(
        "From date",
        value=default_from.date(),
        key="hist_from_date",
    )
    time_from_val = col2.time_input(
        "From time",
        value=default_from.time().replace(microsecond=0),
        key="hist_from_time",
    )

    col3, col4 = st.columns(2)
    date_to = col3.date_input(
        "To date",
        value=default_to.date(),
        key="hist_to_date",
    )
    time_to_val = col4.time_input(
        "To time",
        value=default_to.time().replace(microsecond=0),
        key="hist_to_time",
    )

    dt_from = datetime.combine(date_from, time_from_val).replace(tzinfo=timezone.utc)
    dt_to = datetime.combine(date_to, time_to_val).replace(tzinfo=timezone.utc)

    if dt_to <= dt_from:
        st.error("`To` must be > `From`.")
        st.stop()

    if st.button("Load histogram"):
        series: list[tuple[str, str, list[float]]] = []  # (group_key, side, values)
        all_values: list[float] = []

        try:
            if side_label in ("BID", "ASK"):
                # Jedna strona – jeden call
                raw = fetch_histogram(
                    metric=metric,
                    symbol=symbol,
                    group_by=group_by,
                    side=side_label,
                    time_from=dt_from,
                    time_to=dt_to,
                    volume=volume,
                    exchange=exchange_filter,
                    instrument=instrument_filter,
                )
                calls = [(side_label, raw)]
            else:
                # BOTH – wołamy osobno BID i ASK
                calls = []
                for s in ("BID", "ASK"):
                    raw = fetch_histogram(
                        metric=metric,
                        symbol=symbol,
                        group_by=group_by,
                        side=s,
                        time_from=dt_from,
                        time_to=dt_to,
                        volume=volume,
                        exchange=exchange_filter,
                        instrument=instrument_filter,
                    )
                    calls.append((s, raw))
        except Exception as e:
            st.error(f"Cannot load histogram data from API: {e}")
            st.stop()
            return

        # raw: List[{"group_key": str, "values": [float, ...]}]
        for side_used, raw in calls:
            if not raw:
                continue
            for item in raw:
                group_key = str(item.get("group_key"))
                vals = item.get("values") or []
                if is_pct_metric:
                    vals = [float(v) * 100.0 for v in vals]
                else:
                    vals = [float(v) for v in vals]
                if not vals:
                    continue
                series.append((group_key, side_used, vals))
                all_values.extend(vals)

        if not series:
            st.info("No non-empty histogram series.")
            return

        v_min = min(all_values)
        v_max = max(all_values)
        if v_min == v_max:
            v_min -= 1.0
            v_max += 1.0

        # Bin sizing
        if is_pct_metric:
            # stały krok 0.05 p.p. (0.05% = 5 bps), alignujemy do siatki
            step = 0.02
            from math import floor, ceil
            start = floor(v_min / step) * step
            end = ceil(v_max / step) * step
            bin_size = step
            v_min, v_max = start, end
        else:
            # nominalne wartości – ~40 binów na cały zakres
            bin_size = (v_max - v_min) / 100.0 if v_max > v_min else 1.0

        group_keys = sorted({g for g, _, _ in series})
        single_group = len(group_keys) == 1

        base_colors = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        ]

        fig = go.Figure()

        for group_key, s_used, vals in series:
            # Kolory:
            if single_group and side_label == "BOTH":
                # Specjalny tryb: jedna grupa, osobno BID/ASK
                if s_used == "BID":
                    color = "#d62728"  # red
                    name = "BID (sell opportunities)"
                else:
                    color = "#2ca02c"  # green
                    name = "ASK (buy opportunities)"
            else:
                # Tryb multi-grup: kolory per group_key, side tylko w nazwie
                idx = group_keys.index(group_key)
                color = base_colors[idx % len(base_colors)]
                if side_label == "BOTH":
                    name = f"{group_key} {s_used}"
                else:
                    name = group_key

            fig.add_trace(
                go.Histogram(
                    x=vals,
                    name=name,
                    opacity=0.6,
                    marker=dict(color=color),
                    autobinx=False,
                    xbins=dict(start=v_min, end=v_max, size=bin_size),
                )
            )

        fig.update_layout(
            barmode="overlay",
            legend_title_text={
                "instrument": "Instrument",
                "symbol": "Symbol",
                "exchange": "Exchange",
            }[group_by],
            xaxis_title=x_label,
            yaxis_title="Hits (snapshot count)",
            height=750,
        )

        fig.update_yaxes(type="log" if y_scale == "Logarithmic" else "linear")

        st.plotly_chart(fig, use_container_width=True)


def main():
    # Globalna konfiguracja strony
    st.set_page_config(
        page_title="OrderBook Analytics",
        layout="wide",
    )

    st.sidebar.title("View")
    view_options = (
        "VWAP Gains (current)",
        "Nominal OB Gains",
        "Histograms",
    )
    default_view_idx = 0
    if DEFAULT_VIEW in view_options:
        default_view_idx = view_options.index(DEFAULT_VIEW)
    view = st.sidebar.radio(
        "Select dashboard view:",
        view_options,
        index=default_view_idx,
    )

    if view == "VWAP Gains (current)":
        render_vwap_gains_page()
    elif view == "Nominal OB Gains":
        render_nominal_ob_gains_page()
    elif view == "Histograms":
        render_histograms_page()
    else:
        st.error(f"Unknown view: {view}")


if __name__ == "__main__":
    main()