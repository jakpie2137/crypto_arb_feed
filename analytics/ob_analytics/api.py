from datetime import datetime
from typing import Dict, List, Optional, Literal

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from .db import db_conn
from .config import ApiConfig


app = FastAPI(title="OB Analytics API", version="0.1.0")


class SymbolList(BaseModel):
    symbols: List[str]


class ExchangeList(BaseModel):
    exchanges: List[str]


class GainPoint(BaseModel):
    snapshot_id: int
    snapshot_time: datetime
    exchange: str
    gain_bid_pct: Optional[float]
    gain_ask_pct: Optional[float]


class GainSeries(BaseModel):
    symbol: str
    ref_exchange: str
    volume: float
    points: List[GainPoint]

class NominalGainPoint(BaseModel):
    snapshot_time: datetime
    group_key: str
    side: str
    total_positive_gain_usd: float
    total_positive_volume_quote: float
    avg_gain_pct_total: float

class HistogramSeries(BaseModel):
    group_key: str
    values: List[float]


@app.get("/health")
def health() -> Dict[str, str]:
    """
    Prosty endpoint zdrowotny.
    """
    return {"status": "ok"}


@app.get("/symbols", response_model=SymbolList)
def list_symbols(exchange: Optional[str] = Query(None)) -> SymbolList:
    """
    Lista dostępnych symboli. Opcjonalnie można zawęzić do danej giełdy.
    """
    with db_conn() as conn, conn.cursor() as cur:
        if exchange:
            cur.execute(
                """
                SELECT DISTINCT symbol
                FROM market_data.orderbook_agg
                WHERE exchange = %s
                ORDER BY symbol
                """,
                (exchange,),
            )
        else:
            cur.execute(
                """
                SELECT DISTINCT symbol
                FROM market_data.orderbook_agg
                ORDER BY symbol
                """
            )
        rows = cur.fetchall()

    return SymbolList(symbols=[r[0] for r in rows])


@app.get("/exchanges", response_model=ExchangeList)
def list_exchanges(symbol: str = Query(...)) -> ExchangeList:
    """
    Lista giełd dla danego symbolu.
    """
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT exchange
            FROM market_data.orderbook_agg
            WHERE symbol = %s
            ORDER BY exchange
            """,
            (symbol,),
        )
        rows = cur.fetchall()

    return ExchangeList(exchanges=[r[0] for r in rows])


def _parse_iso_ts(value: Optional[str]) -> Optional[datetime]:
    """
    Parsuje timestamp w ISO 8601 do obiektu datetime.
    Zwraca None, jeśli value jest None lub puste.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid datetime: {value}",
        ) from e


@app.get("/nominal_gains", response_model=List[NominalGainPoint])
def nominal_gains(
    symbol: Optional[str] = Query(
        None, description="Filtr po symbolu (np. DOGEUSDT)"
    ),
    exchange: Optional[str] = Query(
        None, description="Filtr po giełdzie źródłowej (np. MEXC)"
    ),
    instrument: Optional[str] = Query(
        None, description="Filtr po instrumencie (np. MEXC_DOGEUSDT)"
    ),
    side: Literal["BID", "ASK", "BOTH"] = Query(
        "BOTH", description="Którą stronę OB brać pod uwagę"
    ),
    group_by: Literal["instrument", "symbol", "exchange"] = Query(
        "instrument",
        description="Po czym grupować serię: instrument / symbol / exchange",
    ),
    volume: float = Query(
        ...,
        description=(
            "Wolumen w QUOTE; musi odpowiadać bucketowi "
            "volume_bucket_quote w ETL (np. 100, 500, 1000)"
        ),
    ),
    start_time: Optional[str] = Query(
        None,
        alias="time_from",
        description="Początek zakresu czasu (ISO 8601, np. 2025-12-08T07:40:00)",
    ),
    end_time: Optional[str] = Query(
        None,
        alias="time_to",
        description="Koniec zakresu czasu (ISO 8601)",
    ),
) -> List[NominalGainPoint]:
    """
    Zwraca historyczne NOMINALNE zyski z całego orderbooka (tylko pozytywne linie),
    zagregowane po czasie + wybranym wymiarze (instrument/symbol/exchange).

    Dane pochodzą z tabeli market_data.orderbook_gains i bazują na:
      - total_positive_gain_usd
      - total_positive_volume_quote
      - avg_gain_pct_total (wyliczamy ponownie po agregacji)
    """
    start_dt = _parse_iso_ts(start_time)
    end_dt = _parse_iso_ts(end_time)

    # decydujemy po której kolumnie grupujemy
    group_col_map = {
        "instrument": "instrument",
        "symbol": "symbol",
        "exchange": "exchange",
    }
    group_col = group_col_map[group_by]

    with db_conn() as conn, conn.cursor() as cur:
        where_clauses: List[str] = []
        params: List[object] = []

        # wymagamy spójnego volume bucketu
        where_clauses.append("volume_bucket_quote = %s")
        params.append(float(volume))

        if symbol:
            where_clauses.append("symbol = %s")
            params.append(symbol)
        if exchange:
            where_clauses.append("exchange = %s")
            params.append(exchange)
        if instrument:
            where_clauses.append("instrument = %s")
            params.append(instrument)

        if side != "BOTH":
            where_clauses.append("side = %s")
            params.append(side)

        if start_dt:
            where_clauses.append("snapshot_time >= %s")
            params.append(start_dt)
        if end_dt:
            where_clauses.append("snapshot_time <= %s")
            params.append(end_dt)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        query = f"""
            SELECT
                snapshot_time,
                {group_col} AS group_key,
                side,
                SUM(total_positive_gain_usd) AS total_positive_gain_usd,
                SUM(total_positive_volume_quote) AS total_positive_volume_quote,
                CASE
                    WHEN SUM(total_positive_volume_quote) > 0
                    THEN SUM(total_positive_gain_usd) / SUM(total_positive_volume_quote)
                    ELSE 0
                END AS avg_gain_pct_total
            FROM market_data.orderbook_gains
            {where_sql}
            GROUP BY snapshot_time, {group_col}, side
            ORDER BY snapshot_time, {group_col}, side
        """

        cur.execute(query, params)
        rows = cur.fetchall()

    if not rows:
        return []

    result: List[NominalGainPoint] = []
    for (
        snapshot_time,
        group_key,
        side_value,
        total_positive_gain_usd,
        total_positive_volume_quote,
        avg_gain_pct_total,
    ) in rows:
        result.append(
            NominalGainPoint(
                snapshot_time=snapshot_time,
                group_key=group_key,
                side=side_value,
                total_positive_gain_usd=float(total_positive_gain_usd or 0.0),
                total_positive_volume_quote=float(
                    total_positive_volume_quote or 0.0
                ),
                avg_gain_pct_total=float(avg_gain_pct_total or 0.0),
            )
        )

    return result


@app.get("/histogram", response_model=List[HistogramSeries])
def histogram(
    metric: Literal[
        "first_level_gain_pct",
        "vwap_gain_pct",
        "vwap_gain_usd",
        "total_positive_gain_usd",
    ] = Query(
        ...,
        description=(
            "Którą metrykę histogramować: "
            "first_level_gain_pct, vwap_gain_pct, vwap_gain_usd, total_positive_gain_usd"
        ),
    ),
    symbol: Optional[str] = Query(
        None, description="Filtr po symbolu (np. DOGEUSDT)"
    ),
    exchange: Optional[str] = Query(
        None, description="Filtr po giełdzie źródłowej (np. MEXC)"
    ),
    instrument: Optional[str] = Query(
        None, description="Filtr po instrumencie (np. MEXC_DOGEUSDT)"
    ),
    ref_exchange: Optional[str] = Query(
        None, description="Filtr po giełdzie referencyjnej (np. binance_fut)"
    ),
    side: Literal["BID", "ASK", "BOTH"] = Query(
        "BOTH",
        description=(
            "Którą stronę OB brać: BID, ASK, BOTH (BOTH = mieszamy wartości z obu stron)"
        ),
    ),
    group_by: Literal["instrument", "symbol", "exchange"] = Query(
        "instrument",
        description="Po czym grupować dane do histogramu: instrument / symbol / exchange",
    ),
    volume: float = Query(
        ...,
        description=(
            "Wolumen w QUOTE; wybiera odpowiedni bucket "
            "volume_bucket_quote (np. 100, 500, 1000). "
            "Dla metryk niezależnych od volume (first_level, total_positive_gain) "
            "wartości będą identyczne dla wszystkich bucketów, ale tu wybieramy jeden,"
            "żeby uniknąć duplikacji."
        ),
    ),
    start_time: Optional[str] = Query(
        None,
        alias="time_from",
        description="Początek zakresu czasu (ISO 8601, np. 2025-12-08T07:40:00)",
    ),
    end_time: Optional[str] = Query(
        None,
        alias="time_to",
        description="Koniec zakresu czasu (ISO 8601)",
    ),
) -> List[HistogramSeries]:
    """
    Generuje dane do histogramów na podstawie tabeli market_data.orderbook_gains.

    Zwraca po jednej serii na każdą grupę (instrument/symbol/exchange):
      - group_key: wartość grupy (np. 'MEXC_DOGEUSDT' albo 'DOGEUSDT' albo 'MEXC')
      - values: lista wartości metryki (float), które dashboard wrzuci do histogramu.

    Uwaga:
      - zawsze filtrujemy po volume_bucket_quote = volume,
        żeby uniknąć powielania tych samych wartości przy wielu bucketach.
    """
    start_dt = _parse_iso_ts(start_time)
    end_dt = _parse_iso_ts(end_time)

    metric_col_map = {
        "first_level_gain_pct": "first_level_gain_pct",
        "vwap_gain_pct": "vwap_gain_pct",
        "vwap_gain_usd": "vwap_gain_usd",
        "total_positive_gain_usd": "total_positive_gain_usd",
    }
    metric_col = metric_col_map[metric]

    group_col_map = {
        "instrument": "instrument",
        "symbol": "symbol",
        "exchange": "exchange",
    }
    group_col = group_col_map[group_by]

    with db_conn() as conn, conn.cursor() as cur:
        where_clauses: List[str] = []
        params: List[object] = []

        # bucket wolumenowy – zawsze wymagany
        where_clauses.append("volume_bucket_quote = %s")
        params.append(float(volume))

        if symbol:
            where_clauses.append("symbol = %s")
            params.append(symbol)
        if exchange:
            where_clauses.append("exchange = %s")
            params.append(exchange)
        if instrument:
            where_clauses.append("instrument = %s")
            params.append(instrument)
        if ref_exchange:
            where_clauses.append("ref_exchange = %s")
            params.append(ref_exchange)

        if side != "BOTH":
            where_clauses.append("side = %s")
            params.append(side)

        if start_dt:
            where_clauses.append("snapshot_time >= %s")
            params.append(start_dt)
        if end_dt:
            where_clauses.append("snapshot_time <= %s")
            params.append(end_dt)

        # pomijamy nullowe metryki – nie mają sensu w histogramie
        where_clauses.append(f"{metric_col} IS NOT NULL")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        query = f"""
            SELECT
                {group_col} AS group_key,
                array_agg({metric_col}) AS values
            FROM market_data.orderbook_gains
            {where_sql}
            GROUP BY {group_col}
            ORDER BY {group_col}
        """

        cur.execute(query, params)
        rows = cur.fetchall()

    result: List[HistogramSeries] = []
    for group_key, values in rows:
        # psycopg2 zamieni array_agg na listę Pythona – ale dla pewności rzutujemy
        vals = [float(v) for v in (values or [])]
        if not vals:
            continue
        result.append(
            HistogramSeries(
                group_key=str(group_key),
                values=vals,
            )
        )

    return result


@app.get("/gains", response_model=List[GainSeries])
def gains_per_symbol(
    symbol: str = Query(...),
    ref_exchange: str = Query(..., description="Giełda referencyjna"),
    volume: float = Query(
        ..., description="Wolumen w QUOTE – musi pasować do bucketu VWAP"
    ),
    exchanges: Optional[List[str]] = Query(
        None, description="Lista giełd źródłowych; domyślnie wszystkie poza referencyjną"
    ),
    start_time: Optional[str] = Query(
        None,
        alias="time_from",
        description="Początek zakresu czasu (ISO 8601, np. 2024-01-01T00:00:00)",
    ),
    end_time: Optional[str] = Query(
        None,
        alias="time_to",
        description="Koniec zakresu czasu (ISO 8601, np. 2024-01-01T23:59:59)",
    ),
) -> List[GainSeries]:
    """
    Zwraca historyczne gainy per giełda vs giełda referencyjna
    dla zadanego symbolu i wolumenu VWAP.
    """
    start_dt = _parse_iso_ts(start_time)
    end_dt = _parse_iso_ts(end_time)

    volume_key = str(int(volume))

    with db_conn() as conn, conn.cursor() as cur:
        # najpierw sprawdzamy, czy ref_exchange istnieje
        cur.execute(
            """
            SELECT 1
            FROM market_data.orderbook_agg
            WHERE symbol = %s AND exchange = %s
            LIMIT 1
            """,
            (symbol, ref_exchange),
        )
        if not cur.fetchone():
            raise HTTPException(
                status_code=404,
                detail=f"No data for symbol={symbol} on ref_exchange={ref_exchange}",
            )

        # wybieramy wszystkie snapshoty dla symbolu
        params = [symbol]
        time_filter = ""
        if start_dt:
            time_filter += " AND snapshot_time >= %s"
            params.append(start_dt)
        if end_dt:
            time_filter += " AND snapshot_time <= %s"
            params.append(end_dt)

        cur.execute(
            f"""
            SELECT
                snapshot_id,
                snapshot_time,
                exchange,
                vwap_bid,
                vwap_ask
            FROM market_data.orderbook_agg
            WHERE symbol = %s
            {time_filter}
            ORDER BY snapshot_time, exchange
            """,
            params,
        )
        rows = cur.fetchall()

    if not rows:
        return []

    from collections import defaultdict

    # by_time[ts][exchange] = {...}
    by_time = defaultdict(dict)
    for snapshot_id, snapshot_time, exchange, vwap_bid_json, vwap_ask_json in rows:
        # vwap_* mogą być dict (JSONB) lub stringiem zawierającym JSON
        vwap_bid = None
        vwap_ask = None

        try:
            # wariant: driver zwraca JSON jako dict
            if isinstance(vwap_bid_json, dict):
                vwap_bid = vwap_bid_json.get(volume_key)
            if isinstance(vwap_ask_json, dict):
                vwap_ask = vwap_ask_json.get(volume_key)
        except AttributeError:
            # teoretycznie nie powinno się zdarzyć przy powyższym isinstance,
            # ale zostawiamy fallback na string JSON
            pass

        # fallback: jeśli jednak dostaliśmy stringi JSON
        if (vwap_bid is None and isinstance(vwap_bid_json, str)) or (
            vwap_ask is None and isinstance(vwap_ask_json, str)
        ):
            import json as _json

            try:
                vwap_bid_data = (
                    _json.loads(vwap_bid_json) if isinstance(vwap_bid_json, str) else {}
                )
            except Exception:
                vwap_bid_data = {}
            try:
                vwap_ask_data = (
                    _json.loads(vwap_ask_json) if isinstance(vwap_ask_json, str) else {}
                )
            except Exception:
                vwap_ask_data = {}

            if vwap_bid is None:
                vwap_bid = vwap_bid_data.get(volume_key)
            if vwap_ask is None:
                vwap_ask = vwap_ask_data.get(volume_key)

        by_time[snapshot_time][exchange] = {
            "snapshot_id": snapshot_id,
            "vwap_bid": vwap_bid,
            "vwap_ask": vwap_ask,
        }

    # jakie giełdy bierzemy pod uwagę
    all_exchanges = set(
        exch for _, _, exch, _, _ in rows  # type: ignore[misc]
    )
    if exchanges is None:
        src_exchanges = sorted(e for e in all_exchanges if e != ref_exchange)
    else:
        src_exchanges = sorted(set(exchanges) - {ref_exchange})

    if not src_exchanges:
        return []

    series_map: Dict[str, List[GainPoint]] = {ex: [] for ex in src_exchanges}

    for ts in sorted(by_time.keys()):
        snapshot = by_time[ts]
        ref = snapshot.get(ref_exchange)
        if not ref:
            continue

        ref_bid = ref.get("vwap_bid")
        ref_ask = ref.get("vwap_ask")
        if ref_bid is None or ref_ask is None:
            continue

        for ex in src_exchanges:
            src = snapshot.get(ex)
            if not src:
                continue

            src_bid = src.get("vwap_bid")
            src_ask = src.get("vwap_ask")
            if src_bid is None or src_ask is None:
                continue

            # Bid: kupujemy na ref (ask), sprzedajemy na src (bid)
            gain_bid_pct = (src_bid - ref_ask) / ref_ask * 100.0 if ref_ask > 0 else None
            # Ask: kupujemy na src (ask), sprzedajemy na ref (bid)
            gain_ask_pct = (ref_bid - src_ask) / src_ask * 100.0 if src_ask > 0 else None

            series_map[ex].append(
                GainPoint(
                    snapshot_id=int(src["snapshot_id"]),
                    snapshot_time=ts,
                    exchange=ex,
                    gain_bid_pct=gain_bid_pct,
                    gain_ask_pct=gain_ask_pct,
                )
            )

    result: List[GainSeries] = []
    for ex, points in series_map.items():
        if not points:
            continue
        result.append(
            GainSeries(
                symbol=symbol,
                ref_exchange=ref_exchange,
                volume=volume,
                points=points,
            )
        )
    return result


def main() -> None:
    """
    Umożliwia uruchomienie: `python -m ob_analytics.api`
    """
    import uvicorn

    cfg = ApiConfig()
    uvicorn.run(
        "ob_analytics.api:app",
        host=cfg.host,
        port=cfg.port,
        reload=False,
    )


if __name__ == "__main__":
    main()
