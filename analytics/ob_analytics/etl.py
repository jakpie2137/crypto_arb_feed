import json
import time
from typing import Dict, List, Any, Tuple, Optional

from .db import db_conn, fetchone_dict
from .config import EtlConfig


# Buckety wolumenowe w QUOTE (np. USD/USDT)
VOLUME_BUCKETS = [100, 500, 1000, 2500, 5000, 10_000, 25_000, 50_000, 100_000]

from .gain_calc import _vwap_for_volume, compute_first_level_gain, compute_vwap_gain, compute_total_positive_gain

def _ensure_state_table(conn) -> None:
    """
    Tworzy (jeśli trzeba) tabelę market_data.orderbook_etl_state i inicjalny wiersz.
    """
    with conn.cursor() as cur:
        # Tabela stanu w schemacie market_data
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data.orderbook_etl_state (
                id               INTEGER PRIMARY KEY,
                last_snapshot_id BIGINT NOT NULL
            )
            """
        )
        conn.commit()

        cur.execute(
            "SELECT last_snapshot_id FROM market_data.orderbook_etl_state WHERE id = 1"
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO market_data.orderbook_etl_state (id, last_snapshot_id) VALUES (1, 0)"
            )
            conn.commit()


def _get_last_snapshot_id(conn) -> int:
    row = fetchone_dict(
        conn,
        "SELECT last_snapshot_id FROM market_data.orderbook_etl_state WHERE id = 1",
    )
    if row is None:
        # Na wszelki wypadek – powinno być ogarnięte w _ensure_state_table
        return 0
    return int(row["last_snapshot_id"])


def _update_last_snapshot_id(conn, new_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE market_data.orderbook_etl_state
            SET last_snapshot_id = %s
            WHERE id = 1
            """,
            (int(new_id),),
        )
    conn.commit()


def _normalize_levels(raw: Any) -> List[Dict[str, float]]:
    """
    Pg8000 może zwrócić JSONB jako:
    - str
    - bytes
    - już zdekodowaną listę
    Ta funkcja zwraca zawsze listę dictów.
    """
    if raw is None:
        return []

    # Jeśli to już jest lista – bierzemy jak jest
    if isinstance(raw, list):
        return raw

    # Jeśli bytes -> decode do str
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")

    if isinstance(raw, str):
        if raw == "":
            return []
        return json.loads(raw)

    # Jak coś innego – próbujemy brutalnie z json.dumps/loads,
    # ale w praktyce nie powinno być potrzebne
    return json.loads(json.dumps(raw))


def process_batch(cfg: EtlConfig) -> int:
    """
    Pobiera partię nowych snapshotów z market_data.orderbook_snapshots,
    liczy VWAP-y dla zdefiniowanych bucketów i zapisuje do market_data.orderbook_agg,
    a dodatkowo liczy gainy vs giełda referencyjna i zapisuje do market_data.orderbook_gains.

    Zwraca ile wierszy (snapshotów) zostało przetworzonych.
    """
    from .db import db_conn  # lokalny import, żeby uniknąć cykli (jakby co)

    with db_conn() as conn:
        _ensure_state_table(conn)
        last_id = _get_last_snapshot_id(conn)

        # 1) Bierzemy świeże snapshoty
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    snapshot_time,
                    exchange,
                    symbol,
                    bids,
                    asks
                FROM market_data.orderbook_snapshots
                WHERE id > %s
                ORDER BY id
                LIMIT %s
                """,
                (last_id, cfg.batch_size),
            )
            rows = cur.fetchall()

        if not rows:
            return 0

        records_to_insert = []         # do orderbook_agg
        gain_records_to_insert = []    # do orderbook_gains
        max_id = last_id

        for row in rows:
            (
                snapshot_id,
                snapshot_time,
                exchange,
                symbol,
                bids_json,
                asks_json,
            ) = row

            if snapshot_id > max_id:
                max_id = snapshot_id

            # bids malejąco po cenie, asks rosnąco
            bids = _normalize_levels(bids_json)
            asks = _normalize_levels(asks_json)

            bids = sorted(bids, key=lambda x: float(x["p"]), reverse=True)
            asks = sorted(asks, key=lambda x: float(x["p"]))

            # Best bid / ask do szybkiego wglądu
            best_bid = float(bids[0]["p"]) if bids else None
            best_ask = float(asks[0]["p"]) if asks else None

            vwap_bid_map: Dict[str, Any] = {}
            vwap_ask_map: Dict[str, Any] = {}

            for vol in VOLUME_BUCKETS:
                vwap_bid = _vwap_for_volume(bids, vol)
                vwap_ask = _vwap_for_volume(asks, vol)
                # zapisujemy tylko jeśli da się policzyć; inaczej None
                vwap_bid_map[str(vol)] = vwap_bid
                vwap_ask_map[str(vol)] = vwap_ask

            # Rekord do orderbook_agg (jak wcześniej)
            records_to_insert.append(
                (
                    snapshot_id,
                    snapshot_time,
                    exchange,
                    symbol,
                    best_bid,
                    best_ask,
                    json.dumps(vwap_bid_map),
                    json.dumps(vwap_ask_map),
                )
            )

            # --- NOWE: liczymy gainy vs giełda referencyjna i budujemy rekordy do orderbook_gains ---

            # Ref exchange – nie liczymy gainów względem samego siebie
            if exchange.upper() == cfg.ref_exchange:
                continue

            # Szukamy ostatniego dostępnego orderbooka z giełdy referencyjnej
            with conn.cursor() as ref_cur:
                ref_cur.execute(
                    """
                    SELECT bids, asks
                    FROM market_data.orderbook_snapshots
                    WHERE symbol = %s
                      AND exchange = %s
                      AND snapshot_time <= %s
                    ORDER BY snapshot_time DESC
                    LIMIT 1
                    """,
                    (symbol, cfg.ref_exchange, snapshot_time),
                )
                ref_row = ref_cur.fetchone()

            if not ref_row:
                # brak referencyjnego OB wstecz – nie liczymy gainów
                continue

            ref_bids_json, ref_asks_json = ref_row
            ref_bids = _normalize_levels(ref_bids_json)
            ref_asks = _normalize_levels(ref_asks_json)

            ref_bids = sorted(ref_bids, key=lambda x: float(x["p"]), reverse=True)
            ref_asks = sorted(ref_asks, key=lambda x: float(x["p"]))

            ref_bid = float(ref_bids[0]["p"]) if ref_bids else None  # najlepszy BID ref
            ref_ask = float(ref_asks[0]["p"]) if ref_asks else None  # najlepszy ASK ref

            if ref_bid is None or ref_ask is None:
                continue

            instrument = f"{exchange}_{symbol}"
            fees_pct = cfg.total_fees_pct

            # 1st level gain (per side)
            first_bid_gain = (
                compute_first_level_gain(
                    source_best=best_bid,
                    ref_best=ref_ask,  # kupujemy na ref (ASK), sprzedajemy na src (BID)
                    side="BID",
                    fees_pct=fees_pct,
                )
                if best_bid is not None
                else None
            )
            first_ask_gain = (
                compute_first_level_gain(
                    source_best=best_ask,
                    ref_best=ref_bid,  # kupujemy na src (ASK), sprzedajemy na ref (BID)
                    side="ASK",
                    fees_pct=fees_pct,
                )
                if best_ask is not None
                else None
            )

            # Total positive gain z całego OB (per side)
            bid_total_gain_usd, bid_total_vol_quote, bid_avg_gain_pct = compute_total_positive_gain(
                source_levels=bids,
                ref_best=ref_ask,
                side="BID",
                fees_pct=fees_pct,
            )
            ask_total_gain_usd, ask_total_vol_quote, ask_avg_gain_pct = compute_total_positive_gain(
                source_levels=asks,
                ref_best=ref_bid,
                side="ASK",
                fees_pct=fees_pct,
            )

            # VWAP gain per volume bucket (per side)
            for vol in VOLUME_BUCKETS:
                vol_float = float(vol)

                vwap_bid_gain_pct, vwap_bid_gain_usd = compute_vwap_gain(
                    source_levels=bids,
                    ref_best=ref_ask,
                    side="BID",
                    volume_quote=vol_float,
                    fees_pct=fees_pct,
                )
                vwap_ask_gain_pct, vwap_ask_gain_usd = compute_vwap_gain(
                    source_levels=asks,
                    ref_best=ref_bid,
                    side="ASK",
                    volume_quote=vol_float,
                    fees_pct=fees_pct,
                )

                # BID row
                gain_records_to_insert.append(
                    (
                        snapshot_id,
                        snapshot_time,
                        exchange,
                        symbol,
                        instrument,
                        cfg.ref_exchange,
                        "BID",
                        vol_float,
                        first_bid_gain,
                        vwap_bid_gain_pct,
                        vwap_bid_gain_usd,
                        bid_total_gain_usd,
                        bid_total_vol_quote,
                        bid_avg_gain_pct,
                    )
                )

                # ASK row
                gain_records_to_insert.append(
                    (
                        snapshot_id,
                        snapshot_time,
                        exchange,
                        symbol,
                        instrument,
                        cfg.ref_exchange,
                        "ASK",
                        vol_float,
                        first_ask_gain,
                        vwap_ask_gain_pct,
                        vwap_ask_gain_usd,
                        ask_total_gain_usd,
                        ask_total_vol_quote,
                        ask_avg_gain_pct,
                    )
                )

        # 2) Upsert do orderbook_agg
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO market_data.orderbook_agg (
                    snapshot_id,
                    snapshot_time,
                    exchange,
                    symbol,
                    best_bid,
                    best_ask,
                    vwap_bid,
                    vwap_ask
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_id, exchange, symbol) DO UPDATE
                SET
                    best_bid = EXCLUDED.best_bid,
                    best_ask = EXCLUDED.best_ask,
                    vwap_bid = EXCLUDED.vwap_bid,
                    vwap_ask = EXCLUDED.vwap_ask
                """,
                records_to_insert,
            )

        # 3) Upsert do orderbook_gains (jeśli coś jest do wstawienia)
        if gain_records_to_insert:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO market_data.orderbook_gains (
                        snapshot_id,
                        snapshot_time,
                        exchange,
                        symbol,
                        instrument,
                        ref_exchange,
                        side,
                        volume_bucket_quote,
                        first_level_gain_pct,
                        vwap_gain_pct,
                        vwap_gain_usd,
                        total_positive_gain_usd,
                        total_positive_volume_quote,
                        avg_gain_pct_total
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (snapshot_id, exchange, symbol, side, volume_bucket_quote) DO UPDATE
                    SET
                        first_level_gain_pct        = EXCLUDED.first_level_gain_pct,
                        vwap_gain_pct               = EXCLUDED.vwap_gain_pct,
                        vwap_gain_usd               = EXCLUDED.vwap_gain_usd,
                        total_positive_gain_usd     = EXCLUDED.total_positive_gain_usd,
                        total_positive_volume_quote = EXCLUDED.total_positive_volume_quote,
                        avg_gain_pct_total          = EXCLUDED.avg_gain_pct_total
                    """,
                    gain_records_to_insert,
                )

        _update_last_snapshot_id(conn, max_id)
        return len(rows)

def run_forever(poll_interval_sec: float = 1.0) -> None:
    """
    Główny loop ETL-a – ciśnie do oporu, jak nie ma danych, robi krótki sleep.
    """
    cfg = EtlConfig()
    while True:
        processed = process_batch(cfg)
        if processed == 0:
            time.sleep(poll_interval_sec)


if __name__ == "__main__":
    run_forever()
