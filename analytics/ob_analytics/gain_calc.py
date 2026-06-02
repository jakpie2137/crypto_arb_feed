
from typing import Dict, List, Tuple, Any

"""

Pure gain / VWAP math helpers extracted from etl.py so they can be reused
both by the ETL pipeline and by GUI tools (like the live "big gain detector").

All prices are assumed to be positive floats, all gains are returned as fractions
(e.g. 0.01 = 1%).
"""


def _vwap_for_volume(levels: List[Dict[str, float]], target_quote: float) -> float:
    """
    VWAP po tej stronie OB dla zadanego wolumenu w QUOTE.
    levels: lista {"p": price, "s": size}, posortowana w odpowiednim kierunku.
    target_quote: wolumen w QUOTE (np. USDT), który chcemy "przehandlować".
    Zwraca cenę VWAP albo None jeśli w OB jest za mało wolumenu.
    """
    remaining = float(target_quote)
    if remaining <= 0:
        return None

    notional_sum = 0.0
    qty_sum = 0.0

    for level in levels:
        price = float(level["p"])
        size_base = float(level["s"])
        level_notional = price * size_base

        if level_notional >= remaining:
            # Wystarczy część tej linii
            qty = remaining / price
            notional_sum += remaining
            qty_sum += qty
            remaining = 0.0
            break
        else:
            notional_sum += level_notional
            qty_sum += size_base
            remaining -= level_notional

    if remaining > 0 or qty_sum <= 0:
        return None

    return notional_sum / qty_sum


def _normalize_side(side: str) -> str:
    """Helper – normalizuje side i waliduje."""
    s = side.upper()
    if s not in ("BID", "ASK"):
        raise ValueError(f"Invalid side '{side}', expected 'BID' or 'ASK'")
    return s


def _gain_from_prices(
    source_price: float,
    ref_price: float,
    side: str,
    fees_pct: float = 0.0,
) -> float:
    """
    Liczy teoretyczny % gain (jako ułamek, np. 0.01 = 1%)
    z pary (source_price, ref_price) dla danej strony OB.

    - side == "ASK": kupujemy na source (ASK), sprzedajemy na ref (BID)
      raw_gain = ref / source - 1
    - side == "BID": sprzedajemy na source (BID), odkupujemy na ref (ASK)
      raw_gain = source / ref - 1

    fees_pct: łączny koszt (taker source + taker ref + inne) jako ułamek.
    """
    side = _normalize_side(side)

    if source_price <= 0 or ref_price <= 0:
        return 0.0

    if side == "ASK":
        raw_gain = ref_price / source_price - 1.0
    else:  # BID
        raw_gain = source_price / ref_price - 1.0

    gain_pct = raw_gain - (fees_pct or 0.0)
    return gain_pct


def compute_first_level_gain(
    source_best: float,
    ref_best: float,
    side: str,
    fees_pct: float = 0.0,
) -> float:
    """
    Gain dla pierwszego poziomu OB:

    - source_best: best_bid albo best_ask z giełdy source (w zależności od side)
    - ref_best: odpowiedni best z giełdy referencyjnej
    - side: "BID" lub "ASK"
    - fees_pct: łączny koszt w ułamku (np. 0.002 = 0.2%)

    Zwraca gain jako ułamek (0.01 = 1%).
    Może być dodatni, zerowy lub ujemny – nie tnę tego do 0,
    bo w niektórych miejscach chcesz wiedzieć, że jest -0.3%.
    """
    return _gain_from_prices(source_best, ref_best, side, fees_pct)


def compute_vwap_gain(
    source_levels: List[Dict[str, float]],
    ref_best: float,
    side: str,
    volume_quote: float,
    fees_pct: float = 0.0,
) -> Tuple[float, float]:
    """
    Gain z VWAP-a dla zadanego wolumenu w QUOTE.

    - source_levels: lista poziomów OB po stronie source:
        [{"p": price, "s": size}, ...]
      Uwaga: zakładam, że są JUŻ posortowane we właściwą stronę
      (tak jak oczekuje _vwap_for_volume).
    - ref_best: best price z giełdy referencyjnej (bid dla ASK side, ask dla BID side).
    - side: "BID" / "ASK"
    - volume_quote: ile QUOTE (np. USDT) chcemy "przepchnąć" przez VWAP.
    - fees_pct: łączny koszt w ułamku.

    Zwraca:
      (gain_pct, gain_usd)

    gdzie:
      gain_pct – ułamek, np. 0.01 = 1%
      gain_usd – nominalny zysk w QUOTE (volume_quote * gain_pct)

    Jeśli brakuje depthu / coś jest nie tak → (0.0, 0.0).
    """
    side = _normalize_side(side)

    if volume_quote <= 0 or ref_best <= 0:
        return 0.0, 0.0

    try:
        vwap_price = _vwap_for_volume(source_levels, volume_quote)
    except Exception:
        # Jak VWAP się nie uda (za mało poziomów itd.) – traktuj jako brak szansy na trade
        return 0.0, 0.0

    if not vwap_price or vwap_price <= 0:
        return 0.0, 0.0

    gain_pct = _gain_from_prices(vwap_price, ref_best, side, fees_pct)
    gain_usd = gain_pct * volume_quote
    return gain_pct, gain_usd


def compute_total_positive_gain(
    source_levels: List[Dict[str, float]],
    ref_best: float,
    side: str,
    fees_pct: float = 0.0,
) -> Tuple[float, float, float]:
    """
    Suma pozytywnych gainów z całego OB względem ref, ważona wolumenem w QUOTE.

    - source_levels: [{"p": price, "s": size}, ...]
    - ref_best: best price na giełdzie referencyjnej (bid/ask dopasowany do side)
    - side: "BID" / "ASK"
    - fees_pct: łączny koszt jako ułamek

    Logika:
      dla każdego poziomu i:
        volume_quote_i = p_i * s_i
        gain_pct_i     = gain_from_prices(p_i, ref_best, side, fees_pct)

        jeśli gain_pct_i > 0:
          total_gain_usd     += volume_quote_i * gain_pct_i
          total_volume_quote += volume_quote_i

      avg_gain_pct_total = total_gain_usd / total_volume_quote (jeśli vol > 0, inaczej 0)

    Zwraca tuple:
      (total_gain_usd, total_volume_quote, avg_gain_pct_total)
    """
    side = _normalize_side(side)

    if ref_best <= 0:
        return 0.0, 0.0, 0.0

    total_gain_usd = 0.0
    total_volume_quote = 0.0

    for lvl in source_levels:
        try:
            p = float(lvl.get("p", 0.0))
            s = float(lvl.get("s", 0.0))
        except (TypeError, ValueError):
            continue

        if p <= 0.0 or s <= 0.0:
            continue

        volume_quote_i = p * s
        if volume_quote_i <= 0.0:
            continue

        gain_pct_i = _gain_from_prices(p, ref_best, side, fees_pct)

        if gain_pct_i <= 0.0:
            # tylko pozytywne linie nas interesują do "total_positive_gains"
            continue

        total_gain_usd += volume_quote_i * gain_pct_i
        total_volume_quote += volume_quote_i

    if total_volume_quote > 0.0:
        avg_gain_pct_total = total_gain_usd / total_volume_quote
    else:
        avg_gain_pct_total = 0.0

    return total_gain_usd, total_volume_quote, avg_gain_pct_total

def compute_snapshot_metrics(
    source_bids: List[Dict[str, float]],
    source_asks: List[Dict[str, float]],
    ref_bids: List[Dict[str, float]],
    ref_asks: List[Dict[str, float]],
    fees_pct: float = 0.0,
) -> Dict[str, Any]:
    """
    Convenience helper for live detectors.

    Liczy:
      - first_level_gain_pct (bid/ask),
      - total_positive_gain_usd (bid/ask),
      - total_positive_volume_quote (bid/ask),
      - avg_gain_pct_total (bid/ask)

    dla pojedynczego snapshotu pary (source_exchange, ref_exchange).

    Parametry:
      source_bids / source_asks: listy poziomów OB po stronie giełdy source:
          [{"p": price, "s": size}, ...] – MUSZĄ być już posortowane
          (bids malejąco po cenie, asks rosnąco).
      ref_bids / ref_asks: analogiczne poziomy z giełdy referencyjnej.
      fees_pct: łączny koszt transakcji jako ułamek.

    Zwraca dict:
    {
      "first_level_gain_pct_bid": float | None,
      "first_level_gain_pct_ask": float | None,
      "total_positive_gain_usd_bid": float,
      "total_positive_gain_usd_ask": float,
      "total_volume_quote_bid": float,
      "total_volume_quote_ask": float,
      "avg_gain_pct_total_bid": float,
      "avg_gain_pct_total_ask": float,
    }
    """
    # Besty
    best_bid = float(source_bids[0]["p"]) if source_bids else None
    best_ask = float(source_asks[0]["p"]) if source_asks else None
    ref_bid = float(ref_bids[0]["p"]) if ref_bids else None
    ref_ask = float(ref_asks[0]["p"]) if ref_asks else None

    first_bid_gain = (
        compute_first_level_gain(
            source_best=best_bid,
            ref_best=ref_ask,
            side="BID",
            fees_pct=fees_pct,
        )
        if (best_bid is not None and ref_ask is not None)
        else None
    )
    first_ask_gain = (
        compute_first_level_gain(
            source_best=best_ask,
            ref_best=ref_bid,
            side="ASK",
            fees_pct=fees_pct,
        )
        if (best_ask is not None and ref_bid is not None)
        else None
    )

    bid_total_gain_usd, bid_total_vol_quote, bid_avg_gain_pct = compute_total_positive_gain(
        source_levels=source_bids,
        ref_best=ref_ask if ref_ask is not None else 0.0,
        side="BID",
        fees_pct=fees_pct,
    )
    ask_total_gain_usd, ask_total_vol_quote, ask_avg_gain_pct = compute_total_positive_gain(
        source_levels=source_asks,
        ref_best=ref_bid if ref_bid is not None else 0.0,
        side="ASK",
        fees_pct=fees_pct,
    )

    return {
        "first_level_gain_pct_bid": first_bid_gain,
        "first_level_gain_pct_ask": first_ask_gain,
        "total_positive_gain_usd_bid": bid_total_gain_usd,
        "total_positive_gain_usd_ask": ask_total_gain_usd,
        "total_volume_quote_bid": bid_total_vol_quote,
        "total_volume_quote_ask": ask_total_vol_quote,
        "avg_gain_pct_total_bid": bid_avg_gain_pct,
        "avg_gain_pct_total_ask": ask_avg_gain_pct,
    }
