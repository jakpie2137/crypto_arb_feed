-- =============================================================================
-- MARKET DATA – efficient analytics queries (20GB+ friendly)
-- =============================================================================
-- Requires: market_data.ob_gain_per_logic (populated by batch job).
-- Run 020_ob_gain_per_logic.sql first, then populate via MDA batch/ETL.
--
-- See also: ob_gain_both_sides_filter.sql – logiki z gainami > threshold na BUY i SELL
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. 99 / 90 / 95 pct gain per logic (src_exchange, ref_exchange) + symbol + base_curr
--    Index: idx_ob_gain_per_logic_logic_symbol
-- -----------------------------------------------------------------------------

WITH base_curr AS (
    SELECT symbol,
           UPPER(REGEXP_REPLACE(symbol, 'USDT$|USD$|EUR$', '', 'i')) AS base_curr
    FROM (SELECT DISTINCT symbol FROM market_data.ob_gain_per_logic) s
)
SELECT
    g.src_exchange,
    g.ref_exchange,
    g.symbol,
    COALESCE(b.base_curr, g.symbol) AS base_curr,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY g.gain_ask_pct) AS gain_ask_p99,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY g.gain_ask_pct) AS gain_ask_p95,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY g.gain_ask_pct) AS gain_ask_p90,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY g.gain_bid_pct) AS gain_bid_p99,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY g.gain_bid_pct) AS gain_bid_p95,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY g.gain_bid_pct) AS gain_bid_p90,
    COUNT(*) AS observations
FROM market_data.ob_gain_per_logic g
LEFT JOIN base_curr b ON b.symbol = g.symbol
WHERE g.snapshot_time >= NOW() - INTERVAL '24 hours'
  AND g.gain_ask_pct IS NOT NULL
  AND g.gain_bid_pct IS NOT NULL
GROUP BY g.src_exchange, g.ref_exchange, g.symbol, b.base_curr
HAVING COUNT(*) >= 10
ORDER BY gain_ask_p99 DESC NULLS LAST, gain_bid_p99 DESC NULLS LAST;


-- -----------------------------------------------------------------------------
-- 2. Best ASK occasions (BUY source / SELL ref) – last 8h
--    Top N by gain % and by nominal (gain_pct * volume_quote)
-- -----------------------------------------------------------------------------

-- Top 20 by gain %
SELECT
    snapshot_time,
    src_exchange,
    ref_exchange,
    symbol,
    gain_ask_pct AS gain_pct,
    gain_ask_pct * volume_quote AS gain_nominal_usd,
    volume_quote
FROM market_data.ob_gain_per_logic
WHERE snapshot_time >= NOW() - INTERVAL '8 hours'
  AND gain_ask_pct IS NOT NULL
  AND gain_ask_pct > 0
ORDER BY gain_ask_pct DESC
LIMIT 20;

-- Top 20 by nominal ($)
SELECT
    snapshot_time,
    src_exchange,
    ref_exchange,
    symbol,
    gain_ask_pct AS gain_pct,
    gain_ask_pct * volume_quote AS gain_nominal_usd,
    volume_quote
FROM market_data.ob_gain_per_logic
WHERE snapshot_time >= NOW() - INTERVAL '8 hours'
  AND gain_ask_pct IS NOT NULL
  AND gain_ask_pct > 0
ORDER BY (gain_ask_pct * volume_quote) DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 3. Best BID occasions (SELL source / BUY ref) – last 8h
-- -----------------------------------------------------------------------------

SELECT
    snapshot_time,
    src_exchange,
    ref_exchange,
    symbol,
    gain_bid_pct AS gain_pct,
    gain_bid_pct * volume_quote AS gain_nominal_usd,
    volume_quote
FROM market_data.ob_gain_per_logic
WHERE snapshot_time >= NOW() - INTERVAL '8 hours'
  AND gain_bid_pct IS NOT NULL
  AND gain_bid_pct > 0
ORDER BY gain_bid_pct DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 4. Best ASK/BID – last 24h (parametrized window)
-- -----------------------------------------------------------------------------

SELECT
    snapshot_time,
    src_exchange,
    ref_exchange,
    symbol,
    'ASK' AS side,
    gain_ask_pct AS gain_pct,
    gain_ask_pct * volume_quote AS gain_nominal_usd
FROM market_data.ob_gain_per_logic
WHERE snapshot_time >= NOW() - INTERVAL '24 hours'
  AND gain_ask_pct IS NOT NULL
  AND gain_ask_pct > 0
UNION ALL
SELECT
    snapshot_time,
    src_exchange,
    ref_exchange,
    symbol,
    'BID' AS side,
    gain_bid_pct,
    gain_bid_pct * volume_quote
FROM market_data.ob_gain_per_logic
WHERE snapshot_time >= NOW() - INTERVAL '24 hours'
  AND gain_bid_pct IS NOT NULL
  AND gain_bid_pct > 0
ORDER BY gain_pct DESC
LIMIT 30;


-- -----------------------------------------------------------------------------
-- DEBUG: Jak wyglądają snapshoty dla symboli z dziwnymi gainami
-- Pokaże best bid/ask (p,s) z pierwszych poziomów – porównaj SRC vs REF.
-- Zamień src_exchange, ref_exchange, symbol w sample_logics.
-- -----------------------------------------------------------------------------

WITH sample_logics AS (
    SELECT 'BITGET'::text AS src_ex, 'BINANCEFUT'::text AS ref_ex, 'BLESSUSDT'::text AS sym
    UNION ALL SELECT 'KUCOIN', 'BINANCEFUT', 'CLANKERUSDT'
    UNION ALL SELECT 'KUCOIN', 'BINANCEFUT', 'FOLKSUSDT'
    UNION ALL SELECT 'KUCOIN', 'GATEFUT', 'HAEDALUSDT'
    UNION ALL SELECT 'BITGET', 'GATEFUT', 'VELVETUSDT'
),
-- snapshot_time z ob_gain_per_logic (jeśli ma dane) lub ostatni z orderbook_snapshots
sample_ts AS (
    SELECT sl.src_ex AS src_exchange, sl.ref_ex AS ref_exchange, sl.sym AS symbol,
           COALESCE(
               (SELECT g.snapshot_time FROM market_data.ob_gain_per_logic g
                WHERE g.src_exchange = sl.src_ex AND g.ref_exchange = sl.ref_ex AND g.symbol = sl.sym
                ORDER BY g.snapshot_time DESC LIMIT 1),
               (SELECT MAX(snapshot_time) FROM market_data.orderbook_snapshots
                WHERE exchange = sl.src_ex AND symbol = sl.sym)
           ) AS snapshot_time
    FROM sample_logics sl
)
SELECT
    st.src_exchange || ' vs ' || st.ref_exchange AS logic,
    st.symbol,
    s.exchange,
    s.snapshot_time,
    (COALESCE(s.bids->0->>'p', s.bids->0->>'price'))::numeric AS best_bid_p,
    (COALESCE(s.bids->0->>'s', s.bids->0->>'size'))::numeric AS best_bid_s,
    (COALESCE(s.asks->0->>'p', s.asks->0->>'price'))::numeric AS best_ask_p,
    (COALESCE(s.asks->0->>'s', s.asks->0->>'size'))::numeric AS best_ask_s,
    jsonb_build_array(
        s.bids->0, s.bids->1, s.bids->2
    ) AS bids_top3,
    jsonb_build_array(
        s.asks->0, s.asks->1, s.asks->2
    ) AS asks_top3
FROM sample_ts st
JOIN market_data.orderbook_snapshots s
  ON s.exchange = st.src_exchange AND s.symbol = st.symbol AND s.snapshot_time = st.snapshot_time
WHERE st.snapshot_time IS NOT NULL
UNION ALL
SELECT
    st.src_exchange || ' vs ' || st.ref_exchange,
    st.symbol,
    r.exchange,
    r.snapshot_time,
    (COALESCE(r.bids->0->>'p', r.bids->0->>'price'))::numeric,
    (COALESCE(r.bids->0->>'s', r.bids->0->>'size'))::numeric,
    (COALESCE(r.asks->0->>'p', r.asks->0->>'price'))::numeric,
    (COALESCE(r.asks->0->>'s', r.asks->0->>'size'))::numeric,
    jsonb_build_array(r.bids->0, r.bids->1, r.bids->2),
    jsonb_build_array(r.asks->0, r.asks->1, r.asks->2)
FROM sample_ts st
CROSS JOIN LATERAL (
    SELECT exchange, symbol, snapshot_time, bids, asks
    FROM market_data.orderbook_snapshots
    WHERE exchange = st.ref_exchange AND symbol = st.symbol
      AND snapshot_time <= st.snapshot_time
    ORDER BY snapshot_time DESC LIMIT 1
) r
WHERE st.snapshot_time IS NOT NULL
ORDER BY logic, symbol, exchange;

-- contract_size dla tych symboli (REF):
SELECT exchange, symbol, contract_size, fee_market_buy, fee_market_sell
FROM manager.symbol
WHERE is_active
  AND (UPPER(exchange) = 'BINANCEFUT' OR UPPER(exchange) = 'GATEFUT')
  AND UPPER(REPLACE(REPLACE(REPLACE(symbol,'_',''),'-',''),'/','')) IN (
    'BLESSUSDT','CLANKERUSDT','FOLKSUSDT','HAEDALUSDT','VELVETUSDT'
  )
ORDER BY exchange, symbol;


-- -----------------------------------------------------------------------------
-- DEBUG: Czy ekstremalne gain_ask (p99 > 50%) wynikają ze starego REF snapshotu?
-- Dla każdego rekordu g sprawdza ref_snapshot_time (ostatni ref <= src_time).
-- Jeśli delta (src - ref) > 60s → możliwy stale ref (ceny się rozjechały).
-- -----------------------------------------------------------------------------

SELECT
    g.src_exchange,
    g.ref_exchange,
    g.symbol,
    g.snapshot_time AS src_snapshot_time,
    ref_snap.snapshot_time AS ref_snapshot_time,
    EXTRACT(EPOCH FROM (g.snapshot_time - ref_snap.snapshot_time)) AS lag_sec,
    g.price_src,
    g.price_ref,
    g.gain_ask_pct,
    CASE WHEN g.price_src > 0 THEN ROUND((g.price_ref / g.price_src)::numeric, 4) ELSE NULL END AS price_ratio
FROM market_data.ob_gain_per_logic g
CROSS JOIN LATERAL (
    SELECT snapshot_time
    FROM market_data.orderbook_snapshots
    WHERE exchange = g.ref_exchange AND symbol = g.symbol
      AND snapshot_time <= g.snapshot_time
    ORDER BY snapshot_time DESC LIMIT 1
) ref_snap
WHERE g.snapshot_time >= NOW() - INTERVAL '24 hours'
  AND g.gain_ask_pct IS NOT NULL
  AND g.gain_ask_pct > 0.5
ORDER BY g.gain_ask_pct DESC
LIMIT 30;


-- -----------------------------------------------------------------------------
-- 5. Fallback: if ob_gain_per_logic is empty, use orderbook_gains (ETL, single ref)
--    Slower but works with existing ETL.
-- -----------------------------------------------------------------------------

-- 99/95/90 pct per instrument (orderbook_gains has exchange=symbol, instrument=exchange_symbol)
SELECT
    instrument,
    exchange AS src_exchange,
    ref_exchange,
    symbol,
    side,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY vwap_gain_pct) AS p99,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY vwap_gain_pct) AS p95,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY vwap_gain_pct) AS p90,
    COUNT(*) AS n
FROM market_data.orderbook_gains
WHERE snapshot_time >= NOW() - INTERVAL '24 hours'
  AND vwap_gain_pct IS NOT NULL
  AND volume_bucket_quote = 500
GROUP BY instrument, exchange, ref_exchange, symbol, side
HAVING COUNT(*) >= 10
ORDER BY p99 DESC NULLS LAST;
