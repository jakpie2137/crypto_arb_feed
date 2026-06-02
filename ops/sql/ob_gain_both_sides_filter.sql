-- =============================================================================
-- Logiki z gainami > threshold na OBUDW stronach (BUY + SELL)
-- =============================================================================
-- Źródło: market_data.ob_gain_per_logic
-- Filtruje logiki, gdzie zarówno gain_ask_pct (BUY) jak i gain_bid_pct (SELL)
-- przekraczały dany próg w co najmniej jednym snapshotcie.
--
-- Parametry (zamień wartości przed uruchomieniem):
--   :threshold     np. 0, 0.001 (10bps), 0.005 (50bps)
--   :time_from     np. '2026-02-24 00:00:00+00'
--   :time_to       np. '2026-02-24 18:00:00+00'
--   LUB zamiast od-do użyj:  snapshot_time >= NOW() - INTERVAL ':hours hours'
--
--   :sort_by       'MIN' lub 'MAX' – sortowanie po mniejszej/większej z dwóch liczników
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Od-do + sortowanie po MIN (logiki zbalansowane – obie strony podobnie często)
-- -----------------------------------------------------------------------------
-- Zamień: 0.001 → threshold (np. 0, 0.001, 0.005)
--        '2026-02-24 00:00:00+00' → time_from
--        '2026-02-24 23:59:59+00' → time_to

/*
WITH counts AS (
    SELECT
        src_exchange,
        ref_exchange,
        symbol,
        COUNT(*) FILTER (WHERE gain_ask_pct > 0.001) AS cnt_buy_above,
        COUNT(*) FILTER (WHERE gain_bid_pct > 0.001) AS cnt_sell_above
    FROM market_data.ob_gain_per_logic
    WHERE snapshot_time >= '2026-02-24 00:00:00+00'
      AND snapshot_time <= '2026-02-24 23:59:59+00'
      AND gain_ask_pct IS NOT NULL
      AND gain_bid_pct IS NOT NULL
    GROUP BY src_exchange, ref_exchange, symbol
)
SELECT
    src_exchange,
    ref_exchange,
    symbol,
    cnt_buy_above,
    cnt_sell_above,
    LEAST(cnt_buy_above, cnt_sell_above) AS min_count
FROM counts
WHERE cnt_buy_above >= 1 AND cnt_sell_above >= 1
ORDER BY LEAST(cnt_buy_above, cnt_sell_above) DESC, cnt_buy_above + cnt_sell_above DESC;
*/


-- -----------------------------------------------------------------------------
-- 2. Last X hours + sortowanie po MIN (gotowe do uruchomienia)
-- -----------------------------------------------------------------------------
-- Zamień: 0.001 → threshold (0 = powyżej zera, 0.001 = 10bps, 0.005 = 50bps)
--        '24 hours' → np. '8 hours', '12 hours', '48 hours'

WITH counts AS (
    SELECT
        src_exchange,
        ref_exchange,
        symbol,
        COUNT(*) FILTER (WHERE gain_ask_pct > 0.0005) AS cnt_buy_above,
        COUNT(*) FILTER (WHERE gain_bid_pct > 0.0005) AS cnt_sell_above
    FROM market_data.ob_gain_per_logic
    WHERE snapshot_time >= NOW() - INTERVAL '8 hours'
      AND gain_ask_pct IS NOT NULL
      AND gain_bid_pct IS NOT NULL
    GROUP BY src_exchange, ref_exchange, symbol
)
SELECT
    src_exchange,
    ref_exchange,
    symbol,
    cnt_buy_above,
    cnt_sell_above,
    LEAST(cnt_buy_above, cnt_sell_above) AS min_count
FROM counts
WHERE cnt_buy_above >= 1 AND cnt_sell_above >= 1
ORDER BY LEAST(cnt_buy_above, cnt_sell_above) DESC, cnt_buy_above + cnt_sell_above DESC;


-- -----------------------------------------------------------------------------
-- 3. Last X hours + sortowanie po MAX (logiki z dominującą stroną)
-- -----------------------------------------------------------------------------
-- Sortowanie po większej z dwóch liczników – szukamy logik z dużą liczbą okazji
-- na jednej ze stron (ale nadal wymagamy >=1 na obu)

/*
WITH counts AS (
    SELECT
        src_exchange,
        ref_exchange,
        symbol,
        COUNT(*) FILTER (WHERE gain_ask_pct > 0.001) AS cnt_buy_above,
        COUNT(*) FILTER (WHERE gain_bid_pct > 0.001) AS cnt_sell_above
    FROM market_data.ob_gain_per_logic
    WHERE snapshot_time >= NOW() - INTERVAL '24 hours'
      AND gain_ask_pct IS NOT NULL
      AND gain_bid_pct IS NOT NULL
    GROUP BY src_exchange, ref_exchange, symbol
)
SELECT
    src_exchange,
    ref_exchange,
    symbol,
    cnt_buy_above,
    cnt_sell_above,
    GREATEST(cnt_buy_above, cnt_sell_above) AS max_count
FROM counts
WHERE cnt_buy_above >= 1 AND cnt_sell_above >= 1
ORDER BY GREATEST(cnt_buy_above, cnt_sell_above) DESC, cnt_buy_above + cnt_sell_above DESC;
*/


-- -----------------------------------------------------------------------------
-- 4. Szablon do użycia w psql / prompt (skopiuj i wklej wartości)
-- -----------------------------------------------------------------------------
/*
-- Threshold: 0 (=powyżej zera) | 0.001 (10bps) | 0.005 (50bps)
-- Last X hours: 8 | 12 | 24 | 48
-- Sort: MIN (zbalansowane) | MAX (dominująca strona)

WITH counts AS (
    SELECT
        src_exchange,
        ref_exchange,
        symbol,
        COUNT(*) FILTER (WHERE gain_ask_pct > 0.001) AS cnt_buy_above,
        COUNT(*) FILTER (WHERE gain_bid_pct > 0.001) AS cnt_sell_above
    FROM market_data.ob_gain_per_logic
    WHERE snapshot_time >= NOW() - INTERVAL '24 hours'
      AND gain_ask_pct IS NOT NULL
      AND gain_bid_pct IS NOT NULL
    GROUP BY src_exchange, ref_exchange, symbol
)
SELECT
    src_exchange,
    ref_exchange,
    symbol,
    cnt_buy_above,
    cnt_sell_above,
    LEAST(cnt_buy_above, cnt_sell_above) AS sort_min,
    GREATEST(cnt_buy_above, cnt_sell_above) AS sort_max
FROM counts
WHERE cnt_buy_above >= 1 AND cnt_sell_above >= 1
ORDER BY LEAST(cnt_buy_above, cnt_sell_above) DESC;
*/


-- -----------------------------------------------------------------------------
-- 5. Wariant z zakresem od-do (timestamp)
-- -----------------------------------------------------------------------------
/*
WITH counts AS (
    SELECT
        src_exchange,
        ref_exchange,
        symbol,
        COUNT(*) FILTER (WHERE gain_ask_pct > 0) AS cnt_buy_above,
        COUNT(*) FILTER (WHERE gain_bid_pct > 0) AS cnt_sell_above
    FROM market_data.ob_gain_per_logic
    WHERE snapshot_time >= '2026-02-24 12:00:00+00'
      AND snapshot_time <= '2026-02-24 18:30:00+00'
      AND gain_ask_pct IS NOT NULL
      AND gain_bid_pct IS NOT NULL
    GROUP BY src_exchange, ref_exchange, symbol
)
SELECT
    src_exchange,
    ref_exchange,
    symbol,
    cnt_buy_above,
    cnt_sell_above,
    LEAST(cnt_buy_above, cnt_sell_above) AS min_count
FROM counts
WHERE cnt_buy_above >= 1 AND cnt_sell_above >= 1
ORDER BY LEAST(cnt_buy_above, cnt_sell_above) DESC;
*/
