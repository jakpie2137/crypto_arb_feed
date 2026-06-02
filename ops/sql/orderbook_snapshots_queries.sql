-- =============================================================================
-- Optymalne zapytania do market_data.orderbook_snapshots
-- Tabela może być duża (GB+), zapytania wykorzystują indeksy.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Historia orderbooków dla danego symbolu i exchange
--    Wykorzystuje idx_orderbook_snapshots_symbol_exchange (symbol, exchange, snapshot_time DESC)
-- -----------------------------------------------------------------------------

-- Ostatnie N snapshotów
SELECT id, snapshot_time, event_time, bids, asks
FROM market_data.orderbook_snapshots
WHERE symbol = '0GUSDT' AND exchange = 'KUCOIN'
ORDER BY snapshot_time DESC
LIMIT 100;

-- Snapshoty z danego przedziału czasowego
SELECT id, snapshot_time, event_time, bids, asks
FROM market_data.orderbook_snapshots
WHERE symbol = '0GUSDT' AND exchange = 'KUCOIN'
  AND snapshot_time BETWEEN '2026-02-21 03:00:00+01' AND '2026-02-21 04:00:00+01'
ORDER BY snapshot_time DESC;

-- Bez JSON (lekkie – tylko metadata, np. do sprawdzenia gęstości)
SELECT id, snapshot_time, event_time
FROM market_data.orderbook_snapshots
WHERE symbol = '0GUSDT' AND exchange = 'KUCOIN'
ORDER BY snapshot_time DESC
LIMIT 1000;


-- -----------------------------------------------------------------------------
-- 2. Zakres timestampów (najstarsze i najnowsze)
--    Wykorzystuje idx_orderbook_snapshots_snapshot_time lub index-only scan
-- -----------------------------------------------------------------------------

SELECT
    MIN(snapshot_time) AS oldest,
    MAX(snapshot_time) AS newest,
    MAX(snapshot_time) - MIN(snapshot_time) AS span
FROM market_data.orderbook_snapshots;

-- Dla danego symbolu+exchange (index-friendly)
SELECT
    MIN(snapshot_time) AS oldest,
    MAX(snapshot_time) AS newest
FROM market_data.orderbook_snapshots
WHERE symbol = '0GUSDT' AND exchange = 'KUCOIN';


-- -----------------------------------------------------------------------------
-- 3. Liczba różnych par (exchange, symbol)
-- -----------------------------------------------------------------------------

SELECT COUNT(DISTINCT (exchange, symbol)) AS unique_orderbooks
FROM market_data.orderbook_snapshots;

-- Z podziałem na exchange
SELECT exchange, COUNT(DISTINCT symbol) AS symbols_count
FROM market_data.orderbook_snapshots
GROUP BY exchange
ORDER BY symbols_count DESC;

-- Top symboli po liczbie snapshotów
SELECT symbol, exchange, COUNT(*) AS snapshot_count
FROM market_data.orderbook_snapshots
GROUP BY symbol, exchange
ORDER BY snapshot_count DESC
LIMIT 20;
