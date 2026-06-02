-- =============================================================================
-- Prune orderbook_snapshots i ob_gain_per_logic – zwolnienie miejsca na dysku
-- Awaryjny run – usuwa najstarsze. Retencja normalna: snapshoty 8h, ob_gain 48h.
-- Uruchom: psql -h ... -U postgres -d arb_test -f ops/sql/025_prune_orderbook_and_gain_tables.sql
-- =============================================================================

-- 1. orderbook_snapshots – usuń starsze niż 6h (normalna retencja 8h; agresywniejszy prune)
DELETE FROM market_data.orderbook_snapshots
WHERE snapshot_time < NOW() - INTERVAL '6 hours';

-- 2. ob_gain_per_logic – usuń starsze niż 24h (normalna retencja 48h; ~połowa)
DELETE FROM market_data.ob_gain_per_logic
WHERE snapshot_time < NOW() - INTERVAL '24 hours';

-- 3. VACUUM – odzysk miejsca (VACUUM nie zwraca od razu do OS, ale przygotowuje do reuse)
VACUUM ANALYZE market_data.orderbook_snapshots;
VACUUM ANALYZE market_data.ob_gain_per_logic;

-- Jeśli nadal brak miejsca – VACUUM FULL zwraca przestrzeń do OS (blokuje tabelę!):
-- VACUUM FULL market_data.orderbook_snapshots;
-- VACUUM FULL market_data.ob_gain_per_logic;
