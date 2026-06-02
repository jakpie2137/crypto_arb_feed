-- =============================================================================
-- Index dla populate ob_gain_per_logic – query pattern: (exchange, symbol, snapshot_time)
-- =============================================================================
-- Istniejący idx_orderbook_snapshots_symbol_exchange (symbol, exchange, time)
-- mniej optymalny przy filtrze exchange first.
-- Ten index przyspiesza: WHERE exchange = X AND symbol IN (...) AND snapshot_time >= ...
--
-- Na dużych tabelach (10+ GB): CREATE INDEX CONCURRENTLY (bez IF NOT EXISTS) – nie blokuje writes.
-- Jednorazowo: CREATE INDEX IF NOT EXISTS – szybsze budowanie.
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_exchange_symbol_time
    ON market_data.orderbook_snapshots (exchange, symbol, snapshot_time DESC);
