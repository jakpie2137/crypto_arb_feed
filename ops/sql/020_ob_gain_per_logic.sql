-- =============================================================================
-- market_data.ob_gain_per_logic – pre-aggregated gains for efficient analytics
-- =============================================================================
-- Populated by batch job (e.g. analytics/market_data_analysis ETL extension).
-- Enables fast percentile and best-occasions queries on 20GB+ orderbook_snapshots.
-- =============================================================================

CREATE TABLE IF NOT EXISTS market_data.ob_gain_per_logic (
    snapshot_time     TIMESTAMPTZ NOT NULL,
    src_exchange      VARCHAR(64) NOT NULL,
    ref_exchange      VARCHAR(64) NOT NULL,
    symbol            VARCHAR(64) NOT NULL,
    gain_bid_pct      DOUBLE PRECISION,   -- SELL source / BUY ref
    gain_ask_pct      DOUBLE PRECISION,  -- BUY source / SELL ref
    price_src         DOUBLE PRECISION,   -- for dashboard fast path
    price_ref         DOUBLE PRECISION,
    volume_quote      DOUBLE PRECISION NOT NULL DEFAULT 500.0,
    price_mode_src    VARCHAR(16) DEFAULT 'vwap',
    price_mode_ref    VARCHAR(16) DEFAULT 'best',
    UNIQUE (snapshot_time, src_exchange, ref_exchange, symbol)
);

-- Primary index: time-range + logic grouping
CREATE INDEX IF NOT EXISTS idx_ob_gain_per_logic_time
    ON market_data.ob_gain_per_logic (snapshot_time DESC);

-- For percentile/best per (src, ref, symbol)
CREATE INDEX IF NOT EXISTS idx_ob_gain_per_logic_logic_symbol
    ON market_data.ob_gain_per_logic (src_exchange, ref_exchange, symbol, snapshot_time DESC);

-- For filtering by symbol
CREATE INDEX IF NOT EXISTS idx_ob_gain_per_logic_symbol_time
    ON market_data.ob_gain_per_logic (symbol, snapshot_time DESC);

-- Optional: partition by time (monthly) for very large datasets
-- CREATE TABLE market_data.ob_gain_per_logic_2026_02 PARTITION OF market_data.ob_gain_per_logic
--     FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
