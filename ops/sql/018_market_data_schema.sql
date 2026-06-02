-- market_data schema and tables for feed OB_GUI local analytics:
--   - orderbook_snapshots (Start OB snapshots to DB)
--   - ob_arb_rolling_gain_30s (Start Big Gain Scanner / RollingGainDetector)
--   - ob_arb_stats_historic (Big Gain Detector rankings)
--
-- Run once on local/dev Postgres before using feed with OB_GUI=true.

CREATE SCHEMA IF NOT EXISTS market_data;

-- Orderbook snapshots – surowe dane z feedu (OrderBookSnapshotService)
-- ETL (ob_analytics) czyta stąd id, snapshot_time, exchange, symbol, bids, asks
CREATE TABLE IF NOT EXISTS market_data.orderbook_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ NOT NULL,
    exchange        VARCHAR(64) NOT NULL,
    symbol          VARCHAR(64) NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    bids            JSONB NOT NULL DEFAULT '[]',
    asks            JSONB NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_symbol_exchange
    ON market_data.orderbook_snapshots (symbol, exchange, snapshot_time DESC);

CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_snapshot_time
    ON market_data.orderbook_snapshots (snapshot_time);

-- Rolling 30s arbitrage gain – Big Gain Scanner (MarketDataArbRollingGainWriter)
CREATE TABLE IF NOT EXISTS market_data.ob_arb_rolling_gain_30s (
    ts                    TIMESTAMPTZ NOT NULL,
    symbol                VARCHAR(64) NOT NULL,
    src_exchange          VARCHAR(64) NOT NULL,
    ref_exchange          VARCHAR(64) NOT NULL,
    side                  VARCHAR(8) NOT NULL,   -- 'BID' / 'ASK'
    target_volume_quote    DOUBLE PRECISION NOT NULL,
    avg_gain_pct_30s      DOUBLE PRECISION,     -- może być NULL
    observations_count    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ob_arb_rolling_gain_ts
    ON market_data.ob_arb_rolling_gain_30s (ts DESC);

CREATE INDEX IF NOT EXISTS idx_ob_arb_rolling_gain_symbol
    ON market_data.ob_arb_rolling_gain_30s (symbol, ts DESC);

-- Big Gain Detector rankings (MarketDataArbStatsWriter)
CREATE TABLE IF NOT EXISTS market_data.ob_arb_stats_historic (
    ts                            TIMESTAMPTZ NOT NULL,
    symbol                        VARCHAR(64) NOT NULL,
    src_exchange                  VARCHAR(64) NOT NULL,
    ref_exchange                  VARCHAR(64) NOT NULL,
    side                          VARCHAR(8) NOT NULL,
    rank_type                     SMALLINT NOT NULL,
    rank_position                 INTEGER NOT NULL,
    first_level_gain_pct          DOUBLE PRECISION,
    first_level_gain_usd          DOUBLE PRECISION,
    first_level_volume_quote      DOUBLE PRECISION,
    total_positive_gain_usd       DOUBLE PRECISION,
    total_volume_quote            DOUBLE PRECISION,
    avg_gain_pct_total            DOUBLE PRECISION,
    target_volume_quote            DOUBLE PRECISION,
    vwap_gain_pct_for_target_volume DOUBLE PRECISION,
    target_gain_usd               DOUBLE PRECISION,
    volume_quote_to_reach_target_gain DOUBLE PRECISION,
    gain_pct_at_target_gain       DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_ob_arb_stats_historic_ts
    ON market_data.ob_arb_stats_historic (ts DESC);
