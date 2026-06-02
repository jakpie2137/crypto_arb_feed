-- Global ranking snapshots (refreshed every ~15 min by pipeline)
CREATE SCHEMA IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.global_gain_rankings (
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rank_type       VARCHAR(64) NOT NULL,
    payload         JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_global_gain_rankings_type_ts
    ON market_data.global_gain_rankings (rank_type, ts DESC);

-- Retention helpers
CREATE OR REPLACE FUNCTION market_data.prune_orderbook_snapshots(retention_minutes INT DEFAULT 15)
RETURNS BIGINT AS $$
DECLARE deleted BIGINT;
BEGIN
    DELETE FROM market_data.orderbook_snapshots
    WHERE snapshot_time < NOW() - make_interval(mins => retention_minutes);
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION market_data.prune_rolled_gains(retention_hours INT DEFAULT 24)
RETURNS BIGINT AS $$
DECLARE deleted BIGINT;
BEGIN
    DELETE FROM market_data.ob_arb_rolling_gain_30s
    WHERE ts < NOW() - make_interval(hours => retention_hours);
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION market_data.prune_global_rankings(retention_hours INT DEFAULT 48)
RETURNS BIGINT AS $$
DECLARE deleted BIGINT;
BEGIN
    DELETE FROM market_data.global_gain_rankings
    WHERE ts < NOW() - make_interval(hours => retention_hours);
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END;
$$ LANGUAGE plpgsql;
