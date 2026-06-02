-- 022_historic_funding_fee.sql
-- ==============================================================================
-- Historic schema + funding_fee table
-- Stores actual funding fee payments fetched from exchange APIs
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS historic;

CREATE TABLE IF NOT EXISTS historic.funding_fee (
    id               BIGSERIAL PRIMARY KEY,
    exchange         VARCHAR(32) NOT NULL,
    symbol           VARCHAR(64) NOT NULL,
    funding_time     BIGINT NOT NULL,
    funding_time_str VARCHAR(32),
    funding_rate     DOUBLE PRECISION,
    position_size    DOUBLE PRECISION,
    side             VARCHAR(8),
    fee_amount       DOUBLE PRECISION NOT NULL,
    fee_currency     VARCHAR(16) DEFAULT 'USDT',
    inserted_at      BIGINT NOT NULL,
    inserted_at_str  VARCHAR(32)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_funding_fee
    ON historic.funding_fee (exchange, symbol, funding_time);

CREATE INDEX IF NOT EXISTS ix_funding_fee_ts
    ON historic.funding_fee (funding_time DESC);

CREATE INDEX IF NOT EXISTS ix_funding_fee_exchange
    ON historic.funding_fee (exchange);

CREATE INDEX IF NOT EXISTS ix_funding_fee_symbol
    ON historic.funding_fee (exchange, symbol);
