-- ==============================================================================
-- Reporter Schema Migration
-- ==============================================================================
-- Creates tables for storing exchange balances and positions
-- Run this on your PostgreSQL database before starting the reporter
-- ==============================================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS reporter;

-- ==============================================================================
-- TABLE: reporter.balance_spot
-- ==============================================================================
-- Stores SPOT wallet balances for all exchanges and subaccounts
-- ==============================================================================

CREATE TABLE IF NOT EXISTS reporter.balance_spot (
    id              BIGSERIAL PRIMARY KEY,
    ts_utc          BIGINT NOT NULL,           -- Bucket timestamp (ms)
    ts_utc_str      VARCHAR(32),               -- Human-readable timestamp
    exchange        VARCHAR(32) NOT NULL,      -- Exchange identifier (binance, mexc2, gate_hedge, etc.)
    asset           VARCHAR(32) NOT NULL,      -- Asset symbol (BTC, ETH, USDT)
    available       DOUBLE PRECISION,          -- Available balance
    locked          DOUBLE PRECISION,          -- Locked/frozen balance
    total           DOUBLE PRECISION,          -- Total balance
    rate_usd        DOUBLE PRECISION,          -- USD exchange rate at snapshot time
    available_usd   DOUBLE PRECISION,          -- Available in USD
    locked_usd      DOUBLE PRECISION,          -- Locked in USD
    total_usd       DOUBLE PRECISION,          -- Total in USD
    inserted_at     BIGINT NOT NULL,           -- Insert timestamp (ms)
    inserted_at_str VARCHAR(32)
);

-- Unique constraint for upsert
CREATE UNIQUE INDEX IF NOT EXISTS uq_balance_spot 
    ON reporter.balance_spot (exchange, asset, ts_utc);

-- Performance indexes
CREATE INDEX IF NOT EXISTS ix_balance_spot_ts 
    ON reporter.balance_spot (ts_utc DESC);
CREATE INDEX IF NOT EXISTS ix_balance_spot_exchange 
    ON reporter.balance_spot (exchange, ts_utc DESC);


-- ==============================================================================
-- TABLE: reporter.futures_account_info
-- ==============================================================================
-- Stores futures account summary for each exchange
-- ==============================================================================

CREATE TABLE IF NOT EXISTS reporter.futures_account_info (
    id                   BIGSERIAL PRIMARY KEY,
    ts_utc               BIGINT NOT NULL,
    ts_utc_str           VARCHAR(32),
    exchange             VARCHAR(32) NOT NULL,
    available_balance    DOUBLE PRECISION,      -- Available margin
    available_usdt       DOUBLE PRECISION,      -- Same as above (USDT)
    margin_used          DOUBLE PRECISION,      -- Used margin
    initial_margin       DOUBLE PRECISION,      -- Initial margin requirement
    maintenance_margin   DOUBLE PRECISION,      -- Maintenance margin requirement
    unrealised_profit    DOUBLE PRECISION,      -- Unrealized PnL
    wallet_balance       DOUBLE PRECISION,      -- Total wallet balance
    withdrawable_amount  DOUBLE PRECISION,      -- Available to withdraw
    position_sum         DOUBLE PRECISION,      -- Sum of position notionals
    inserted_at          BIGINT NOT NULL,
    inserted_at_str      VARCHAR(32)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_futures_account_info 
    ON reporter.futures_account_info (exchange, ts_utc);

CREATE INDEX IF NOT EXISTS ix_futures_account_info_ts 
    ON reporter.futures_account_info (ts_utc DESC);


-- ==============================================================================
-- TABLE: reporter.position_info
-- ==============================================================================
-- Stores individual futures positions
-- ==============================================================================

CREATE TABLE IF NOT EXISTS reporter.position_info (
    id                   BIGSERIAL PRIMARY KEY,
    ts_utc               BIGINT NOT NULL,
    ts_utc_str           VARCHAR(32),
    exchange             VARCHAR(32) NOT NULL,
    symbol               VARCHAR(64) NOT NULL,  -- Contract symbol
    side                 VARCHAR(8),            -- 'long' or 'short'
    margin_mode          VARCHAR(16),           -- 'cross' / 'isolated'
    leverage             DOUBLE PRECISION,
    contract_size        DOUBLE PRECISION,      -- Base currency per contract (from manager.symbol)
    entry_time           BIGINT,                -- Position open time (ms)
    entry_time_str       VARCHAR(32),
    entry_price          DOUBLE PRECISION,
    mark_price           DOUBLE PRECISION,
    amount               DOUBLE PRECISION,      -- Position size in contracts/coins
    position_size        DOUBLE PRECISION,      -- Notional value (USD)
    unrealised_profit    DOUBLE PRECISION,
    realised_profit      DOUBLE PRECISION,
    margin               DOUBLE PRECISION,      -- Margin reported from API (actual margin used)
    initial_margin       DOUBLE PRECISION,     -- Calculated or from API
    maintenance_margin   DOUBLE PRECISION,     -- Calculated or from API
    funding_rate         DOUBLE PRECISION,
    take_profit          DOUBLE PRECISION,
    stop_loss            DOUBLE PRECISION,
    liquidation_price    DOUBLE PRECISION,
    inserted_at          BIGINT NOT NULL,
    inserted_at_str      VARCHAR(32)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_position_info 
    ON reporter.position_info (exchange, symbol, side, ts_utc);

CREATE INDEX IF NOT EXISTS ix_position_info_ts 
    ON reporter.position_info (ts_utc DESC);
CREATE INDEX IF NOT EXISTS ix_position_info_exchange_symbol 
    ON reporter.position_info (exchange, symbol, ts_utc DESC);


-- ==============================================================================
-- FUTURE TABLES (placeholder comments)
-- ==============================================================================

-- historic.transaction
-- mm.transaction  
-- reporter.exposure
-- reporter.warning (wrong_fee, wrong_precision)

-- ==============================================================================
-- Comments
-- ==============================================================================

COMMENT ON SCHEMA reporter IS 'Exchange balance and position reporting data';
COMMENT ON TABLE reporter.balance_spot IS 'SPOT wallet balances from all exchanges';
COMMENT ON TABLE reporter.futures_account_info IS 'Futures account summary snapshots';
COMMENT ON TABLE reporter.position_info IS 'Individual futures position snapshots';

COMMENT ON COLUMN reporter.balance_spot.exchange IS 
    'Exchange identifier. Supports subaccounts: binance, mexc2, gate_hedge, etc.';
COMMENT ON COLUMN reporter.position_info.side IS 
    'Position side: long or short (lowercase)';
