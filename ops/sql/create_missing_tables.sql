-- ==============================================================================
-- Create missing tables: router.transaction and mm.signal
-- ==============================================================================
-- Run this if tables don't exist after router/MM startup
-- ==============================================================================

-- ==============================================================================
-- router.transaction
-- ==============================================================================
CREATE SCHEMA IF NOT EXISTS router;

CREATE TABLE IF NOT EXISTS router.transaction (
    id BIGSERIAL PRIMARY KEY,
    ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    side VARCHAR(10) NOT NULL,
    order_type VARCHAR(20),
    qty_requested DECIMAL(24,8),
    price_requested DECIMAL(24,8),
    qty_filled DECIMAL(24,8),
    price_filled DECIMAL(24,8),
    fee DECIMAL(24,8),
    fee_asset VARCHAR(20),
    order_id VARCHAR(100),
    client_order_id VARCHAR(100),
    trade_id VARCHAR(100),
    status VARCHAR(20) DEFAULT 'FILLED',
    source_app VARCHAR(50),
    logic_id VARCHAR(100),
    cycle_id VARCHAR(100),
    pnl_gross_quote DECIMAL(24,8),
    pnl_net_quote DECIMAL(24,8),
    app_id VARCHAR(50) DEFAULT 'ROUTER',
    leg VARCHAR(20) DEFAULT 'HEDGE',
    ts_signal_received TIMESTAMPTZ,
    ts_order_sent TIMESTAMPTZ,
    ts_exchange_ack TIMESTAMPTZ,
    ts_filled TIMESTAMPTZ,
    matched_reporter_tx_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(exchange, order_id)
);

CREATE INDEX IF NOT EXISTS idx_router_tx_ts ON router.transaction(ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_router_tx_exchange ON router.transaction(exchange);
CREATE INDEX IF NOT EXISTS idx_router_tx_symbol ON router.transaction(symbol);
CREATE INDEX IF NOT EXISTS idx_router_tx_cycle ON router.transaction(cycle_id);
CREATE INDEX IF NOT EXISTS idx_router_tx_logic ON router.transaction(logic_id);

-- ==============================================================================
-- mm.signal
-- ==============================================================================
CREATE SCHEMA IF NOT EXISTS mm;

CREATE TABLE IF NOT EXISTS mm.signal (
    id BIGSERIAL PRIMARY KEY,
    ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    logic_id VARCHAR(100) NOT NULL,
    cycle_id VARCHAR(100) NOT NULL,
    app_id VARCHAR(50) DEFAULT 'MM',
    source_exchange VARCHAR(50),
    source_symbol VARCHAR(50),
    source_side VARCHAR(10),
    source_price DECIMAL(24,8),
    source_qty DECIMAL(24,8),
    source_order_id VARCHAR(100),
    ref_exchange VARCHAR(50),
    ref_symbol VARCHAR(50),
    ref_side VARCHAR(10),
    ref_price DECIMAL(24,8),
    ref_order_id VARCHAR(100),
    net_profit DECIMAL(24,8),
    pnl_gross_quote DECIMAL(24,8),
    pnl_net_quote DECIMAL(24,8),
    fee_rate_source DECIMAL(12,8),
    fee_rate_ref DECIMAL(12,8),
    status VARCHAR(20) DEFAULT 'SIGNAL',
    ts_signal_created TIMESTAMPTZ,
    ts_source_sent TIMESTAMPTZ,
    ts_source_ack TIMESTAMPTZ,
    ts_hedge_sent TIMESTAMPTZ,
    ts_hedge_ack TIMESTAMPTZ,
    quote_level_expo_checked BOOLEAN DEFAULT FALSE,
    quote_level_expo_allowed BOOLEAN,
    quote_level_expo_required_gain DECIMAL(24,8),
    quote_level_expo_actual_gain DECIMAL(24,8),
    quote_level_expo_balance DECIMAL(24,8),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(logic_id, cycle_id)
);

CREATE INDEX IF NOT EXISTS idx_mm_signal_ts ON mm.signal(ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_mm_signal_logic ON mm.signal(logic_id);
CREATE INDEX IF NOT EXISTS idx_mm_signal_cycle ON mm.signal(cycle_id);
CREATE INDEX IF NOT EXISTS idx_mm_signal_status ON mm.signal(status);

-- ==============================================================================
-- Add missing columns if tables already exist (migration)
-- ==============================================================================

-- router.transaction
ALTER TABLE router.transaction ADD COLUMN IF NOT EXISTS client_order_id VARCHAR(100);
ALTER TABLE router.transaction ADD COLUMN IF NOT EXISTS matched_reporter_tx_id BIGINT;

-- mm.signal (all columns should already be in CREATE TABLE, but add if missing)
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS source_order_id VARCHAR(100);
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS ref_order_id VARCHAR(100);
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS ts_signal_created TIMESTAMPTZ;
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS ts_source_sent TIMESTAMPTZ;
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS ts_source_ack TIMESTAMPTZ;
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS ts_hedge_sent TIMESTAMPTZ;
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS ts_hedge_ack TIMESTAMPTZ;
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS quote_level_expo_checked BOOLEAN DEFAULT FALSE;
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS quote_level_expo_allowed BOOLEAN;
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS quote_level_expo_required_gain DECIMAL(24,8);
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS quote_level_expo_actual_gain DECIMAL(24,8);
ALTER TABLE mm.signal ADD COLUMN IF NOT EXISTS quote_level_expo_balance DECIMAL(24,8);
