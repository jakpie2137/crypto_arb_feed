-- Create mm.signal table for logging MM signals
CREATE SCHEMA IF NOT EXISTS mm;

CREATE TABLE IF NOT EXISTS mm.signal (
    id BIGSERIAL PRIMARY KEY,
    ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    logic_id VARCHAR(100) NOT NULL,
    cycle_id VARCHAR(50) NOT NULL,
    app_id VARCHAR(50),
    
    -- Source side
    source_exchange VARCHAR(50) NOT NULL,
    source_symbol VARCHAR(50) NOT NULL,
    source_side VARCHAR(10) NOT NULL,  -- BUY or SELL
    source_price DECIMAL(24,8),
    source_qty DECIMAL(24,8),
    
    -- Reference side
    ref_exchange VARCHAR(50) NOT NULL,
    ref_symbol VARCHAR(50) NOT NULL,
    ref_side VARCHAR(10) NOT NULL,  -- Opposite of source (hedge)
    ref_price DECIMAL(24,8),
    
    -- Signal metrics
    net_profit DECIMAL(24,8),  -- Net profit from MM calculation
    pnl_gross_quote DECIMAL(24,8),
    pnl_net_quote DECIMAL(24,8),
    
    -- Fee rates (nominal from MM)
    fee_rate_source DECIMAL(24,8),
    fee_rate_ref DECIMAL(24,8),
    
    -- Execution status
    status VARCHAR(20) DEFAULT 'SIGNAL',  -- SIGNAL, EXECUTED, FAILED
    source_order_id VARCHAR(100),
    ref_order_id VARCHAR(100),
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(logic_id, cycle_id)
);

CREATE INDEX IF NOT EXISTS idx_mm_signal_ts ON mm.signal(ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_mm_signal_logic ON mm.signal(logic_id);
CREATE INDEX IF NOT EXISTS idx_mm_signal_cycle ON mm.signal(cycle_id);
CREATE INDEX IF NOT EXISTS idx_mm_signal_status ON mm.signal(status);
