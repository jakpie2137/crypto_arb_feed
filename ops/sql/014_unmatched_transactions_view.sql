-- View to identify unmatched transactions (source without hedge or vice versa)
-- This helps identify exposure issues when hedge orders fail or are rejected

CREATE OR REPLACE VIEW router.unmatched_transactions AS
WITH source_txs AS (
    SELECT 
        cycle_id,
        logic_id,
        exchange as source_exchange,
        symbol as source_symbol,
        side as source_side,
        qty_filled as source_qty,
        price_filled as source_price,
        ts_utc as source_ts,
        order_id as source_order_id,
        leg
    FROM router.transaction
    WHERE leg = 'SOURCE'
        AND cycle_id IS NOT NULL
        AND status = 'FILLED'
),
hedge_txs AS (
    SELECT 
        cycle_id,
        logic_id,
        exchange as hedge_exchange,
        symbol as hedge_symbol,
        side as hedge_side,
        qty_filled as hedge_qty,
        price_filled as hedge_price,
        ts_utc as hedge_ts,
        order_id as hedge_order_id,
        leg
    FROM router.transaction
    WHERE leg = 'HEDGE'
        AND cycle_id IS NOT NULL
        AND status = 'FILLED'
)
SELECT 
    COALESCE(s.cycle_id, h.cycle_id) as cycle_id,
    COALESCE(s.logic_id, h.logic_id) as logic_id,
    s.source_exchange,
    s.source_symbol,
    s.source_side,
    s.source_qty,
    s.source_price,
    s.source_ts,
    s.source_order_id,
    h.hedge_exchange,
    h.hedge_symbol,
    h.hedge_side,
    h.hedge_qty,
    h.hedge_price,
    h.hedge_ts,
    h.hedge_order_id,
    CASE 
        WHEN s.cycle_id IS NULL THEN 'MISSING_SOURCE'
        WHEN h.cycle_id IS NULL THEN 'MISSING_HEDGE'
        ELSE 'MATCHED'
    END as match_status,
    CASE 
        WHEN s.cycle_id IS NULL THEN 'Hedge order exists but no matching source order'
        WHEN h.cycle_id IS NULL THEN 'Source order exists but no matching hedge order - EXPOSURE RISK!'
        ELSE 'Both orders matched'
    END as match_description,
    -- Calculate exposure if hedge is missing
    CASE 
        WHEN h.cycle_id IS NULL AND s.source_qty IS NOT NULL AND s.source_price IS NOT NULL 
        THEN s.source_qty * s.source_price
        ELSE NULL
    END as unhedged_exposure_usd
FROM source_txs s
FULL OUTER JOIN hedge_txs h ON s.cycle_id = h.cycle_id
WHERE s.cycle_id IS NULL OR h.cycle_id IS NULL  -- Only show unmatched
ORDER BY COALESCE(s.source_ts, h.hedge_ts) DESC;

-- Index to improve query performance
CREATE INDEX IF NOT EXISTS idx_router_transaction_cycle_leg 
    ON router.transaction(cycle_id, leg) 
    WHERE cycle_id IS NOT NULL AND status = 'FILLED';

-- Query to check unmatched transactions
-- SELECT * FROM router.unmatched_transactions ORDER BY COALESCE(source_ts, hedge_ts) DESC LIMIT 50;

-- Query to check transactions with UNKNOWN logic_id (should be fixed)
-- SELECT cycle_id, logic_id, exchange, symbol, leg, ts_utc 
-- FROM router.transaction 
-- WHERE logic_id = 'UNKNOWN' AND cycle_id IS NOT NULL
-- ORDER BY ts_utc DESC;
