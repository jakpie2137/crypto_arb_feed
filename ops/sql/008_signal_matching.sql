-- Signal matching view: JOIN router.transaction with mm.signal by cycle_id
-- cycle_id is passed directly from MM to router, so matching is straightforward

-- View to see matched signals with their transactions
CREATE OR REPLACE VIEW router.signal_transaction_match AS
SELECT 
    s.id as signal_id,
    s.ts_utc as signal_ts,
    s.logic_id,
    s.cycle_id,
    s.source_exchange,
    s.source_symbol,
    s.source_side,
    s.source_price as signal_source_price,
    s.source_qty as signal_source_qty,
    s.ref_exchange,
    s.ref_symbol,
    s.ref_price as signal_ref_price,
    s.net_profit,
    s.pnl_gross_quote as signal_pnl_gross,
    s.pnl_net_quote as signal_pnl_net,
    s.status as signal_status,
    s.source_order_id,
    s.ref_order_id,
    -- Source transaction
    src_tx.id as src_tx_id,
    src_tx.ts_utc as src_tx_ts,
    src_tx.qty_filled as src_filled_qty,
    src_tx.price_filled as src_filled_price,
    src_tx.status as src_status,
    -- Reference transaction
    ref_tx.id as ref_tx_id,
    ref_tx.ts_utc as ref_tx_ts,
    ref_tx.qty_filled as ref_filled_qty,
    ref_tx.price_filled as ref_filled_price,
    ref_tx.status as ref_status,
    -- Calculated execution metrics
    EXTRACT(EPOCH FROM (src_tx.ts_utc - s.ts_utc)) * 1000 as src_latency_ms,
    EXTRACT(EPOCH FROM (ref_tx.ts_utc - src_tx.ts_utc)) * 1000 as hedge_latency_ms
FROM mm.signal s
LEFT JOIN router.transaction src_tx 
    ON src_tx.cycle_id = s.cycle_id 
    AND src_tx.exchange = s.source_exchange
LEFT JOIN router.transaction ref_tx 
    ON ref_tx.cycle_id = s.cycle_id 
    AND ref_tx.exchange = s.ref_exchange
ORDER BY s.ts_utc DESC;

-- Query to show recent matched signals
SELECT * FROM router.signal_transaction_match 
WHERE signal_ts > NOW() - INTERVAL '1 hour'
LIMIT 50;

-- Query to find unmatched signals (no transactions)
SELECT 
    s.id, s.ts_utc, s.logic_id, s.cycle_id, 
    s.source_exchange, s.source_symbol, s.source_side,
    s.status
FROM mm.signal s
LEFT JOIN router.transaction tx ON tx.cycle_id = s.cycle_id
WHERE tx.id IS NULL
  AND s.ts_utc > NOW() - INTERVAL '1 hour'
ORDER BY s.ts_utc DESC;
