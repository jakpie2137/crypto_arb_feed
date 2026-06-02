-- Migration script for router.transaction table
-- Run this manually before deploying the new router version
-- This ensures the table has all required columns without ALTER TABLE operations in runtime

-- Add time_in_force column (for LIMIT/MARKET/IOC distinction)
ALTER TABLE router.transaction ADD COLUMN IF NOT EXISTS time_in_force VARCHAR(20);

-- Verify all required columns exist
-- If any column is missing, add it manually:
-- ALTER TABLE router.transaction ADD COLUMN IF NOT EXISTS <column_name> <type>;

-- Expected columns (for reference):
-- id, ts_utc, exchange, symbol, side, order_type, time_in_force,
-- qty_requested, price_requested, qty_filled, price_filled,
-- fee, fee_asset, order_id, client_order_id, trade_id, status,
-- source_app, logic_id, cycle_id, pnl_gross_quote, pnl_net_quote,
-- app_id, leg, ts_signal_received, ts_order_sent, ts_exchange_ack, ts_filled,
-- matched_reporter_tx_id, created_at, updated_at

-- Verify indexes exist
CREATE INDEX IF NOT EXISTS idx_router_tx_ts ON router.transaction(ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_router_tx_cycle_id ON router.transaction(cycle_id);
CREATE INDEX IF NOT EXISTS idx_router_tx_logic_id ON router.transaction(logic_id);
CREATE INDEX IF NOT EXISTS idx_router_tx_exchange_order ON router.transaction(exchange, order_id);
