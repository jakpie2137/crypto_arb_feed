-- Add funding_rate from feed (OrderbookUpdate) to orderbook_snapshots
-- Funding rate applies to futures ref; spot rows stay NULL.
ALTER TABLE market_data.orderbook_snapshots
ADD COLUMN IF NOT EXISTS funding_rate DOUBLE PRECISION;
