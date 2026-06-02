-- Unique index na orderbook_snapshots – zapobiega duplikatom (ten sam snapshot_time+exchange+symbol).
-- Snapshot feed używa ON CONFLICT DO NOTHING.
-- Idempotentny – można puszczać wielokrotnie (pomiń CREATE jeśli index już jest).
--
-- Krok 1: usuń duplikaty (zostaw wiersz z max id per snapshot_time,exchange,symbol)
DELETE FROM market_data.orderbook_snapshots a
USING market_data.orderbook_snapshots b
WHERE a.snapshot_time = b.snapshot_time
  AND a.exchange = b.exchange
  AND a.symbol = b.symbol
  AND a.id < b.id;

-- Krok 2: dodaj unikalny index tylko gdy nie istnieje
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'market_data' AND tablename = 'orderbook_snapshots'
      AND indexname = 'idx_orderbook_snapshots_unique_tick'
  ) THEN
    CREATE UNIQUE INDEX idx_orderbook_snapshots_unique_tick
      ON market_data.orderbook_snapshots (snapshot_time, exchange, symbol);
  END IF;
END $$;
