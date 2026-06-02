-- Fix router.transaction table schema
-- Adds missing 'qty' column if it doesn't exist

CREATE SCHEMA IF NOT EXISTS router;

-- Check if qty column exists, if not add it
DO $$
BEGIN
    -- Check if table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'router' AND table_name = 'transaction') THEN
        
        -- Check if qty column exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'qty') THEN
            
            -- Add qty column
            ALTER TABLE router.transaction ADD COLUMN qty DECIMAL(24,8);
            
            RAISE NOTICE 'Added qty column to router.transaction';
        ELSE
            RAISE NOTICE 'Column qty already exists in router.transaction';
        END IF;
    ELSE
        -- Create table if it doesn't exist
        CREATE TABLE router.transaction (
            id BIGSERIAL PRIMARY KEY,
            ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            exchange VARCHAR(50) NOT NULL,
            symbol VARCHAR(50) NOT NULL,
            side VARCHAR(10) NOT NULL,
            order_type VARCHAR(20),
            qty DECIMAL(24,8),
            price DECIMAL(24,8),
            filled_qty DECIMAL(24,8),
            filled_price DECIMAL(24,8),
            fee DECIMAL(24,8),
            fee_asset VARCHAR(20),
            order_id VARCHAR(100),
            trade_id VARCHAR(100),
            status VARCHAR(20) DEFAULT 'FILLED',
            source_app VARCHAR(50),
            logic_id VARCHAR(100),
            pnl_gross_quote DECIMAL(24,8),
            pnl_net_quote DECIMAL(24,8),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(exchange, order_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_router_tx_ts ON router.transaction(ts_utc DESC);
        CREATE INDEX IF NOT EXISTS idx_router_tx_exchange ON router.transaction(exchange);
        CREATE INDEX IF NOT EXISTS idx_router_tx_symbol ON router.transaction(symbol);
        
        RAISE NOTICE 'Created router.transaction table';
    END IF;
END $$;
