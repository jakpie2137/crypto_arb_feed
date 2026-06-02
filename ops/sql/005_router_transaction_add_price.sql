-- Add missing columns to router.transaction table
-- Fixes: ERROR: column "price" of relation "transaction" does not exist
-- Also creates the table if it doesn't exist

CREATE SCHEMA IF NOT EXISTS router;

DO $$
BEGIN
    -- Check if table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'router' AND table_name = 'transaction') THEN
        
        -- Add qty column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'qty') THEN
            ALTER TABLE router.transaction ADD COLUMN qty DECIMAL(24,8);
            RAISE NOTICE 'Added qty column to router.transaction';
        END IF;
        
        -- Add price column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'price') THEN
            ALTER TABLE router.transaction ADD COLUMN price DECIMAL(24,8);
            RAISE NOTICE 'Added price column to router.transaction';
        END IF;
        
        -- Add filled_qty column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'filled_qty') THEN
            ALTER TABLE router.transaction ADD COLUMN filled_qty DECIMAL(24,8);
            RAISE NOTICE 'Added filled_qty column to router.transaction';
        END IF;
        
        -- Add filled_price column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'filled_price') THEN
            ALTER TABLE router.transaction ADD COLUMN filled_price DECIMAL(24,8);
            RAISE NOTICE 'Added filled_price column to router.transaction';
        END IF;
        
        -- Add order_type column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'order_type') THEN
            ALTER TABLE router.transaction ADD COLUMN order_type VARCHAR(20);
            RAISE NOTICE 'Added order_type column to router.transaction';
        END IF;
        
        -- Add fee column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'fee') THEN
            ALTER TABLE router.transaction ADD COLUMN fee DECIMAL(24,8);
            RAISE NOTICE 'Added fee column to router.transaction';
        END IF;
        
        -- Add fee_asset column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'fee_asset') THEN
            ALTER TABLE router.transaction ADD COLUMN fee_asset VARCHAR(20);
            RAISE NOTICE 'Added fee_asset column to router.transaction';
        END IF;
        
        -- Add trade_id column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'trade_id') THEN
            ALTER TABLE router.transaction ADD COLUMN trade_id VARCHAR(100);
            RAISE NOTICE 'Added trade_id column to router.transaction';
        END IF;
        
        -- Add status column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'status') THEN
            ALTER TABLE router.transaction ADD COLUMN status VARCHAR(20) DEFAULT 'FILLED';
            RAISE NOTICE 'Added status column to router.transaction';
        END IF;
        
        -- Add source_app column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'source_app') THEN
            ALTER TABLE router.transaction ADD COLUMN source_app VARCHAR(50);
            RAISE NOTICE 'Added source_app column to router.transaction';
        END IF;
        
        -- Add logic_id column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'logic_id') THEN
            ALTER TABLE router.transaction ADD COLUMN logic_id VARCHAR(100);
            RAISE NOTICE 'Added logic_id column to router.transaction';
        END IF;
        
        -- Add pnl_gross_quote column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'pnl_gross_quote') THEN
            ALTER TABLE router.transaction ADD COLUMN pnl_gross_quote DECIMAL(24,8);
            RAISE NOTICE 'Added pnl_gross_quote column to router.transaction';
        END IF;
        
        -- Add pnl_net_quote column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'pnl_net_quote') THEN
            ALTER TABLE router.transaction ADD COLUMN pnl_net_quote DECIMAL(24,8);
            RAISE NOTICE 'Added pnl_net_quote column to router.transaction';
        END IF;
        
        -- Add updated_at column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'updated_at') THEN
            ALTER TABLE router.transaction ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW();
            RAISE NOTICE 'Added updated_at column to router.transaction';
        END IF;
        
        RAISE NOTICE 'router.transaction table columns verified';
    ELSE
        -- Create table if it doesn't exist (with all columns)
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
