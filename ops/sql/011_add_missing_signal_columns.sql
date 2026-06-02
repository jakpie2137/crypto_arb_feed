-- Add missing columns to mm.signal for new signal logging
-- (Table already exists with different structure, so we add what's missing)
DO $$
BEGIN
    -- Source side columns
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'source_exchange') THEN
        ALTER TABLE mm.signal ADD COLUMN source_exchange VARCHAR(50);
        RAISE NOTICE 'Added source_exchange to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'source_symbol') THEN
        ALTER TABLE mm.signal ADD COLUMN source_symbol VARCHAR(50);
        RAISE NOTICE 'Added source_symbol to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'source_side') THEN
        ALTER TABLE mm.signal ADD COLUMN source_side VARCHAR(10);
        RAISE NOTICE 'Added source_side to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'source_price') THEN
        ALTER TABLE mm.signal ADD COLUMN source_price DECIMAL(24,8);
        RAISE NOTICE 'Added source_price to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'source_qty') THEN
        ALTER TABLE mm.signal ADD COLUMN source_qty DECIMAL(24,8);
        RAISE NOTICE 'Added source_qty to mm.signal';
    END IF;
    
    -- Reference side columns
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ref_exchange') THEN
        ALTER TABLE mm.signal ADD COLUMN ref_exchange VARCHAR(50);
        RAISE NOTICE 'Added ref_exchange to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ref_symbol') THEN
        ALTER TABLE mm.signal ADD COLUMN ref_symbol VARCHAR(50);
        RAISE NOTICE 'Added ref_symbol to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ref_side') THEN
        ALTER TABLE mm.signal ADD COLUMN ref_side VARCHAR(10);
        RAISE NOTICE 'Added ref_side to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ref_price') THEN
        ALTER TABLE mm.signal ADD COLUMN ref_price DECIMAL(24,8);
        RAISE NOTICE 'Added ref_price to mm.signal';
    END IF;
    
    -- Metrics columns
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'net_profit') THEN
        ALTER TABLE mm.signal ADD COLUMN net_profit DECIMAL(24,8);
        RAISE NOTICE 'Added net_profit to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'pnl_gross_quote') THEN
        ALTER TABLE mm.signal ADD COLUMN pnl_gross_quote DECIMAL(24,8);
        RAISE NOTICE 'Added pnl_gross_quote to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'pnl_net_quote') THEN
        ALTER TABLE mm.signal ADD COLUMN pnl_net_quote DECIMAL(24,8);
        RAISE NOTICE 'Added pnl_net_quote to mm.signal';
    END IF;
    
    -- Fee rate columns
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'fee_rate_source') THEN
        ALTER TABLE mm.signal ADD COLUMN fee_rate_source DECIMAL(24,8);
        RAISE NOTICE 'Added fee_rate_source to mm.signal';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'fee_rate_ref') THEN
        ALTER TABLE mm.signal ADD COLUMN fee_rate_ref DECIMAL(24,8);
        RAISE NOTICE 'Added fee_rate_ref to mm.signal';
    END IF;
    
    -- Add index on cycle_id if missing
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE schemaname = 'mm' AND tablename = 'signal' AND indexname = 'idx_mm_signal_cycle'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_mm_signal_cycle ON mm.signal(cycle_id);
        RAISE NOTICE 'Added index on cycle_id';
    END IF;
    
    -- Add index on status if missing
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE schemaname = 'mm' AND tablename = 'signal' AND indexname = 'idx_mm_signal_status'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_mm_signal_status ON mm.signal(status);
        RAISE NOTICE 'Added index on status';
    END IF;
    
    RAISE NOTICE 'All missing columns added to mm.signal';
END $$;
