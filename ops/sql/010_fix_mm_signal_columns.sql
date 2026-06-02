-- Fix missing columns in existing mm.signal table
DO $$
BEGIN
    -- Add cycle_id if missing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'cycle_id') THEN
        ALTER TABLE mm.signal ADD COLUMN cycle_id VARCHAR(50);
        RAISE NOTICE 'Added cycle_id to mm.signal';
    END IF;
    
    -- Add status if missing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'status') THEN
        ALTER TABLE mm.signal ADD COLUMN status VARCHAR(20) DEFAULT 'SIGNAL';
        RAISE NOTICE 'Added status to mm.signal';
    END IF;
    
    -- Add source_order_id if missing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'source_order_id') THEN
        ALTER TABLE mm.signal ADD COLUMN source_order_id VARCHAR(100);
        RAISE NOTICE 'Added source_order_id to mm.signal';
    END IF;
    
    -- Add ref_order_id if missing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ref_order_id') THEN
        ALTER TABLE mm.signal ADD COLUMN ref_order_id VARCHAR(100);
        RAISE NOTICE 'Added ref_order_id to mm.signal';
    END IF;
    
    -- Add unique constraint if missing
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'signal_logic_id_cycle_id_key'
    ) THEN
        ALTER TABLE mm.signal ADD CONSTRAINT signal_logic_id_cycle_id_key UNIQUE(logic_id, cycle_id);
        RAISE NOTICE 'Added unique constraint on (logic_id, cycle_id)';
    END IF;
    
    RAISE NOTICE 'mm.signal columns fixed';
END $$;
