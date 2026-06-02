-- Add cycle_id column to router.transaction for signal matching
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'router' AND table_name = 'transaction') THEN
        
        -- Add cycle_id column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' 
                       AND table_name = 'transaction' 
                       AND column_name = 'cycle_id') THEN
            ALTER TABLE router.transaction ADD COLUMN cycle_id VARCHAR(50);
            RAISE NOTICE 'Added cycle_id column to router.transaction';
        END IF;
        
        -- Add index for cycle_id matching
        CREATE INDEX IF NOT EXISTS idx_router_tx_cycle ON router.transaction(cycle_id);
        
        RAISE NOTICE 'router.transaction cycle_id column verified';
    END IF;
END $$;
