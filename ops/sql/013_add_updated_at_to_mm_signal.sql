-- Add updated_at column to mm.signal if missing
-- This column is used by SignalLogger for tracking when signals are updated

DO $$
BEGIN
    -- Check if table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'mm' 
               AND table_name = 'signal') THEN
        
        -- Add updated_at column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'mm' 
                       AND table_name = 'signal' 
                       AND column_name = 'updated_at') THEN
            ALTER TABLE mm.signal ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW();
            RAISE NOTICE 'Added updated_at column to mm.signal';
        ELSE
            RAISE NOTICE 'mm.signal.updated_at column already exists';
        END IF;
        
        RAISE NOTICE 'mm.signal table columns verified';
    ELSE
        RAISE NOTICE 'mm.signal table does not exist - skipping';
    END IF;
END $$;
