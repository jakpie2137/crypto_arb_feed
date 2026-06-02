-- Migration: Add margin column to reporter.position_info
-- This column stores the margin value reported directly from Gate.io API
-- (instead of calculating initial_margin + maintenance_margin)

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'reporter'
        AND table_name = 'position_info'
        AND column_name = 'margin'
    ) THEN
        ALTER TABLE reporter.position_info 
        ADD COLUMN margin DOUBLE PRECISION;
        
        COMMENT ON COLUMN reporter.position_info.margin IS
            'Margin reported directly from exchange API (Gate.io position.margin). This is the actual margin used by the position, not a calculated sum.';
        
        RAISE NOTICE 'Added margin column to reporter.position_info';
    ELSE
        RAISE NOTICE 'Column margin already exists in reporter.position_info';
    END IF;
END $$;
