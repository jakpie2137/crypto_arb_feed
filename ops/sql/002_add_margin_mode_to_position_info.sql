-- Migration: Add margin_mode column to reporter.position_info
-- Date: 2026-01-20
-- Issue: Code tries to write margin_mode but column doesn't exist

-- Add margin_mode column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'reporter' 
        AND table_name = 'position_info' 
        AND column_name = 'margin_mode'
    ) THEN
        ALTER TABLE reporter.position_info 
        ADD COLUMN margin_mode VARCHAR(16);
        
        COMMENT ON COLUMN reporter.position_info.margin_mode IS 
            'Margin mode: cross or isolated';
    END IF;
END $$;
