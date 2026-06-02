-- Migration: Add contract_size column to reporter.position_info
-- Date: 2026-01-20
-- Issue: Exposure calculation needs contract_size to convert contracts to base currency

-- Add contract_size column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'reporter' 
        AND table_name = 'position_info' 
        AND column_name = 'contract_size'
    ) THEN
        ALTER TABLE reporter.position_info 
        ADD COLUMN contract_size DOUBLE PRECISION;
        
        COMMENT ON COLUMN reporter.position_info.contract_size IS 
            'Base currency per contract (from manager.symbol)';
    END IF;
END $$;
