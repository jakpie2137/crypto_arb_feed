-- Add ts_utc_str column to reporter.transaction table
-- Fixes: ERROR: column "ts_utc_str" of relation "transaction" does not exist

CREATE SCHEMA IF NOT EXISTS reporter;

DO $$
BEGIN
    -- Check if table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'reporter' AND table_name = 'transaction') THEN
        
        -- Add ts_utc_str column if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'reporter' 
                       AND table_name = 'transaction' 
                       AND column_name = 'ts_utc_str') THEN
            ALTER TABLE reporter.transaction ADD COLUMN ts_utc_str VARCHAR(32);
            RAISE NOTICE 'Added ts_utc_str column to reporter.transaction';
        ELSE
            RAISE NOTICE 'Column ts_utc_str already exists in reporter.transaction';
        END IF;
        
    ELSE
        RAISE NOTICE 'Table reporter.transaction does not exist - will be created by SQLAlchemy metadata.create_all()';
    END IF;
END $$;
