-- Add cross1, cross2 columns to router.transaction for PnL/expPnL
-- When logic trades with cross, we store the cross rate at signal time.
-- Example: cross1 = 1.0850 (EUR/USDT rate when signal was generated)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'router' AND table_name = 'transaction') THEN
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' AND table_name = 'transaction' 
                       AND column_name = 'cross1') THEN
            ALTER TABLE router.transaction ADD COLUMN cross1 DECIMAL(24,8);
            RAISE NOTICE 'Added cross1 column to router.transaction';
        ELSE
            RAISE NOTICE 'Column cross1 already exists in router.transaction';
        END IF;
        
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_schema = 'router' AND table_name = 'transaction' 
                       AND column_name = 'cross2') THEN
            ALTER TABLE router.transaction ADD COLUMN cross2 DECIMAL(24,8);
            RAISE NOTICE 'Added cross2 column to router.transaction';
        ELSE
            RAISE NOTICE 'Column cross2 already exists in router.transaction';
        END IF;
        
        RAISE NOTICE 'router.transaction cross1/cross2 columns verified';
    ELSE
        RAISE NOTICE 'router.transaction table does not exist - run base migrations first';
    END IF;
END $$;
