-- Add quoteLevelExpo validation result columns to mm.signal
DO $$
BEGIN
    -- quote_level_expo_checked: whether validation was performed
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'quote_level_expo_checked') THEN
        ALTER TABLE mm.signal ADD COLUMN quote_level_expo_checked BOOLEAN;
        RAISE NOTICE 'Added quote_level_expo_checked to mm.signal';
    END IF;
    
    -- quote_level_expo_allowed: whether order was allowed by quoteLevelExpo
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'quote_level_expo_allowed') THEN
        ALTER TABLE mm.signal ADD COLUMN quote_level_expo_allowed BOOLEAN;
        RAISE NOTICE 'Added quote_level_expo_allowed to mm.signal';
    END IF;
    
    -- quote_level_expo_required_gain: required gain threshold based on balance
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'quote_level_expo_required_gain') THEN
        ALTER TABLE mm.signal ADD COLUMN quote_level_expo_required_gain DECIMAL(24,8);
        RAISE NOTICE 'Added quote_level_expo_required_gain to mm.signal';
    END IF;
    
    -- quote_level_expo_actual_gain: actual gain of the trade
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'quote_level_expo_actual_gain') THEN
        ALTER TABLE mm.signal ADD COLUMN quote_level_expo_actual_gain DECIMAL(24,8);
        RAISE NOTICE 'Added quote_level_expo_actual_gain to mm.signal';
    END IF;
    
    -- quote_level_expo_balance: quote currency balance at validation time
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'quote_level_expo_balance') THEN
        ALTER TABLE mm.signal ADD COLUMN quote_level_expo_balance DECIMAL(24,8);
        RAISE NOTICE 'Added quote_level_expo_balance to mm.signal';
    END IF;
    
    RAISE NOTICE 'quoteLevelExpo columns added to mm.signal';
END $$;
