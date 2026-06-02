-- Sync BITGET8SUB0, KUCOIN8SUB0, MEXC8SUB0 from their main exchanges (BITGET, KUCOIN, MEXC)
-- 1) DELETE all symbols from sub0 exchanges
-- 2) COPY all ACTIVE (is_active=true) symbols from main to sub0
--
-- Result: sub0 exchanges have identical active symbol set as their main counterparts.
-- Run on: main DB
--
-- WARNING: This will DELETE existing sub0 symbol rows. Ensure you have a backup if needed.

BEGIN;

-- =============================================================================
-- Step 1: Delete all symbols from sub0 exchanges
-- =============================================================================
DELETE FROM manager.symbol
WHERE UPPER(exchange) IN ('BITGET8SUB0', 'KUCOIN8SUB0', 'MEXC8SUB0');

-- =============================================================================
-- Step 2: Copy active symbols from BITGET -> BITGET8SUB0
-- =============================================================================
INSERT INTO manager.symbol (
    id, exchange, symbol, is_active,
    base_curr, quote_curr,
    external_symbol, external_base, external_quote,
    price_precision, volume_precision,
    min_vol, min_vol_type,
    max_vol, max_vol_type,
    fee_curr_buy, fee_curr_sell,
    fee_limit_buy, fee_market_buy,
    fee_limit_sell, fee_market_sell,
    quotation, symbol_type,
    contract_size, max_position,
    max_position_type, leverage
)
SELECT
    (SELECT COALESCE(MAX(id), 0) FROM manager.symbol) + ROW_NUMBER() OVER (ORDER BY symbol),
    'BITGET8SUB0',
    symbol, is_active,
    base_curr, quote_curr,
    external_symbol, external_base, external_quote,
    price_precision, volume_precision,
    min_vol, min_vol_type,
    max_vol, max_vol_type,
    fee_curr_buy, fee_curr_sell,
    fee_limit_buy, fee_market_buy,
    fee_limit_sell, fee_market_sell,
    quotation, symbol_type,
    contract_size, max_position,
    max_position_type, leverage
FROM manager.symbol
WHERE UPPER(exchange) = 'BITGET' AND is_active = true;

-- =============================================================================
-- Step 3: Copy active symbols from KUCOIN -> KUCOIN8SUB0
-- =============================================================================
INSERT INTO manager.symbol (
    id, exchange, symbol, is_active,
    base_curr, quote_curr,
    external_symbol, external_base, external_quote,
    price_precision, volume_precision,
    min_vol, min_vol_type,
    max_vol, max_vol_type,
    fee_curr_buy, fee_curr_sell,
    fee_limit_buy, fee_market_buy,
    fee_limit_sell, fee_market_sell,
    quotation, symbol_type,
    contract_size, max_position,
    max_position_type, leverage
)
SELECT
    (SELECT COALESCE(MAX(id), 0) FROM manager.symbol) + ROW_NUMBER() OVER (ORDER BY symbol),
    'KUCOIN8SUB0',
    symbol, is_active,
    base_curr, quote_curr,
    external_symbol, external_base, external_quote,
    price_precision, volume_precision,
    min_vol, min_vol_type,
    max_vol, max_vol_type,
    fee_curr_buy, fee_curr_sell,
    fee_limit_buy, fee_market_buy,
    fee_limit_sell, fee_market_sell,
    quotation, symbol_type,
    contract_size, max_position,
    max_position_type, leverage
FROM manager.symbol
WHERE UPPER(exchange) = 'KUCOIN' AND is_active = true;

-- =============================================================================
-- Step 4: Copy active symbols from MEXC -> MEXC8SUB0
-- =============================================================================
INSERT INTO manager.symbol (
    id, exchange, symbol, is_active,
    base_curr, quote_curr,
    external_symbol, external_base, external_quote,
    price_precision, volume_precision,
    min_vol, min_vol_type,
    max_vol, max_vol_type,
    fee_curr_buy, fee_curr_sell,
    fee_limit_buy, fee_market_buy,
    fee_limit_sell, fee_market_sell,
    quotation, symbol_type,
    contract_size, max_position,
    max_position_type, leverage
)
SELECT
    (SELECT COALESCE(MAX(id), 0) FROM manager.symbol) + ROW_NUMBER() OVER (ORDER BY symbol),
    'MEXC8SUB0',
    symbol, is_active,
    base_curr, quote_curr,
    external_symbol, external_base, external_quote,
    price_precision, volume_precision,
    min_vol, min_vol_type,
    max_vol, max_vol_type,
    fee_curr_buy, fee_curr_sell,
    fee_limit_buy, fee_market_buy,
    fee_limit_sell, fee_market_sell,
    quotation, symbol_type,
    contract_size, max_position,
    max_position_type, leverage
FROM manager.symbol
WHERE UPPER(exchange) = 'MEXC' AND is_active = true;

COMMIT;

-- Verification (run separately after commit):
-- SELECT exchange, COUNT(*), COUNT(*) FILTER (WHERE is_active) as active_count
-- FROM manager.symbol
-- WHERE UPPER(exchange) IN ('BITGET', 'BITGET8SUB0', 'KUCOIN', 'KUCOIN8SUB0', 'MEXC', 'MEXC8SUB0')
-- GROUP BY exchange ORDER BY exchange;
