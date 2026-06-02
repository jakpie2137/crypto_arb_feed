-- ==============================================================================
-- Ustaw fee na 0.0004 (4bps) dla BINANCEFUT i BINANCEFUT8SUBFIN
-- ==============================================================================
-- Finandy/Binance Futures: taker 0.04% (4bps), maker 0.02% – zakładamy 4bps
-- wszędzie (gdy BNB discount zadziała, będzie ~0.036%, miła niespodzianka).
--
-- Uruchomienie:
--   psql -U postgres -d arb_test -f ops/sql/016_set_binancefut_fee_4bps.sql
-- ==============================================================================

UPDATE manager.symbol
SET fee_limit_buy   = 0.0004,
    fee_market_buy  = 0.0004,
    fee_limit_sell  = 0.0004,
    fee_market_sell = 0.0004
WHERE UPPER(exchange) IN ('BINANCEFUT', 'BINANCEFUT8SUBFIN');

-- Weryfikacja
SELECT exchange, symbol,
       fee_limit_buy, fee_market_buy, fee_limit_sell, fee_market_sell
FROM manager.symbol
WHERE UPPER(exchange) IN ('BINANCEFUT', 'BINANCEFUT8SUBFIN')
ORDER BY exchange, symbol
LIMIT 20;
