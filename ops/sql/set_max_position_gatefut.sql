-- Ustaw max_position = 20 dla wszystkich GATEFUT futures
UPDATE manager.symbol
SET max_position = 20
WHERE symbol_type = 'FUTURES'
  AND exchange LIKE 'GATEFUT%';

-- Sprawdź ile rekordów zostało zaktualizowanych
SELECT 
    exchange,
    symbol,
    symbol_type,
    max_position,
    contract_size
FROM manager.symbol
WHERE symbol_type = 'FUTURES'
  AND exchange LIKE 'GATEFUT%'
ORDER BY exchange, symbol;
