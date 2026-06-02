-- Ustaw max_position = 80 i max_position_type = QUOTE dla wszystkich symboli FUTURES
UPDATE manager.symbol
SET max_position = 80,
    max_position_type = 'QUOTE'
WHERE symbol_type = 'FUTURES';

-- Weryfikacja
SELECT exchange, symbol, symbol_type, max_position, max_position_type
FROM manager.symbol
WHERE symbol_type = 'FUTURES'
ORDER BY exchange, symbol;
