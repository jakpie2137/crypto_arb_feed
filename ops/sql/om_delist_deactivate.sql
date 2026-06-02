-- OM delisting: deactivate all manager.symbol rows with base_curr='OM'
-- Run on: main DB and any replica/shard that has manager.symbol
-- Date: 2025-02

UPDATE manager.symbol
SET is_active = false
WHERE UPPER(base_curr) = 'OM';
