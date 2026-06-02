# Jak uruchomić migracje SQL na AWS

## Kolejność uruchamiania

```bash
# 1. Połącz się z kontenerem PostgreSQL
docker exec -it trading-postgres psql -U postgres -d arb_test

# LUB jeśli łączysz się zdalnie przez SSH:
ssh root@your-aws-host
docker exec -it trading-postgres psql -U postgres -d arb_test
```

## Migracje do uruchomienia (w kolejności)

### Nowe tabele i kolumny:

```sql
-- 1. Tabela mm.signal (sygnały MM)
\i /path/to/006_mm_signal_table.sql

-- 2. Kolumna cycle_id w router.transaction
\i /path/to/007_router_transaction_add_cycle_id.sql

-- 3. Timestampy latency
\i /path/to/009_latency_timestamps.sql
```

### Albo kopiuj i wklejaj bezpośrednio:

```bash
# Skopiuj pliki SQL na serwer
scp ops/sql/006_mm_signal_table.sql root@aws-host:/tmp/
scp ops/sql/007_router_transaction_add_cycle_id.sql root@aws-host:/tmp/
scp ops/sql/009_latency_timestamps.sql root@aws-host:/tmp/

# Na serwerze:
docker exec -i trading-postgres psql -U postgres -d arb_test < /tmp/006_mm_signal_table.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < /tmp/007_router_transaction_add_cycle_id.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < /tmp/009_latency_timestamps.sql
```

### Albo po prostu wklej zawartość:

```bash
# Wejdź do psql
docker exec -it trading-postgres psql -U postgres -d arb_test

# Wklej zawartość każdego pliku SQL
# (kopiuj tekst z plików 006, 007, 009)
```

## Weryfikacja

```sql
-- Sprawdź czy tabela mm.signal istnieje
\d mm.signal

-- Sprawdź kolumny router.transaction
\d router.transaction

-- Sprawdź view latency_analysis
\d router.latency_analysis

-- Test view
SELECT * FROM router.latency_analysis LIMIT 5;
```

## Latency timestamps - co mierzymy

### MM perspective (mm.signal):
- `ts_signal_created` - MM wykrył okazję
- `ts_source_sent` - MM wysłał source order do routera
- `ts_source_ack` - MM otrzymał ACK (orderId) od routera
- `ts_hedge_sent` - MM wysłał hedge order do routera
- `ts_hedge_ack` - MM otrzymał ACK (orderId) od routera

### Router perspective (router.transaction):
- `ts_signal_received` - Router otrzymał sygnał od MM
- `ts_order_sent` - Router wysłał order do giełdy (API call)
- `ts_exchange_ack` - Giełda zwróciła ACK (orderId)
- `ts_filled` - Giełda potwierdziła fill

### Latency analysis:
```sql
SELECT * FROM router.latency_analysis 
WHERE ts_signal_created > NOW() - INTERVAL '1 hour' 
LIMIT 20;

-- Aggregate stats
SELECT 
    logic_id,
    COUNT(*) as trades,
    ROUND(AVG(total_mm_ms)::numeric, 1) as avg_mm_ms,
    ROUND(AVG(e2e_total_ms)::numeric, 1) as avg_e2e_ms,
    ROUND(MAX(e2e_total_ms)::numeric, 1) as max_e2e_ms,
    ROUND(AVG(src_exchange_ack_ms)::numeric, 1) as avg_src_ack_ms,
    ROUND(AVG(ref_exchange_ack_ms)::numeric, 1) as avg_ref_ack_ms
FROM router.latency_analysis
WHERE ts_signal_created > NOW() - INTERVAL '4 hours'
GROUP BY logic_id;
```
