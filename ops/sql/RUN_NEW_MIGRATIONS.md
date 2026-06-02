# 🚀 Uruchomienie nowych migracji SQL

## Migracje do uruchomienia (w kolejności):

1. **010_fix_mm_signal_columns.sql** - Dodaje brakujące kolumny do `mm.signal` (cycle_id, status, source_order_id, ref_order_id)
2. **011_add_missing_signal_columns.sql** - Dodaje kolumny source/ref exchange/symbol/side/price/qty, net_profit, pnl, fee_rates
3. **012_add_quotelevelexpo_columns.sql** - Dodaje kolumny quoteLevelExpo validation
4. **013_add_updated_at_to_mm_signal.sql** - Dodaje kolumnę `updated_at` (potrzebna dla SignalLogger)
5. **014_unmatched_transactions_view.sql** - Tworzy VIEW do monitorowania unmatched transactions (opcjonalna, nie krytyczna)
6. **015_add_ts_utc_str_to_reporter_transaction.sql** - Dodaje kolumnę `ts_utc_str` do `reporter.transaction` (naprawia błąd reportera)

## ⚠️ WAŻNE:

- Migracje 010-013 są **idempotentne** (używają `IF NOT EXISTS`) - bezpieczne do uruchomienia wielokrotnie
- Migracja 014 tworzy tylko VIEW i INDEX - nie jest krytyczna przed startem systemu, ale warto mieć

## 📋 Jak uruchomić:

### Metoda 1: Wszystkie naraz (NAJŁATWIEJSZA)

```bash
# Na serwerze AWS:
cd ~/repo

# Uruchom wszystkie nowe migracje (kopiuj i wklej wszystkie linie):
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/010_fix_mm_signal_columns.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/011_add_missing_signal_columns.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/012_add_quotelevelexpo_columns.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/013_add_updated_at_to_mm_signal.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/014_unmatched_transactions_view.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/015_add_ts_utc_str_to_reporter_transaction.sql
```

### Metoda 2: Tylko krytyczne (przed startem systemu)

Jeśli chcesz tylko to co **niezbędne** przed startem:

```bash
cd ~/repo

# Tylko migracje krytyczne (010-013, 015):
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/010_fix_mm_signal_columns.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/011_add_missing_signal_columns.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/012_add_quotelevelexpo_columns.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/013_add_updated_at_to_mm_signal.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/015_add_ts_utc_str_to_reporter_transaction.sql
```

Migrację 014 możesz uruchomić później (to tylko VIEW do monitorowania).

## ✅ Sprawdź czy działa:

```bash
# Sprawdź strukturę mm.signal:
docker exec -it trading-postgres psql -U postgres -d arb_test -c "\d mm.signal"

# Sprawdź czy view istnieje (jeśli uruchomiłeś 014):
docker exec -it trading-postgres psql -U postgres -d arb_test -c "\d router.unmatched_transactions"
```

## 📝 Notatki:

- **010-013**: Idempotentne - możesz uruchomić wielokrotnie bez problemu
- **014**: Tworzy VIEW - jeśli już istnieje, zostanie zastąpione (CREATE OR REPLACE)
- Wszystkie migracje są bezpieczne - nie usuwają danych
