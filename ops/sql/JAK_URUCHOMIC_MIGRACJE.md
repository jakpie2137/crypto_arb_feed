# 🚀 Jak uruchomić migracje SQL - PROSTO

## Metoda 1: Przez skrypt (najłatwiej)

```bash
# 1. Wejdź na serwer AWS
ssh root@twoj-serwer

# 2. Przejdź do katalogu repo
cd ~/repo

# 3. NADAJ UPRAWNIENIA (WAŻNE!):
chmod +x ops/scripts/run_all_migrations.sh

# 4. Uruchom migracje:
./ops/scripts/run_all_migrations.sh
```

**To wszystko!** Skrypt automatycznie:
- Sprawdzi czy kontener `trading-postgres` działa
- Wykona SQL w bazie `arb_test`
- Pokaże czy się udało

---

## Metoda 2: Bezpośrednio przez docker exec (NAJPROSTSZA - bez skryptu)

```bash
# Na serwerze:
cd ~/repo

# Uruchom każdą migrację (kopiuj i wklej wszystkie 3 linie):
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/006_mm_signal_table.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/007_router_transaction_add_cycle_id.sql
docker exec -i trading-postgres psql -U postgres -d arb_test < ops/sql/009_latency_timestamps.sql
```

**To działa zawsze, bez uprawnień!**

---

## Metoda 3: Bezpośrednio przez psql (jeśli docker exec nie działa)

```bash
# 1. Wejdź do psql
docker exec -it trading-postgres psql -U postgres -d arb_test

# 2. Wklej zawartość każdego pliku SQL (kopiuj cały tekst z pliku i wklej)
# Najpierw 006_mm_signal_table.sql, potem 007, potem 009

# 3. Wyjdź z psql
\q
```

---

## ✅ Sprawdź czy działa:

```bash
# Wejdź do psql
docker exec -it trading-postgres psql -U postgres -d arb_test

# Sprawdź czy tabela mm.signal istnieje:
\d mm.signal

# Sprawdź czy kolumny są w router.transaction:
\d router.transaction

# Sprawdź view latency:
\d router.latency_analysis

# Wyjdź:
\q
```

---

## ❌ Jeśli coś nie działa:

**Błąd: "Permission denied"**
```bash
# Nadaj uprawnienia:
chmod +x ops/scripts/run_all_migrations.sh

# LUB użyj metody 2 (bez skryptu) - zawsze działa!
```

**Błąd: "container trading-postgres not found"**
```bash
# Sprawdź nazwę kontenera:
docker ps | grep postgres
# Użyj prawdziwej nazwy zamiast "trading-postgres"
```

**Błąd: "file not found"**
```bash
# Sprawdź gdzie jesteś:
pwd
# Powinno być: /home/ubuntu/repo (lub gdzie masz repo)

# Sprawdź czy pliki istnieją:
ls -la ops/sql/*.sql
```
