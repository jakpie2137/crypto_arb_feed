-- ==============================================================================
-- ODTWORZENIE USUNIĘTYCH TABEL (mm, router)
-- ==============================================================================
-- Skrypt odtworzenia tabel usuniętych przy zamknięciu starego serwera.
-- Źródło: analiza kodu (LogicRegistryRepository, SignalLogger, TransactionLogger)
-- ==============================================================================
-- Kolejność wykonywania:
--   1. Schematy
--   2. mm.app_registry (FK z logic_registry)
--   3. mm.logic_registry
--   4. mm.logic_runs
--   5. mm.signal
--   6. router.transaction
-- ==============================================================================

-- ==============================================================================
-- 1. SCHEMATY
-- ==============================================================================
CREATE SCHEMA IF NOT EXISTS mm;
CREATE SCHEMA IF NOT EXISTS router;

-- ==============================================================================
-- 2. mm.app_registry
-- ==============================================================================
-- Używane przez: LogicRegistryRepository.registerAppStart, updateAppLogicsCount
-- FK: mm.logic_registry.app_registry_id → mm.app_registry.id
-- ==============================================================================
CREATE TABLE IF NOT EXISTS mm.app_registry (
    id                  BIGSERIAL PRIMARY KEY,
    app_id              VARCHAR(100) NOT NULL,
    instance_id         VARCHAR(100) NOT NULL,
    config_path         VARCHAR(500),
    started_at          TIMESTAMP NOT NULL,
    stopped_at          TIMESTAMP,
    is_running          BOOLEAN NOT NULL DEFAULT true,
    logics_count        INTEGER,
    active_logics_count INTEGER,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (app_id, started_at)
);

CREATE INDEX IF NOT EXISTS ix_app_registry_app_id ON mm.app_registry (app_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_app_registry_instance ON mm.app_registry (instance_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_app_registry_running ON mm.app_registry (is_running) WHERE is_running = true;

-- ==============================================================================
-- 3. mm.logic_registry
-- ==============================================================================
-- Używane przez: LogicRegistryRepository.upsertLogicStatus (enhanced)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS mm.logic_registry (
    id                     VARCHAR(200) PRIMARY KEY,
    app_id                 VARCHAR(100) NOT NULL,
    instance_id            VARCHAR(100),
    is_active              BOOLEAN NOT NULL DEFAULT false,
    strategy_type          VARCHAR(50),
    source_exchange        VARCHAR(50),
    source_symbol          VARCHAR(50),
    reference_exchange     VARCHAR(50),
    reference_symbol       VARCHAR(50),
    first_registered_ts    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_registered_ts     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_ts           TIMESTAMP,
    last_run_ts            TIMESTAMP,
    config_json            TEXT,
    version                INTEGER NOT NULL DEFAULT 1,
    app_registry_id        BIGINT REFERENCES mm.app_registry(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS ix_logic_registry_app_id ON mm.logic_registry (app_id);
CREATE INDEX IF NOT EXISTS ix_logic_registry_instance ON mm.logic_registry (instance_id);
CREATE INDEX IF NOT EXISTS ix_logic_registry_active ON mm.logic_registry (is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS ix_logic_registry_source ON mm.logic_registry (source_exchange, source_symbol);
CREATE INDEX IF NOT EXISTS ix_logic_registry_reference ON mm.logic_registry (reference_exchange, reference_symbol);
CREATE INDEX IF NOT EXISTS ix_logic_registry_last_seen ON mm.logic_registry (last_seen_ts DESC);

-- Fallback: prostsza wersja logic_registry (jeśli enhanced powoduje konflikt)
-- Używana przez starszy upsertLogicStatus(appId, logic):
--   id, app_id, is_active, source_exchange, source_symbol, reference_exchange, reference_symbol, registered_ts, last_run_ts
-- Jeśli 002_logic_registry_schema.sql już wykonał DROP, to powyższa tabela jest ok.
-- Jeśli full_structure.sql ma starszą wersję (registered_ts, last_run_ts) - trzeba by ALTER.
-- Ten skrypt zakłada odtworzenie od zera - enhanced schema.

-- ==============================================================================
-- 4. mm.logic_runs
-- ==============================================================================
-- Używane przez: LogicRegistryRepository.saveRun
-- Kolumny: run_id, logic_id, app_id, start_time, end_time, strategy_type,
--          exchange_a, symbol_a, exchange_b, symbol_b, config_json, status
-- ==============================================================================
CREATE TABLE IF NOT EXISTS mm.logic_runs (
    run_id         UUID PRIMARY KEY,
    logic_id       VARCHAR(200) NOT NULL,
    app_id         VARCHAR(100),
    start_time     TIMESTAMP NOT NULL,
    end_time       TIMESTAMP,
    strategy_type  VARCHAR(50),
    exchange_a     VARCHAR(50),
    symbol_a       VARCHAR(50),
    exchange_b     VARCHAR(50),
    symbol_b       VARCHAR(50),
    config_json    TEXT,
    status         VARCHAR(50) NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_logic_runs_logic_id ON mm.logic_runs (logic_id, start_time DESC);
CREATE INDEX IF NOT EXISTS ix_logic_runs_app_id ON mm.logic_runs (app_id, start_time DESC);
CREATE INDEX IF NOT EXISTS ix_logic_runs_status ON mm.logic_runs (status, start_time DESC);
CREATE INDEX IF NOT EXISTS ix_logic_runs_start_time ON mm.logic_runs (start_time DESC);

-- ==============================================================================
-- 5. mm.signal
-- ==============================================================================
-- Używane przez: SignalLogger (ArbitrageStrategy → mm.core.SignalLogger)
-- Łączy sygnały MM z transakcjami router.transaction (cycle_id)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS mm.signal (
    id BIGSERIAL PRIMARY KEY,
    ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    logic_id VARCHAR(100) NOT NULL,
    cycle_id VARCHAR(100) NOT NULL,
    app_id VARCHAR(50) DEFAULT 'MM',
    source_exchange VARCHAR(50),
    source_symbol VARCHAR(50),
    source_side VARCHAR(10),
    source_price DECIMAL(24,8),
    source_qty DECIMAL(24,8),
    source_order_id VARCHAR(100),
    ref_exchange VARCHAR(50),
    ref_symbol VARCHAR(50),
    ref_side VARCHAR(10),
    ref_price DECIMAL(24,8),
    ref_order_id VARCHAR(100),
    net_profit DECIMAL(24,8),
    pnl_gross_quote DECIMAL(24,8),
    pnl_net_quote DECIMAL(24,8),
    fee_rate_source DECIMAL(12,8),
    fee_rate_ref DECIMAL(12,8),
    status VARCHAR(20) DEFAULT 'SIGNAL',
    ts_signal_created TIMESTAMPTZ,
    ts_source_sent TIMESTAMPTZ,
    ts_source_ack TIMESTAMPTZ,
    ts_hedge_sent TIMESTAMPTZ,
    ts_hedge_ack TIMESTAMPTZ,
    quote_level_expo_checked BOOLEAN DEFAULT FALSE,
    quote_level_expo_allowed BOOLEAN,
    quote_level_expo_required_gain DECIMAL(24,8),
    quote_level_expo_actual_gain DECIMAL(24,8),
    quote_level_expo_balance DECIMAL(24,8),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(logic_id, cycle_id)
);

CREATE INDEX IF NOT EXISTS idx_mm_signal_ts ON mm.signal(ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_mm_signal_logic ON mm.signal(logic_id);
CREATE INDEX IF NOT EXISTS idx_mm_signal_cycle ON mm.signal(cycle_id);
CREATE INDEX IF NOT EXISTS idx_mm_signal_status ON mm.signal(status);

-- ==============================================================================
-- 6. router.transaction
-- ==============================================================================
-- Używane przez: TransactionLogger (router)
-- GlobalBalanceService ładuje delty z tej tabeli.
-- Krytyczne dla zapisu transakcji i odświeżania balansów.
-- ==============================================================================
CREATE TABLE IF NOT EXISTS router.transaction (
    id BIGSERIAL PRIMARY KEY,
    ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exchange VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    side VARCHAR(10) NOT NULL,
    order_type VARCHAR(20),
    time_in_force VARCHAR(20),
    qty_requested DECIMAL(24,8),
    price_requested DECIMAL(24,8),
    qty_filled DECIMAL(24,8),
    price_filled DECIMAL(24,8),
    fee DECIMAL(24,8),
    fee_asset VARCHAR(20),
    order_id VARCHAR(100),
    client_order_id VARCHAR(100),
    trade_id VARCHAR(100),
    status VARCHAR(20) DEFAULT 'FILLED',
    source_app VARCHAR(50),
    logic_id VARCHAR(100),
    cycle_id VARCHAR(100),
    pnl_gross_quote DECIMAL(24,8),
    pnl_net_quote DECIMAL(24,8),
    app_id VARCHAR(50) DEFAULT 'ROUTER',
    leg VARCHAR(20) DEFAULT 'HEDGE',
    ts_signal_received TIMESTAMPTZ,
    ts_order_sent TIMESTAMPTZ,
    ts_exchange_ack TIMESTAMPTZ,
    ts_filled TIMESTAMPTZ,
    matched_reporter_tx_id BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(exchange, order_id)
);

CREATE INDEX IF NOT EXISTS idx_router_tx_ts ON router.transaction(ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_router_tx_exchange ON router.transaction(exchange);
CREATE INDEX IF NOT EXISTS idx_router_tx_symbol ON router.transaction(symbol);
CREATE INDEX IF NOT EXISTS idx_router_tx_cycle ON router.transaction(cycle_id);
CREATE INDEX IF NOT EXISTS idx_router_tx_logic ON router.transaction(logic_id);

-- ==============================================================================
-- OPCJONALNIE: widoki (jeśli używane przez dashboard/raporty)
-- ==============================================================================
-- CREATE OR REPLACE VIEW mm.app_registry_current AS ...
-- CREATE OR REPLACE VIEW mm.logic_registry_current AS ...
-- (patrz ops/sql/002_logic_registry_schema.sql)
