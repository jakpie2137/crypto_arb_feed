-- manager schema and manager.symbol table
-- Run when manager.symbol was dropped or never existed.
-- After this, run sync_manager_symbol_from_gcp.ps1 to populate data.

CREATE SCHEMA IF NOT EXISTS manager;

CREATE TABLE IF NOT EXISTS manager.symbol (
    id                  BIGSERIAL PRIMARY KEY,
    exchange            VARCHAR(64) NOT NULL,
    symbol              VARCHAR(64) NOT NULL,
    is_active           BOOLEAN NOT NULL DEFAULT true,
    base_curr           VARCHAR(32),
    quote_curr          VARCHAR(32),
    external_symbol     VARCHAR(64),
    external_base       VARCHAR(32),
    external_quote      VARCHAR(32),
    price_precision     INTEGER,
    volume_precision    INTEGER,
    min_vol             DOUBLE PRECISION,
    min_vol_type        VARCHAR(16),
    max_vol             DOUBLE PRECISION,
    max_vol_type        VARCHAR(16),
    fee_curr_buy        VARCHAR(16),
    fee_curr_sell       VARCHAR(16),
    fee_limit_buy       DOUBLE PRECISION,
    fee_market_buy      DOUBLE PRECISION,
    fee_limit_sell      DOUBLE PRECISION,
    fee_market_sell     DOUBLE PRECISION,
    quotation           DOUBLE PRECISION,
    symbol_type         VARCHAR(16),
    contract_size        DOUBLE PRECISION,
    max_position        DOUBLE PRECISION,
    max_position_type   VARCHAR(16),
    leverage            DOUBLE PRECISION
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_manager_symbol_exchange_symbol
    ON manager.symbol (exchange, symbol);

CREATE INDEX IF NOT EXISTS idx_manager_symbol_is_active
    ON manager.symbol (is_active) WHERE is_active = true;
