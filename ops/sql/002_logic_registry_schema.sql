-- ==============================================================================
-- Logic Registry Schema Enhancement
-- ==============================================================================
-- Enhanced schema for tracking MM app instances and their logics
-- History of app starts/stops and logic registrations
-- ==============================================================================

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS mm;

-- ==============================================================================
-- TABLE: mm.app_registry
-- ==============================================================================
-- Tracks app instance startup/shutdown history
-- ==============================================================================

CREATE TABLE IF NOT EXISTS mm.app_registry (
    id                  BIGSERIAL PRIMARY KEY,
    app_id              VARCHAR(100) NOT NULL,
    instance_id         VARCHAR(100) NOT NULL,  -- Container name (e.g., trading-mm-bitget-01)
    config_path         VARCHAR(500),            -- Path to YAML config file
    started_at          TIMESTAMP NOT NULL,      -- When app started
    stopped_at          TIMESTAMP,               -- When app stopped (NULL if still running)
    is_running          BOOLEAN NOT NULL DEFAULT true,
    logics_count        INTEGER,                 -- Number of logics registered in this run
    active_logics_count INTEGER,                 -- Number of active logics
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE (app_id, started_at)  -- Prevent duplicate entries for same app/start_time
);

-- Indexes for app_registry
CREATE INDEX IF NOT EXISTS ix_app_registry_app_id ON mm.app_registry (app_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_app_registry_instance ON mm.app_registry (instance_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_app_registry_running ON mm.app_registry (is_running) WHERE is_running = true;

-- ==============================================================================
-- TABLE: mm.logic_registry (ENHANCED)
-- ==============================================================================
-- Current state of all logics across all app instances
-- Enhanced with more tracking fields
-- ==============================================================================

-- Drop and recreate with enhanced schema
DROP TABLE IF EXISTS mm.logic_registry CASCADE;

CREATE TABLE mm.logic_registry (
    id                     VARCHAR(200) PRIMARY KEY,  -- logic_id (e.g., bitget8sub0_bnb-usdt_gatefut)
    app_id                 VARCHAR(100) NOT NULL,     -- App instance ID (e.g., ml_bitget8sub0_01)
    instance_id            VARCHAR(100),              -- Container name (e.g., trading-mm-bitget-01)
    
    -- Logic configuration
    is_active              BOOLEAN NOT NULL DEFAULT false,
    strategy_type          VARCHAR(50),               -- ARB_HYBRID, etc.
    
    -- Source exchange/symbol
    source_exchange        VARCHAR(50),
    source_symbol          VARCHAR(50),
    
    -- Reference exchange/symbol
    reference_exchange     VARCHAR(50),
    reference_symbol       VARCHAR(50),
    
    -- Timestamps
    first_registered_ts    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- First time seen
    last_registered_ts     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- Last registration
    last_seen_ts           TIMESTAMP,                                      -- Last time seen running
    last_run_ts            TIMESTAMP,                                      -- Last execution time
    
    -- Metadata
    config_json            TEXT,                      -- Full config JSON (optional, for reference)
    version                INTEGER NOT NULL DEFAULT 1, -- Increment on each registration
    
    -- Foreign key to app_registry (optional, can be NULL)
    app_registry_id        BIGINT,  -- Reference to app_registry.id
    
    CONSTRAINT fk_logic_registry_app 
        FOREIGN KEY (app_registry_id) 
        REFERENCES mm.app_registry(id)
        ON DELETE SET NULL
);

-- Indexes for logic_registry
CREATE INDEX IF NOT EXISTS ix_logic_registry_app_id ON mm.logic_registry (app_id);
CREATE INDEX IF NOT EXISTS ix_logic_registry_instance ON mm.logic_registry (instance_id);
CREATE INDEX IF NOT EXISTS ix_logic_registry_active ON mm.logic_registry (is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS ix_logic_registry_source ON mm.logic_registry (source_exchange, source_symbol);
CREATE INDEX IF NOT EXISTS ix_logic_registry_reference ON mm.logic_registry (reference_exchange, reference_symbol);
CREATE INDEX IF NOT EXISTS ix_logic_registry_last_seen ON mm.logic_registry (last_seen_ts DESC);

-- ==============================================================================
-- TABLE: mm.logic_runs (KEEP EXISTING, but add indexes if needed)
-- ==============================================================================
-- History of individual logic runs (already exists)
-- This table tracks each execution attempt
-- ==============================================================================

-- Ensure table exists (don't drop, may have data)
-- Check if table exists, if so, alter it; if not, create it
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM information_schema.tables 
                   WHERE table_schema = 'mm' AND table_name = 'logic_runs') THEN
        CREATE TABLE mm.logic_runs (
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
    ELSE
        -- Add app_id column if it doesn't exist
        IF NOT EXISTS (SELECT FROM information_schema.columns 
                       WHERE table_schema = 'mm' AND table_name = 'logic_runs' 
                       AND column_name = 'app_id') THEN
            ALTER TABLE mm.logic_runs ADD COLUMN app_id VARCHAR(100);
        END IF;
        -- Add end_time column if it doesn't exist
        IF NOT EXISTS (SELECT FROM information_schema.columns 
                       WHERE table_schema = 'mm' AND table_name = 'logic_runs' 
                       AND column_name = 'end_time') THEN
            ALTER TABLE mm.logic_runs ADD COLUMN end_time TIMESTAMP;
        END IF;
    END IF;
END $$;

-- Indexes for logic_runs
CREATE INDEX IF NOT EXISTS ix_logic_runs_logic_id ON mm.logic_runs (logic_id, start_time DESC);
CREATE INDEX IF NOT EXISTS ix_logic_runs_app_id ON mm.logic_runs (app_id, start_time DESC);
CREATE INDEX IF NOT EXISTS ix_logic_runs_status ON mm.logic_runs (status, start_time DESC);
CREATE INDEX IF NOT EXISTS ix_logic_runs_start_time ON mm.logic_runs (start_time DESC);

-- ==============================================================================
-- VIEW: mm.logic_registry_current
-- ==============================================================================
-- Current state view: latest registration for each logic_id
-- ==============================================================================

CREATE OR REPLACE VIEW mm.logic_registry_current AS
SELECT DISTINCT ON (id)
    id,
    app_id,
    instance_id,
    is_active,
    strategy_type,
    source_exchange,
    source_symbol,
    reference_exchange,
    reference_symbol,
    first_registered_ts,
    last_registered_ts,
    last_seen_ts,
    last_run_ts,
    config_json,
    version
FROM mm.logic_registry
ORDER BY id, last_registered_ts DESC;

-- ==============================================================================
-- VIEW: mm.app_registry_current
-- ==============================================================================
-- Current running app instances
-- ==============================================================================

CREATE OR REPLACE VIEW mm.app_registry_current AS
SELECT DISTINCT ON (app_id)
    app_id,
    instance_id,
    config_path,
    started_at,
    stopped_at,
    is_running,
    logics_count,
    active_logics_count
FROM mm.app_registry
WHERE is_running = true
ORDER BY app_id, started_at DESC;

-- ==============================================================================
-- VIEW: mm.logic_registry_with_app_status
-- ==============================================================================
-- Full view combining logic registry with app status
-- ==============================================================================

CREATE OR REPLACE VIEW mm.logic_registry_with_app_status AS
SELECT 
    lr.id as logic_id,
    lr.app_id,
    lr.instance_id,
    lr.is_active as logic_active,
    ar.is_running as app_running,
    lr.strategy_type,
    lr.source_exchange,
    lr.source_symbol,
    lr.reference_exchange,
    lr.reference_symbol,
    lr.first_registered_ts,
    lr.last_registered_ts,
    lr.last_seen_ts,
    lr.last_run_ts,
    ar.started_at as app_started_at,
    ar.stopped_at as app_stopped_at,
    ar.logics_count,
    ar.active_logics_count
FROM mm.logic_registry_current lr
LEFT JOIN mm.app_registry_current ar ON lr.app_id = ar.app_id;

-- ==============================================================================
-- COMMENTS
-- ==============================================================================

COMMENT ON TABLE mm.app_registry IS 'History of MM app instance startups and shutdowns';
COMMENT ON TABLE mm.logic_registry IS 'Current state of all registered logics (latest registration per logic_id)';
COMMENT ON TABLE mm.logic_runs IS 'History of individual logic execution attempts';
COMMENT ON VIEW mm.logic_registry_current IS 'Current state view: latest registration for each logic_id';
COMMENT ON VIEW mm.app_registry_current IS 'Currently running app instances';
COMMENT ON VIEW mm.logic_registry_with_app_status IS 'Full view: logic registry combined with app running status';
