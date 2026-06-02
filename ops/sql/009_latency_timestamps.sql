-- Add latency timestamps to mm.signal and router.transaction
-- For measuring delays at each step of the trading flow

-- ============================================
-- MM.SIGNAL - timestamps from MM perspective
-- ============================================
DO $$
BEGIN
    -- ts_signal_created: when MM generated the signal (opportunity detected)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ts_signal_created') THEN
        ALTER TABLE mm.signal ADD COLUMN ts_signal_created TIMESTAMPTZ;
        RAISE NOTICE 'Added ts_signal_created to mm.signal';
    END IF;
    
    -- ts_source_sent: when MM sent source order to router
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ts_source_sent') THEN
        ALTER TABLE mm.signal ADD COLUMN ts_source_sent TIMESTAMPTZ;
        RAISE NOTICE 'Added ts_source_sent to mm.signal';
    END IF;
    
    -- ts_source_ack: when MM received ACK (orderId) from router for source
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ts_source_ack') THEN
        ALTER TABLE mm.signal ADD COLUMN ts_source_ack TIMESTAMPTZ;
        RAISE NOTICE 'Added ts_source_ack to mm.signal';
    END IF;
    
    -- ts_hedge_sent: when MM sent hedge order to router
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ts_hedge_sent') THEN
        ALTER TABLE mm.signal ADD COLUMN ts_hedge_sent TIMESTAMPTZ;
        RAISE NOTICE 'Added ts_hedge_sent to mm.signal';
    END IF;
    
    -- ts_hedge_ack: when MM received ACK (orderId) from router for hedge
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'mm' AND table_name = 'signal' 
                   AND column_name = 'ts_hedge_ack') THEN
        ALTER TABLE mm.signal ADD COLUMN ts_hedge_ack TIMESTAMPTZ;
        RAISE NOTICE 'Added ts_hedge_ack to mm.signal';
    END IF;
    
    RAISE NOTICE 'mm.signal latency columns added';
END $$;

-- ============================================
-- ROUTER.TRANSACTION - timestamps from Router perspective
-- ============================================
DO $$
BEGIN
    -- ts_signal_received: when router received signal from MM
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'router' AND table_name = 'transaction' 
                   AND column_name = 'ts_signal_received') THEN
        ALTER TABLE router.transaction ADD COLUMN ts_signal_received TIMESTAMPTZ;
        RAISE NOTICE 'Added ts_signal_received to router.transaction';
    END IF;
    
    -- ts_order_sent: when router sent order to exchange API
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'router' AND table_name = 'transaction' 
                   AND column_name = 'ts_order_sent') THEN
        ALTER TABLE router.transaction ADD COLUMN ts_order_sent TIMESTAMPTZ;
        RAISE NOTICE 'Added ts_order_sent to router.transaction';
    END IF;
    
    -- ts_exchange_ack: when exchange returned ACK (orderId)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'router' AND table_name = 'transaction' 
                   AND column_name = 'ts_exchange_ack') THEN
        ALTER TABLE router.transaction ADD COLUMN ts_exchange_ack TIMESTAMPTZ;
        RAISE NOTICE 'Added ts_exchange_ack to router.transaction';
    END IF;
    
    -- ts_filled: when exchange confirmed fill
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'router' AND table_name = 'transaction' 
                   AND column_name = 'ts_filled') THEN
        ALTER TABLE router.transaction ADD COLUMN ts_filled TIMESTAMPTZ;
        RAISE NOTICE 'Added ts_filled to router.transaction';
    END IF;
    
    RAISE NOTICE 'router.transaction latency columns added';
END $$;

-- ============================================
-- VIEW: Latency analysis
-- ============================================
-- Drop view if exists (will recreate after columns are added)
DROP VIEW IF EXISTS router.latency_analysis;

-- Recreate view after ensuring all columns exist
CREATE OR REPLACE VIEW router.latency_analysis AS
SELECT 
    COALESCE(s.cycle_id, 'UNKNOWN') as cycle_id,
    s.logic_id,
    s.source_exchange,
    s.source_symbol,
    s.ref_exchange,
    COALESCE(s.status, 'SIGNAL') as status,
    
    -- MM-side latencies (in milliseconds)
    EXTRACT(EPOCH FROM (s.ts_source_sent - s.ts_signal_created)) * 1000 as mm_prep_ms,
    EXTRACT(EPOCH FROM (s.ts_source_ack - s.ts_source_sent)) * 1000 as source_roundtrip_ms,
    EXTRACT(EPOCH FROM (s.ts_hedge_sent - s.ts_source_ack)) * 1000 as source_to_hedge_ms,
    EXTRACT(EPOCH FROM (s.ts_hedge_ack - s.ts_hedge_sent)) * 1000 as hedge_roundtrip_ms,
    EXTRACT(EPOCH FROM (s.ts_hedge_ack - s.ts_signal_created)) * 1000 as total_mm_ms,
    
    -- Router-side latencies for SOURCE order
    EXTRACT(EPOCH FROM (src_tx.ts_order_sent - src_tx.ts_signal_received)) * 1000 as src_router_prep_ms,
    EXTRACT(EPOCH FROM (src_tx.ts_exchange_ack - src_tx.ts_order_sent)) * 1000 as src_exchange_ack_ms,
    EXTRACT(EPOCH FROM (src_tx.ts_filled - src_tx.ts_exchange_ack)) * 1000 as src_fill_wait_ms,
    EXTRACT(EPOCH FROM (src_tx.ts_filled - src_tx.ts_signal_received)) * 1000 as src_total_router_ms,
    
    -- Router-side latencies for HEDGE order
    EXTRACT(EPOCH FROM (ref_tx.ts_order_sent - ref_tx.ts_signal_received)) * 1000 as ref_router_prep_ms,
    EXTRACT(EPOCH FROM (ref_tx.ts_exchange_ack - ref_tx.ts_order_sent)) * 1000 as ref_exchange_ack_ms,
    EXTRACT(EPOCH FROM (ref_tx.ts_filled - ref_tx.ts_exchange_ack)) * 1000 as ref_fill_wait_ms,
    EXTRACT(EPOCH FROM (ref_tx.ts_filled - ref_tx.ts_signal_received)) * 1000 as ref_total_router_ms,
    
    -- End-to-end latency
    EXTRACT(EPOCH FROM (GREATEST(src_tx.ts_filled, ref_tx.ts_filled) - s.ts_signal_created)) * 1000 as e2e_total_ms,
    
    s.ts_signal_created

FROM mm.signal s
LEFT JOIN router.transaction src_tx 
    ON COALESCE(src_tx.cycle_id, '') = COALESCE(s.cycle_id, '') 
    AND src_tx.exchange = s.source_exchange
LEFT JOIN router.transaction ref_tx 
    ON COALESCE(ref_tx.cycle_id, '') = COALESCE(s.cycle_id, '') 
    AND ref_tx.exchange = s.ref_exchange
WHERE s.ts_signal_created IS NOT NULL
ORDER BY s.ts_signal_created DESC;

-- ============================================
-- Query: Recent latency stats
-- ============================================
-- SELECT * FROM router.latency_analysis WHERE ts_signal_created > NOW() - INTERVAL '1 hour' LIMIT 20;

-- Aggregate stats:
-- SELECT 
--     logic_id,
--     COUNT(*) as trades,
--     ROUND(AVG(total_mm_ms)::numeric, 1) as avg_mm_ms,
--     ROUND(AVG(e2e_total_ms)::numeric, 1) as avg_e2e_ms,
--     ROUND(MAX(e2e_total_ms)::numeric, 1) as max_e2e_ms
-- FROM router.latency_analysis
-- WHERE ts_signal_created > NOW() - INTERVAL '4 hours'
-- GROUP BY logic_id;
