-- Add price columns to ob_gain_per_logic for dashboard fast path.
-- Dashboard can read pre-aggregated price + gain time series instead of raw OBs.

ALTER TABLE market_data.ob_gain_per_logic
    ADD COLUMN IF NOT EXISTS price_src DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS price_ref DOUBLE PRECISION;
