-- Staging model for rolled gains exported to BigQuery via GCS external table.
-- Run after loading Parquet from GCS (see README DE section).

{{ config(materialized='view') }}

SELECT
  ts,
  symbol,
  src_exchange,
  ref_exchange,
  side,
  target_volume_quote,
  avg_gain_pct_30s,
  observations_count
FROM {{ source('market_data', 'ob_arb_rolling_gain_30s') }}
