-- =============================================================================
-- 10_mart__load_facts.sql
-- Purpose: Populate mart facts (idempotent upserts)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- Facts
CREATE TABLE IF NOT EXISTS mart.fact_retail_price (
  state_sk        BIGINT NOT NULL,
  month_sk        BIGINT NOT NULL,
  sector_sk       BIGINT NOT NULL,
  customers       NUMERIC,
  price_cents_kwh NUMERIC,
  revenue_musd    NUMERIC,
  sales_mkwh      NUMERIC,
  PRIMARY KEY (state_sk, month_sk, sector_sk)
);

CREATE TABLE IF NOT EXISTS mart.fact_dc_state (
  state_sk         BIGINT PRIMARY KEY,
  dc_count         INTEGER,
  total_mw         NUMERIC,
  total_sqft       NUMERIC,
  avg_mw           NUMERIC,
  mw_per_100k_sqft NUMERIC,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mart.fact_ops_fuel (
  state_sk        BIGINT NOT NULL,
  month_sk        BIGINT NOT NULL,
  fuel_sk         BIGINT NOT NULL,
  generation_mwh  NUMERIC,
  fuel_cost_proxy NUMERIC,
  PRIMARY KEY (state_sk, month_sk, fuel_sk)
);

BEGIN;
-- fact_retail_price
INSERT INTO mart.fact_retail_price (
  state_sk, month_sk, sector_sk,
  customers, price_cents_kwh, revenue_musd, sales_mkwh
)
SELECT
  ds.state_sk,
  dm.month_sk,
  dsec.sector_sk,
  r.customers,
  r.price_cents_kwh,
  r.revenue_musd,
  r.sales_mkwh
FROM stage.retail_state_month_sector r
JOIN mart.dim_state ds
  ON ds.state_code = r.state_code
JOIN mart.dim_month dm
  ON dm.month_date = r.month_date
JOIN mart.dim_retail_sector dsec
  ON dsec.sector_code = r.sector_code
ON CONFLICT (state_sk, month_sk, sector_sk) DO UPDATE
SET
  customers       = EXCLUDED.customers,
  price_cents_kwh = EXCLUDED.price_cents_kwh,
  revenue_musd    = EXCLUDED.revenue_musd,
  sales_mkwh      = EXCLUDED.sales_mkwh;

-- fact_dc_state (static by state)
INSERT INTO mart.fact_dc_state (
  state_sk, dc_count, total_mw, total_sqft, avg_mw, mw_per_100k_sqft, updated_at
)
SELECT
  ds.state_sk,
  e.dc_count,
  e.total_mw,
  e.total_sqft,
  e.avg_mw,
  e.mw_per_100k_sqft,
  now()
FROM stage.dc_state_exposure e
JOIN mart.dim_state ds
  ON ds.state_code = e.state_code
ON CONFLICT (state_sk) DO UPDATE
SET
  dc_count         = EXCLUDED.dc_count,
  total_mw         = EXCLUDED.total_mw,
  total_sqft       = EXCLUDED.total_sqft,
  avg_mw           = EXCLUDED.avg_mw,
  mw_per_100k_sqft = EXCLUDED.mw_per_100k_sqft,
  updated_at       = now();

-- fact_ops_fuel (unpivot from controls into small fuel dimension set)
WITH u AS (
  SELECT state_code, month_date, 'TOTAL'::TEXT AS fuel_code, generation_mwh_total AS generation_mwh, fuel_cost_proxy
  FROM stage.ops_state_month_controls
  UNION ALL
  SELECT state_code, month_date, 'COAL', generation_mwh_coal, NULL
  FROM stage.ops_state_month_controls
  UNION ALL
  SELECT state_code, month_date, 'GAS', generation_mwh_gas, NULL
  FROM stage.ops_state_month_controls
  UNION ALL
  SELECT state_code, month_date, 'NUCLEAR', generation_mwh_nuclear, NULL
  FROM stage.ops_state_month_controls
  UNION ALL
  SELECT state_code, month_date, 'RENEW', generation_mwh_renew, NULL
  FROM stage.ops_state_month_controls
)
INSERT INTO mart.fact_ops_fuel (state_sk, month_sk, fuel_sk, generation_mwh, fuel_cost_proxy)
SELECT
  ds.state_sk,
  dm.month_sk,
  df.fuel_sk,
  u.generation_mwh,
  u.fuel_cost_proxy
FROM u
JOIN mart.dim_state ds
  ON ds.state_code = u.state_code
JOIN mart.dim_month dm
  ON dm.month_date = u.month_date
JOIN mart.dim_fuel df
  ON df.fuel_code = u.fuel_code
ON CONFLICT (state_sk, month_sk, fuel_sk) DO UPDATE
SET
  generation_mwh  = EXCLUDED.generation_mwh,
  fuel_cost_proxy = EXCLUDED.fuel_cost_proxy;

COMMIT;
