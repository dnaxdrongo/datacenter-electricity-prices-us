-- =============================================================================
-- 11_mart__mv_analysis_refresh.sql
-- Purpose:
--   Create/refresh mart.mv_analysis_state_month for Python modeling
--   (state-month grain, RES outcome, DC exposure, ops controls)
-- =============================================================================

BEGIN;

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_analysis_state_month AS
SELECT
  s.state_code,
  m.month_date,
  m.ym,

  -- Outcome (RES)
  fr.price_cents_kwh AS res_price_cents_kwh,
  fr.sales_mkwh      AS res_sales_mkwh,
  fr.customers       AS res_customers,

  -- Exposure (static by state)
  dc.dc_count,
  dc.total_mw,
  dc.total_sqft,
  dc.avg_mw,
  dc.mw_per_100k_sqft,

  -- Controls (state-month)
  c.generation_mwh_total,
  c.share_coal,
  c.share_gas,
  c.share_nuclear,
  c.share_renew,
  c.fuel_cost_proxy,

  -- Helpful analysis-ready derived fields (still “data prep”, not modeling):
  CASE WHEN fr.sales_mkwh IS NULL OR fr.sales_mkwh = 0 THEN NULL
       ELSE (dc.total_mw / fr.sales_mkwh) END          AS dc_mw_per_res_mkwh,
  LN(1 + COALESCE(dc.total_mw, 0))                      AS ln_total_mw

FROM mart.dim_state s
JOIN mart.dim_month m ON 1=1
LEFT JOIN mart.fact_dc_state dc
  ON dc.state_sk = s.state_sk
LEFT JOIN mart.fact_retail_price fr
  ON fr.state_sk = s.state_sk
 AND fr.month_sk = m.month_sk
LEFT JOIN mart.dim_retail_sector rs
  ON rs.sector_sk = fr.sector_sk
LEFT JOIN stage.ops_state_month_controls c
  ON c.state_code = s.state_code
 AND c.month_date = m.month_date
WHERE rs.sector_code = 'RES';

-- Refresh each run so the notebook always sees latest staged data
REFRESH MATERIALIZED VIEW mart.mv_analysis_state_month;

-- Index for fast pulls
CREATE INDEX IF NOT EXISTS ix_mv_analysis_state_month
ON mart.mv_analysis_state_month (state_code, month_date);

COMMIT;
