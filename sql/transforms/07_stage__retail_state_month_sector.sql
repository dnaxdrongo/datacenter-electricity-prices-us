-- =============================================================================
-- 07_stage__retail_state_month_sector.sql
-- Purpose: Load/refresh stage.retail_state_month_sector from raw.eia_retail_sales
-- Grain: (state_code, month_date, sector_code)
-- =============================================================================

BEGIN;

INSERT INTO stage.retail_state_month_sector (
  state_code, month_date, sector_code,
  customers, price_cents_kwh, revenue_musd, sales_mkwh
)
SELECT
  UPPER(TRIM(stateid))::CHAR(2)                                        AS state_code,
  TO_DATE(TRIM(period) || '-01', 'YYYY-MM-DD')                         AS month_date,
  UPPER(TRIM(sectorid))                                                AS sector_code,
  customers                                                            AS customers,
  price                                                                AS price_cents_kwh,
  revenue                                                              AS revenue_musd,
  sales                                                                AS sales_mkwh
FROM raw.eia_retail_sales
WHERE TRIM(stateid) ~ '^[A-Za-z]{2}$'
  AND TRIM(period) ~ '^[0-9]{4}-[0-9]{2}$'
  AND NULLIF(TRIM(sectorid), '') IS NOT NULL
ON CONFLICT (state_code, month_date, sector_code) DO UPDATE
SET
  customers       = EXCLUDED.customers,
  price_cents_kwh = EXCLUDED.price_cents_kwh,
  revenue_musd    = EXCLUDED.revenue_musd,
  sales_mkwh      = EXCLUDED.sales_mkwh;

COMMIT;
