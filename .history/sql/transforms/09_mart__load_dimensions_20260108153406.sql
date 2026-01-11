-- =============================================================================
-- 09_mart__load_dimensions.sql
-- Purpose: Populate mart dimensions (idempotent upserts)
-- =============================================================================

BEGIN;

-- dim_state
WITH states AS (
  SELECT DISTINCT state_code FROM stage.retail_state_month_sector
  UNION
  SELECT DISTINCT state_code FROM stage.ops_state_month_controls
  UNION
  SELECT DISTINCT state_code FROM stage.dc_state_exposure
),
names AS (
  SELECT UPPER(TRIM(stateid))::CHAR(2) AS state_code,
         MAX(statedescription)         AS state_name
  FROM raw.eia_retail_sales
  WHERE TRIM(stateid) ~ '^[A-Za-z]{2}$'
  GROUP BY 1
)
INSERT INTO mart.dim_state (state_code, state_name)
SELECT s.state_code, n.state_name
FROM states s
LEFT JOIN names n USING (state_code)
ON CONFLICT (state_code) DO UPDATE
SET state_name = COALESCE(EXCLUDED.state_name, mart.dim_state.state_name);

-- dim_month (generate full month series between observed min/max)
WITH bounds AS (
  SELECT
    DATE_TRUNC('month', MIN(month_date))::DATE AS min_m,
    DATE_TRUNC('month', MAX(month_date))::DATE AS max_m
  FROM stage.retail_state_month_sector
),
months AS (
  SELECT generate_series(min_m, max_m, interval '1 month')::DATE AS month_date
  FROM bounds
  WHERE min_m IS NOT NULL AND max_m IS NOT NULL
)
INSERT INTO mart.dim_month (month_date, year, month, ym)
SELECT
  month_date,
  EXTRACT(YEAR FROM month_date)::SMALLINT  AS year,
  EXTRACT(MONTH FROM month_date)::SMALLINT AS month,
  TO_CHAR(month_date, 'YYYY-MM')           AS ym
FROM months
ON CONFLICT (month_date) DO UPDATE
SET
  year  = EXCLUDED.year,
  month = EXCLUDED.month,
  ym    = EXCLUDED.ym;

-- dim_retail_sector
INSERT INTO mart.dim_retail_sector (sector_code, sector_name)
SELECT DISTINCT
  UPPER(TRIM(sectorid)) AS sector_code,
  MAX(sectorname) OVER (PARTITION BY UPPER(TRIM(sectorid))) AS sector_name
FROM raw.eia_retail_sales
WHERE NULLIF(TRIM(sectorid), '') IS NOT NULL
ON CONFLICT (sector_code) DO UPDATE
SET sector_name = COALESCE(EXCLUDED.sector_name, mart.dim_retail_sector.sector_name);

-- dim_fuel (small, analysis-friendly set)
INSERT INTO mart.dim_fuel (fuel_code, fuel_name, fuel_group)
VALUES
  ('TOTAL',   'Total generation',   'total'),
  ('COAL',    'Coal generation',    'coal'),
  ('GAS',     'Natural gas generation', 'gas'),
  ('NUCLEAR', 'Nuclear generation', 'nuclear'),
  ('RENEW',   'Renewable generation','renew')
ON CONFLICT (fuel_code) DO UPDATE
SET
  fuel_name  = EXCLUDED.fuel_name,
  fuel_group = EXCLUDED.fuel_group;

COMMIT;
