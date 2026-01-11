-- =============================================================================
-- 08_stage__ops_fuel_and_controls.sql
-- Purpose:
--   (1) Load/refresh stage.ops_state_month_fuel from raw.eia_power_ops
--   (2) Compute stage.ops_state_month_controls:
--        - total generation (MWh)
--        - generation by major fuel groups (coal/gas/nuclear/renew)
--        - shares of total generation
--        - fuel_cost_proxy: documented proxy combining fossil cost z-scores and shares
-- =============================================================================

BEGIN;

-- -------------------------------------------------------------------------
-- 1) stage.ops_state_month_fuel
-- -------------------------------------------------------------------------
INSERT INTO stage.ops_state_month_fuel (
  state_code, month_date, ops_sector_id, fuel_code, fuel_name,
  generation_thousand_mwh, cost, cost_units,
  consumption_for_eg_btu, total_consumption_btu
)
SELECT
  UPPER(TRIM(location))::CHAR(2)                                      AS state_code,
  TO_DATE(TRIM(period) || '-01', 'YYYY-MM-DD')                        AS month_date,
  NULLIF(TRIM(sectorid), '')                                          AS ops_sector_id,
  NULLIF(TRIM(fueltypeid), '')                                        AS fuel_code,
  NULLIF(TRIM(fueltypedescription), '')                               AS fuel_name,
  generation                                                          AS generation_thousand_mwh,
  cost                                                                AS cost,
  NULLIF(TRIM(cost_units), '')                                        AS cost_units,
  consumption_for_eg_btu                                              AS consumption_for_eg_btu,
  total_consumption_btu                                               AS total_consumption_btu
FROM raw.eia_power_ops
WHERE TRIM(location) ~ '^[A-Za-z]{2}$'
  AND TRIM(period) ~ '^[0-9]{4}-[0-9]{2}$'
  AND NULLIF(TRIM(fueltypeid), '') IS NOT NULL
ON CONFLICT (state_code, month_date, ops_sector_id, fuel_code) DO UPDATE
SET
  fuel_name               = EXCLUDED.fuel_name,
  generation_thousand_mwh = EXCLUDED.generation_thousand_mwh,
  cost                    = EXCLUDED.cost,
  cost_units              = EXCLUDED.cost_units,
  consumption_for_eg_btu  = EXCLUDED.consumption_for_eg_btu,
  total_consumption_btu   = EXCLUDED.total_consumption_btu;

-- -------------------------------------------------------------------------
-- 2) stage.ops_state_month_controls
-- -------------------------------------------------------------------------

WITH base AS (
  SELECT
    state_code,
    month_date,
    ops_sector_id,
    fuel_code,
    COALESCE(fuel_name, '') AS fuel_name,
    (greatest(generation_thousand_mwh, 0) * 1000.0) AS generation_mwh,
    cost,
    cost_units
  FROM stage.ops_state_month_fuel
  WHERE ops_sector_id = '1'
),
labeled AS (
  SELECT
    *,
    CASE
      WHEN UPPER(fuel_code) = 'ALL' OR LOWER(fuel_name) LIKE 'all fuels%' THEN 'total'
      WHEN LOWER(fuel_name) LIKE '%coal%' THEN 'coal'
      WHEN LOWER(fuel_name) LIKE '%natural gas%' THEN 'gas'
      WHEN LOWER(fuel_name) LIKE '%nuclear%' THEN 'nuclear'
      WHEN LOWER(fuel_name) LIKE '%renewable%'
        OR LOWER(fuel_name) LIKE '%wind%'
        OR LOWER(fuel_name) LIKE '%solar%'
        OR LOWER(fuel_name) LIKE '%hydro%'
        OR LOWER(fuel_name) LIKE '%geothermal%'
        OR LOWER(fuel_name) LIKE '%biomass%'
      THEN 'renew'
      ELSE 'other'
    END AS fuel_group
  FROM base
),
best_in_group AS (
  SELECT DISTINCT ON (state_code, month_date, fuel_group)
    state_code,
    month_date,
    fuel_group,
    generation_mwh,
    cost,
    cost_units
  FROM labeled
  WHERE fuel_group IN ('total','coal','gas','nuclear','renew')
  ORDER BY state_code, month_date, fuel_group,
           generation_mwh DESC NULLS LAST,
           cost DESC NULLS LAST
),
pivoted AS (
  SELECT
    state_code,
    month_date,

    MAX(CASE WHEN fuel_group='total'   THEN generation_mwh END) AS generation_mwh_total_raw,
    MAX(CASE WHEN fuel_group='coal'    THEN generation_mwh END) AS generation_mwh_coal,
    MAX(CASE WHEN fuel_group='gas'     THEN generation_mwh END) AS generation_mwh_gas,
    MAX(CASE WHEN fuel_group='nuclear' THEN generation_mwh END) AS generation_mwh_nuclear,
    MAX(CASE WHEN fuel_group='renew'   THEN generation_mwh END) AS generation_mwh_renew,

    MAX(CASE WHEN fuel_group='coal' THEN cost END)              AS cost_coal,
    MAX(CASE WHEN fuel_group='gas'  THEN cost END)              AS cost_gas
  FROM best_in_group
  GROUP BY state_code, month_date
),
totals AS (
  SELECT
    state_code,
    month_date,
    GREATEST(
    COALESCE(generation_mwh_total_raw, 0),
    COALESCE(generation_mwh_coal,0) + COALESCE(generation_mwh_gas,0)
    + COALESCE(generation_mwh_nuclear,0) + COALESCE(generation_mwh_renew,0)
    ) AS generation_mwh_total,
    generation_mwh_coal,
    generation_mwh_gas,
    generation_mwh_nuclear,
    generation_mwh_renew,
    cost_coal,
    cost_gas
  FROM pivoted
),
shares AS (
  SELECT
    *,
    CASE WHEN generation_mwh_total IS NULL OR generation_mwh_total = 0 THEN NULL
         ELSE LEAST(1, GREATEST(0, generation_mwh_coal / NULLIF(generation_mwh_total,0))) END AS share_coal,
    CASE WHEN generation_mwh_total IS NULL OR generation_mwh_total = 0 THEN NULL
         ELSE LEAST(1, GREATEST(0, generation_mwh_gas / NULLIF(generation_mwh_total,0))) END AS share_gas,
    CASE WHEN generation_mwh_total IS NULL OR generation_mwh_total = 0 THEN NULL
         ELSE LEAST(1, GREATEST(0, generation_mwh_nuclear / NULLIF(generation_mwh_total,0))) END AS share_nuclear,
    CASE WHEN generation_mwh_total IS NULL OR generation_mwh_total = 0 THEN NULL
         ELSE LEAST(1, GREATEST(0, generation_mwh_renew / NULLIF(generation_mwh_total,0))) END AS share_renew
  FROM totals
),
zscored AS (
  SELECT
    *,
    (cost_gas  - AVG(cost_gas)  OVER()) / NULLIF(STDDEV_POP(cost_gas)  OVER(), 0) AS z_cost_gas,
    (cost_coal - AVG(cost_coal) OVER()) / NULLIF(STDDEV_POP(cost_coal) OVER(), 0) AS z_cost_coal
  FROM shares
)
INSERT INTO stage.ops_state_month_controls (
  state_code, month_date,
  generation_mwh_total,
  generation_mwh_coal,
  generation_mwh_gas,
  generation_mwh_nuclear,
  generation_mwh_renew,
  share_coal, share_gas, share_nuclear, share_renew,
  fuel_cost_proxy
)
SELECT
  state_code,
  month_date,
  generation_mwh_total,
  generation_mwh_coal,
  generation_mwh_gas,
  generation_mwh_nuclear,
  generation_mwh_renew,
  share_coal, share_gas, share_nuclear, share_renew,
  (
    COALESCE(share_gas, 0)  * COALESCE(z_cost_gas, 0)
    + COALESCE(share_coal, 0) * COALESCE(z_cost_coal, 0)
  ) AS fuel_cost_proxy
FROM zscored
ON CONFLICT (state_code, month_date) DO UPDATE
SET
  generation_mwh_total   = EXCLUDED.generation_mwh_total,
  generation_mwh_coal    = EXCLUDED.generation_mwh_coal,
  generation_mwh_gas     = EXCLUDED.generation_mwh_gas,
  generation_mwh_nuclear = EXCLUDED.generation_mwh_nuclear,
  generation_mwh_renew   = EXCLUDED.generation_mwh_renew,
  share_coal             = EXCLUDED.share_coal,
  share_gas              = EXCLUDED.share_gas,
  share_nuclear          = EXCLUDED.share_nuclear,
  share_renew            = EXCLUDED.share_renew,
  fuel_cost_proxy        = EXCLUDED.fuel_cost_proxy;

COMMIT;
