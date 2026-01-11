-- =============================================================================
-- 06_stage__datacenters_operating.sql
-- Purpose:
--   (1) Filter FracTracker to Operating facilities only
--   (2) Materialize stage.data_centers_operating using:
--         - reported MW when present
--         - KNN-imputed MW when reported MW is missing (from stage tables below)
--   (3) Roll up state exposure into stage.dc_state_exposure
--
-- Notes for the notebook orchestration:
--   - First run: loads reported-MW operating facilities immediately.
--   - After you run Python KNN and INSERT results into stage.dc_mw_imputation_result
--     and mark a run as is_current=TRUE, re-run this script to fully populate MW.
-- =============================================================================

BEGIN;


-- -------------------------------------------------------------------------
-- Imputation audit tables (clean provenance for KNN results)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.dc_mw_imputation_run (
  run_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  algorithm   TEXT NOT NULL,                    -- e.g., 'KNNRegressor'
  params      JSONB NOT NULL DEFAULT '{}'::jsonb, -- e.g., {"k": 7, "weights":"distance", "features":[...]}
  is_current  BOOLEAN NOT NULL DEFAULT FALSE,
  notes       TEXT
);

-- Enforce at most one "current" run
CREATE UNIQUE INDEX IF NOT EXISTS ux_dc_mw_imputation_run_current
ON stage.dc_mw_imputation_run ((is_current))
WHERE is_current;

CREATE TABLE IF NOT EXISTS stage.dc_mw_imputation_result (
  run_id        UUID NOT NULL REFERENCES stage.dc_mw_imputation_run(run_id) ON DELETE CASCADE,
  facility_id   TEXT NOT NULL,
  mw_imputed    NUMERIC NOT NULL CHECK (mw_imputed >= 0),
  -- optional diagnostics: neighbors, distances, etc. (produced by Python, stored for transparency)
  diagnostics   JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (run_id, facility_id)
);

CREATE INDEX IF NOT EXISTS ix_dc_mw_imputation_result_facility
ON stage.dc_mw_imputation_result (facility_id);

-- -------------------------------------------------------------------------
-- 1) Load/update stage.data_centers_operating for Operating facilities
--    We do this in two passes:
--      A) reported MW rows (can always load)
--      B) missing MW rows (only load once current imputation exists)
-- -------------------------------------------------------------------------

-- A) Upsert Operating facilities that have reported MW
INSERT INTO stage.data_centers_operating (
  facility_id, name, state_code, county, city, lat, long,
  status, status_detail,
  facility_size_sqft, sizerank, sizerank_numeric,
  mw_reported, mw_imputed, mw_final, mw_source,
  imputation_model, imputation_notes, updated_at
)
SELECT
  NULLIF(TRIM(facility_id), '')                                            AS facility_id,
  NULLIF(TRIM(name), '')                                                   AS name,
  UPPER(TRIM(state))::CHAR(2)                                              AS state_code,
  NULLIF(TRIM(county), '')                                                 AS county,
  NULLIF(TRIM(city), '')                                                   AS city,
  lat                                                                      AS lat,
  long                                                                     AS long,
  NULLIF(TRIM(status), '')                                                 AS status,
  NULLIF(TRIM(status_detail), '')                                          AS status_detail,
  facility_size_sqft                                                       AS facility_size_sqft,
  NULLIF(TRIM(sizerank), '')                                               AS sizerank,
  sizerank_numeric                                                         AS sizerank_numeric,
  mw_high                                                                  AS mw_reported,
  NULL                                                                     AS mw_imputed,
  mw_high                                                                  AS mw_final,
  'reported'                                                               AS mw_source,
  NULL                                                                     AS imputation_model,
  NULL                                                                     AS imputation_notes,
  now()                                                                    AS updated_at
FROM raw.fractracker_datacenters
WHERE LOWER(TRIM(status)) = 'operating'
  AND mw_high IS NOT NULL
  AND TRIM(state) ~ '^[A-Za-z]{2}$'
  AND NULLIF(TRIM(facility_id), '') IS NOT NULL
ON CONFLICT (facility_id) DO UPDATE
SET
  name              = EXCLUDED.name,
  state_code         = EXCLUDED.state_code,
  county             = EXCLUDED.county,
  city               = EXCLUDED.city,
  lat                = EXCLUDED.lat,
  long               = EXCLUDED.long,
  status             = EXCLUDED.status,
  status_detail      = EXCLUDED.status_detail,
  facility_size_sqft = EXCLUDED.facility_size_sqft,
  sizerank           = EXCLUDED.sizerank,
  sizerank_numeric   = EXCLUDED.sizerank_numeric,
  mw_reported        = EXCLUDED.mw_reported,
  -- keep any existing imputed fields, but ensure reported wins for final:
  mw_final           = EXCLUDED.mw_final,
  mw_source          = 'reported',
  imputation_model   = NULL,
  imputation_notes   = NULL,
  updated_at         = now();

-- B) Upsert Operating facilities missing MW, but ONLY if a "current" imputation run exists
WITH current_run AS (
  SELECT run_id, algorithm, params
  FROM stage.dc_mw_imputation_run
  WHERE is_current = TRUE
  LIMIT 1
)
INSERT INTO stage.data_centers_operating (
  facility_id, name, state_code, county, city, lat, long,
  status, status_detail,
  facility_size_sqft, sizerank, sizerank_numeric,
  mw_reported, mw_imputed, mw_final, mw_source,
  imputation_model, imputation_notes, updated_at
)
SELECT
  NULLIF(TRIM(r.facility_id), '')                                          AS facility_id,
  NULLIF(TRIM(r.name), '')                                                 AS name,
  UPPER(TRIM(r.state))::CHAR(2)                                            AS state_code,
  NULLIF(TRIM(r.county), '')                                               AS county,
  NULLIF(TRIM(r.city), '')                                                 AS city,
  r.lat                                                                    AS lat,
  r.long                                                                   AS long,
  NULLIF(TRIM(r.status), '')                                               AS status,
  NULLIF(TRIM(r.status_detail), '')                                        AS status_detail,
  r.facility_size_sqft                                                     AS facility_size_sqft,
  NULLIF(TRIM(r.sizerank), '')                                             AS sizerank,
  r.sizerank_numeric                                                       AS sizerank_numeric,
  NULL                                                                     AS mw_reported,
  i.mw_imputed                                                             AS mw_imputed,
  i.mw_imputed                                                             AS mw_final,
  'imputed'                                                                AS mw_source,
  (cr.algorithm || '(' || COALESCE(cr.params->>'k','?') || ')')             AS imputation_model,
  'MW imputed from facility attributes; diagnostics stored in stage.dc_mw_imputation_result' AS imputation_notes,
  now()                                                                    AS updated_at
FROM raw.fractracker_datacenters r
JOIN current_run cr ON TRUE
JOIN stage.dc_mw_imputation_result i
  ON i.run_id = cr.run_id
 AND i.facility_id = NULLIF(TRIM(r.facility_id), '')
WHERE LOWER(TRIM(r.status)) = 'operating'
  AND r.mw_high IS NULL
  AND TRIM(r.state) ~ '^[A-Za-z]{2}$'
  AND NULLIF(TRIM(r.facility_id), '') IS NOT NULL
ON CONFLICT (facility_id) DO UPDATE
SET
  name              = EXCLUDED.name,
  state_code         = EXCLUDED.state_code,
  county             = EXCLUDED.county,
  city               = EXCLUDED.city,
  lat                = EXCLUDED.lat,
  long               = EXCLUDED.long,
  status             = EXCLUDED.status,
  status_detail      = EXCLUDED.status_detail,
  facility_size_sqft = EXCLUDED.facility_size_sqft,
  sizerank           = EXCLUDED.sizerank,
  sizerank_numeric   = EXCLUDED.sizerank_numeric,
  mw_reported        = NULL,
  mw_imputed         = EXCLUDED.mw_imputed,
  mw_final           = EXCLUDED.mw_final,
  mw_source          = 'imputed',
  imputation_model   = EXCLUDED.imputation_model,
  imputation_notes   = EXCLUDED.imputation_notes,
  updated_at         = now()
WHERE stage.data_centers_operating.mw_source <> 'reported'; -- never overwrite reported finals

-- -------------------------------------------------------------------------
-- 2) Rebuild stage.dc_state_exposure from stage.data_centers_operating
--    (Static exposure by state; used later for analysis joins)
-- -------------------------------------------------------------------------
TRUNCATE TABLE stage.dc_state_exposure;

INSERT INTO stage.dc_state_exposure (
  state_code, dc_count, total_mw, total_sqft, avg_mw, mw_per_100k_sqft, updated_at
)
SELECT
  state_code,
  COUNT(*)::INTEGER                                     AS dc_count,
  COALESCE(SUM(mw_final), 0)                             AS total_mw,
  SUM(facility_size_sqft)                               AS total_sqft,
  AVG(mw_final)                                         AS avg_mw,
  CASE
    WHEN SUM(facility_size_sqft) IS NULL OR SUM(facility_size_sqft) = 0 THEN NULL
    ELSE (SUM(mw_final) / SUM(facility_size_sqft)) * 100000
  END                                                   AS mw_per_100k_sqft,
  now()                                                 AS updated_at
FROM stage.data_centers_operating
GROUP BY state_code;

COMMIT;
