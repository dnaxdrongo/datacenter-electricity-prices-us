-- Canonical month key helper (store 'YYYY-MM' as DATE = first day of month)
-- You can generate month_date in Python before load OR in SQL transforms.

CREATE TABLE IF NOT EXISTS stage.data_centers_operating (
  facility_id        TEXT PRIMARY KEY,
  name               TEXT,
  state_code         CHAR(2) NOT NULL,
  county             TEXT,
  city               TEXT,
  lat                DOUBLE PRECISION,
  long               DOUBLE PRECISION,

  status             TEXT,
  status_detail      TEXT,

  facility_size_sqft NUMERIC,
  sizerank           TEXT,
  sizerank_numeric   INTEGER,

  mw_reported        NUMERIC,
  mw_imputed         NUMERIC,
  mw_final           NUMERIC NOT NULL,
  mw_source          TEXT NOT NULL CHECK (mw_source IN ('reported','imputed')),

  imputation_model   TEXT,          -- e.g., 'KNN(k=5)'
  imputation_notes   TEXT,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_stage_dc_state ON stage.data_centers_operating(state_code);


CREATE TABLE IF NOT EXISTS stage.dc_state_exposure (
  state_code         CHAR(2) PRIMARY KEY,
  dc_count           INTEGER NOT NULL,
  total_mw           NUMERIC NOT NULL,
  total_sqft         NUMERIC,
  avg_mw             NUMERIC,
  mw_per_100k_sqft   NUMERIC,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS stage.retail_state_month_sector (
  state_code         CHAR(2) NOT NULL,
  month_date         DATE NOT NULL,            -- e.g., 2025-10-01
  sector_code        TEXT NOT NULL,            -- RES, COM, etc.

  customers          NUMERIC,
  price_cents_kwh    NUMERIC,
  revenue_musd       NUMERIC,
  sales_mkwh         NUMERIC,

  PRIMARY KEY (state_code, month_date, sector_code)
);

CREATE INDEX IF NOT EXISTS ix_stage_retail_month ON stage.retail_state_month_sector(month_date);


CREATE TABLE IF NOT EXISTS stage.ops_state_month_fuel (
  state_code              CHAR(2) NOT NULL,
  month_date              DATE NOT NULL,
  ops_sector_id           TEXT,
  fuel_code               TEXT NOT NULL,
  fuel_name               TEXT,

  generation_thousand_mwh NUMERIC,
  cost                    NUMERIC,
  cost_units              TEXT,

  consumption_for_eg_btu   NUMERIC,
  total_consumption_btu    NUMERIC,

  PRIMARY KEY (state_code, month_date, ops_sector_id, fuel_code)
);

CREATE INDEX IF NOT EXISTS ix_stage_ops_month ON stage.ops_state_month_fuel(month_date);


-- Aggregated supply-side controls (state-month grain)
CREATE TABLE IF NOT EXISTS stage.ops_state_month_controls (
  state_code        CHAR(2) NOT NULL,
  month_date        DATE NOT NULL,

  generation_mwh_total     NUMERIC,
  generation_mwh_coal      NUMERIC,
  generation_mwh_gas       NUMERIC,
  generation_mwh_nuclear   NUMERIC,
  generation_mwh_renew     NUMERIC,

  share_coal        NUMERIC,
  share_gas         NUMERIC,
  share_nuclear     NUMERIC,
  share_renew       NUMERIC,

  fuel_cost_proxy   NUMERIC,   -- documented proxy (see notes)
  PRIMARY KEY (state_code, month_date)
);
