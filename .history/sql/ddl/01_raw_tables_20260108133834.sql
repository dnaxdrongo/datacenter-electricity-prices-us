-- =========================
-- RAW: FracTracker data centers (CSV snapshot)
-- =========================
CREATE TABLE IF NOT EXISTS raw.fractracker_datacenters (
  facility_id                TEXT,
  name                       TEXT,
  address                    TEXT,
  city                       TEXT,
  state                      TEXT,
  zip                        TEXT,
  location_confidence        TEXT,
  purpose                    TEXT,
  operator                   TEXT,
  tenant                     TEXT,
  location_determination     TEXT,
  info_source_1              TEXT,
  info_source_2              TEXT,
  info_source_3              TEXT,
  info_source_4              TEXT,
  info_source_5              TEXT,
  info_source_6              TEXT,
  info_source_7              TEXT,
  info_source_8              TEXT,
  lat                        DOUBLE PRECISION,
  long                       DOUBLE PRECISION,
  date_created               TEXT,
  date_updated               TEXT,
  mw_high                    NUMERIC,
  sizerank                   TEXT,
  sizerank_numeric           INTEGER,
  power_source               TEXT,
  dedicated_power_plant      TEXT,
  number_of_generators       TEXT,
  number_of_buildings        TEXT,
  cooling_source             TEXT,
  facility_size_sqft         NUMERIC,
  cooling_type               TEXT,
  property_size_acres        TEXT,
  project_cost               TEXT,
  other_info                 TEXT,
  status                     TEXT,
  status_detail              TEXT,
  expected_date_online       TEXT,
  county                     TEXT,

  -- ingestion metadata
  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_name                TEXT NOT NULL,
  source_ref                 TEXT NOT NULL,
  source_row_hash            TEXT
);

CREATE INDEX IF NOT EXISTS ix_raw_fractracker_state ON raw.fractracker_datacenters(state);
CREATE INDEX IF NOT EXISTS ix_raw_fractracker_status ON raw.fractracker_datacenters(status);


-- =========================
-- RAW: EIA retail sales (state-month-sector outcomes)
-- Source: EIA Open Data API v2 retail sales endpoint
-- =========================
CREATE TABLE IF NOT EXISTS raw.eia_retail_sales (
  period           TEXT,   -- 'YYYY-MM'
  stateid          TEXT,   -- 'VA'
  statedescription TEXT,
  sectorid         TEXT,   -- 'RES','COM',...
  sectorname       TEXT,

  customers        NUMERIC,
  price            NUMERIC,  -- cents/kWh
  revenue          NUMERIC,  -- million dollars
  sales            NUMERIC,  -- million kWh

  customers_units  TEXT,
  price_units      TEXT,
  revenue_units    TEXT,
  sales_units      TEXT,

  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_name      TEXT NOT NULL,
  source_ref       TEXT NOT NULL,
  source_row_hash  TEXT
);

CREATE INDEX IF NOT EXISTS ix_raw_retail_state_period ON raw.eia_retail_sales(stateid, period);
CREATE INDEX IF NOT EXISTS ix_raw_retail_sector ON raw.eia_retail_sales(sectorid);


-- =========================
-- RAW: EIA operational / generation (state-month-fuel controls)
-- Source: EIA Open Data API v2 electric power operational data
-- =========================
CREATE TABLE IF NOT EXISTS raw.eia_power_ops (
  period                     TEXT,  -- 'YYYY-MM'
  location                   TEXT,  -- state code
  statedescription           TEXT,
  sectorid                   TEXT,  -- often numeric-coded
  sectordescription          TEXT,
  fueltypeid                 TEXT,
  fueltypedescription        TEXT,

  consumption_for_eg         NUMERIC,
  consumption_for_eg_units   TEXT,
  consumption_for_eg_btu     NUMERIC,
  consumption_for_eg_btu_units TEXT,

  cost                       NUMERIC,
  cost_units                 TEXT,

  generation                 NUMERIC,
  generation_units           TEXT,

  total_consumption          NUMERIC,
  total_consumption_units    TEXT,
  total_consumption_btu      NUMERIC,
  total_consumption_btu_units TEXT,

  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_name                TEXT NOT NULL,
  source_ref                 TEXT NOT NULL,
  source_row_hash            TEXT
);

CREATE INDEX IF NOT EXISTS ix_raw_ops_state_period ON raw.eia_power_ops(location, period);
CREATE INDEX IF NOT EXISTS ix_raw_ops_fuel ON raw.eia_power_ops(fueltypeid);
