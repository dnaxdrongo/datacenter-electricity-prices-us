CREATE TABLE IF NOT EXISTS mart.dim_state (
  state_sk      SMALLSERIAL PRIMARY KEY,
  state_code    CHAR(2) NOT NULL UNIQUE,
  state_name    TEXT
);

CREATE TABLE IF NOT EXISTS mart.dim_month (
  month_sk      SMALLSERIAL PRIMARY KEY,
  month_date    DATE NOT NULL UNIQUE,
  year          SMALLINT NOT NULL,
  month         SMALLINT NOT NULL,
  ym            TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS mart.dim_retail_sector (
  sector_sk     SMALLSERIAL PRIMARY KEY,
  sector_code   TEXT NOT NULL UNIQUE,
  sector_name   TEXT
);

CREATE TABLE IF NOT EXISTS mart.dim_fuel (
  fuel_sk       SMALLSERIAL PRIMARY KEY,
  fuel_code     TEXT NOT NULL UNIQUE,
  fuel_name     TEXT,
  fuel_group    TEXT    -- coal/gas/nuclear/renew/other
);
