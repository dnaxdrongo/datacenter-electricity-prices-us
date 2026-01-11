-- Retail outcomes: fact at state-month-sector grain
CREATE TABLE IF NOT EXISTS mart.fact_retail_price (
  state_sk        SMALLINT NOT NULL REFERENCES mart.dim_state(state_sk),
  month_sk        SMALLINT NOT NULL REFERENCES mart.dim_month(month_sk),
  sector_sk       SMALLINT NOT NULL REFERENCES mart.dim_retail_sector(sector_sk),

  customers       NUMERIC,
  price_cents_kwh NUMERIC,
  revenue_musd    NUMERIC,
  sales_mkwh      NUMERIC,

  PRIMARY KEY (state_sk, month_sk, sector_sk)
);

CREATE INDEX IF NOT EXISTS ix_fact_retail_state_month ON mart.fact_retail_price(state_sk, month_sk);


-- Fuel-level operational data (useful for auditing + optional richer models)
CREATE TABLE IF NOT EXISTS mart.fact_ops_fuel (
  state_sk          SMALLINT NOT NULL REFERENCES mart.dim_state(state_sk),
  month_sk          SMALLINT NOT NULL REFERENCES mart.dim_month(month_sk),
  fuel_sk           SMALLINT NOT NULL REFERENCES mart.dim_fuel(fuel_sk),

  generation_mwh    NUMERIC,
  fuel_cost_proxy   NUMERIC,

  PRIMARY KEY (state_sk, month_sk, fuel_sk)
);

CREATE INDEX IF NOT EXISTS ix_fact_ops_state_month ON mart.fact_ops_fuel(state_sk, month_sk);


-- Data center exposure: state grain (static snapshot, joined to months in analysis mart)
CREATE TABLE IF NOT EXISTS mart.fact_dc_state (
  state_sk        SMALLINT PRIMARY KEY REFERENCES mart.dim_state(state_sk),
  dc_count        INTEGER NOT NULL,
  total_mw        NUMERIC NOT NULL,
  total_sqft      NUMERIC,
  avg_mw          NUMERIC,
  mw_per_100k_sqft NUMERIC,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
