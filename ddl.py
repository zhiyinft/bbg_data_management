# -*- coding: utf-8 -*-
"""
DDL statements for NFT database
Creator: Zhiyi Lu
Create time: 2026-01-21
Description: All DDL statements for creating tables in the database
"""

# DDL: Create table est_cf_comp_ffo_per_share in bbg schema
# Based on est.is_eps table structure from local database
# Column 'is_eps' renamed to 'cf_comp_ffo_per_share'
# Field types optimized based on actual data usage

DDL_EST_CF_COMP_FFO_PER_SHARE = """
CREATE TABLE IF NOT EXISTS bbg.est_cf_comp_ffo_per_share (
    ticker VARCHAR(20) NOT NULL,           -- Stock ticker (e.g., 'AAPL')
    dt TIMESTAMP NOT NULL,                 -- Date/time
    fpt VARCHAR(10) NOT NULL,              -- Short code (e.g., 'Q', 'A', 'FY')
    fpo SMALLINT NOT NULL,                 -- Small integer (0-255)
    dollar VARCHAR(10) NOT NULL DEFAULT 'lc', -- Currency indicator ('lc', 'usd')
    ID VARCHAR(20),                        -- Short identifier
    REVISION_DATE TIMESTAMP,               -- Revision date
    SAMPLING_DATE TIMESTAMP,               -- Sampling date
    PERIOD_END_DATE TIMESTAMP,             -- Period end date
    PERIOD VARCHAR(20),                    -- Period code (e.g., '2024Q1')
    PERIOD_OFFSET VARCHAR(10),             -- Short offset code (e.g., '+0', '-1')
    ACT_EST_DATA VARCHAR(10),              -- Short code (e.g., 'ACTUAL', 'ESTIMATE')
    AS_OF_DATE TIMESTAMP,                  -- As-of date
    CURRENCY VARCHAR(20),                  -- Currency code
    cf_comp_ffo_per_share DOUBLE PRECISION, -- Cash flow/comprehensive FFO per share
    bql_fld_str TEXT,                      -- BQL field string (can be long)
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Last update time
    PRIMARY KEY (ticker, dt, fpt, fpo, dollar)
);
"""

# DDL: Create schema bbg (if not exists)
DDL_CREATE_BBG_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS bbg;
"""

# DDL: Create table info_tracking_names in bbg schema
# Based on info.tracking_names table structure from local database

DDL_INFO_TRACKING_NAMES = """
CREATE TABLE IF NOT EXISTS bbg.info_tracking_names (
    ticker              VARCHAR(64)      NOT NULL,    -- Stock ticker
    name                VARCHAR(255)     NULL,        -- Company name
    custom_sector       VARCHAR(255)     NOT NULL,    -- Custom sector classification
    market_cap          DOUBLE PRECISION NULL,        -- Market capitalization
    update_time         TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Last update time
    cluster             VARCHAR(32)      NOT NULL,    -- Cluster identifier
    watching            VARCHAR(32)      NULL,        -- Watching status
    vld                 BOOLEAN          NOT NULL DEFAULT true,  -- Valid flag
    exposure            DOUBLE PRECISION NULL,        -- Exposure
    PRIMARY KEY (cluster, custom_sector, ticker, vld)
);
"""

# DDL: Create table px in bbg schema
# Based on bbg.px table structure from local database
# Price data with original and adjusted values for open/high/low/close and volume

DDL_PX = """
CREATE TABLE IF NOT EXISTS bbg.px (
    ticker          VARCHAR(32)      NOT NULL,    -- Stock ticker
    dt              TIMESTAMP        NOT NULL,    -- Date/time
    orig_open       REAL             NULL,        -- Original open price
    orig_high       REAL             NULL,        -- Original high price
    orig_low        REAL             NULL,        -- Original low price
    orig_close      REAL             NULL,        -- Original close price
    orig_volume     BIGINT           NULL,        -- Original volume
    adj_fac_px      DOUBLE PRECISION NULL,        -- Adjustment factor for prices
    adj_fac_vol     DOUBLE PRECISION NULL,        -- Adjustment factor for volume
    update_time     TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Last update time
    PRIMARY KEY (ticker, dt)
);
"""


def get_all_ddls() -> dict:
    """Return all DDL statements as a dictionary"""
    return {
        "create_bbg_schema": DDL_CREATE_BBG_SCHEMA,
        "est_cf_comp_ffo_per_share": DDL_EST_CF_COMP_FFO_PER_SHARE,
        "info_tracking_names": DDL_INFO_TRACKING_NAMES,
        "px": DDL_PX,
    }


if __name__ == "__main__":
    # Print all DDL statements
    for name, ddl in get_all_ddls().items():
        print(f"=== {name} ===")
        print(ddl)
        print()
