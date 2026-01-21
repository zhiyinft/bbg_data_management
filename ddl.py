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
    ID VARCHAR(20),                        -- Short identifier
    PERIOD_OFFSET VARCHAR(10),             -- Short offset code (e.g., '+0', '-1')
    REVISION_DATE TIMESTAMP,               -- Revision date
    ACT_EST_DATA VARCHAR(10),              -- Short code (e.g., 'ACTUAL', 'ESTIMATE')
    AS_OF_DATE TIMESTAMP,                  -- As-of date
    PERIOD_END_DATE TIMESTAMP,             -- Period end date
    PERIOD VARCHAR(20),                    -- Period code (e.g., '2024Q1')
    CURRENCY VARCHAR(20),                  -- Currency code
    cf_comp_ffo_per_share DOUBLE PRECISION, -- Cash flow/comprehensive FFO per share
    bql_fld_str TEXT,                      -- BQL field string (can be long)
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Last update time
    dollar VARCHAR(10) NOT NULL DEFAULT 'lc', -- Currency indicator ('lc', 'usd')
    PRIMARY KEY (ticker, dt, fpt, fpo, dollar)
);
"""

# DDL: Create schema bbg (if not exists)
DDL_CREATE_BBG_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS bbg;
"""


def get_all_ddls() -> dict:
    """Return all DDL statements as a dictionary"""
    return {
        "create_bbg_schema": DDL_CREATE_BBG_SCHEMA,
        "est_cf_comp_ffo_per_share": DDL_EST_CF_COMP_FFO_PER_SHARE,
    }


if __name__ == "__main__":
    # Print all DDL statements
    for name, ddl in get_all_ddls().items():
        print(f"=== {name} ===")
        print(ddl)
        print()
