# -*- coding: utf-8 -*-
"""
Creator: Zhiyi Lu
Create time: 2026-02-13 11:20
Description: wrappers and presets for ease of use
"""
from .res import rds, bbg
from .db_client import get_client

c = get_client()


def get_px(ticker, start=None, raw=False):
    df = (
        bbg.px.fields(["dt", "orig_close", "adj_fac_px"])
        .symbol(ticker)
        .on_after(start)
        .get(force_raw=raw)
    )
    if not df.empty:
        df = df.set_index("dt").sort_index()
        df["ticker"] = ticker
        if raw:
            df = df[["ticker", "orig_close"]]
        else:
            df = df[["ticker", "close"]]
    return df


def get_indeed(company, start=None):
    s = rds.scraping.summary_count.fields(
        [
            "raw_date",
            "identifier",
            "count",
        ]
    ).on_after(start)
    if company is not None:
        s = s.symbol(company)

    df = s.get()
    if not df.empty:
        df = df.set_index(s.dt_col).sort_index()
    return df
