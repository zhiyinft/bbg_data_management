# -*- coding: utf-8 -*-
"""
Creator: Zhiyi Lu
Create time: 2026-02-09 13:35
Description: Database procedures
"""
import sqlalchemy as sa

from typing import Union
from loguru import logger

from .res import TB, ClientTable


def del_ticker_after(
    tb: Union[TB, ClientTable], ticker: str = None, dt: str = None, **kwargs
) -> int:
    """
    Delete rows from table based on conditions.

    Supports both TB (SQLAlchemy) and ClientTable (connection_server).

    Args:
        tb: TB or ClientTable instance
        ticker: Filter by ticker column
        dt: Filter by dt column (delete rows >= this date)
        **kwargs: Additional column filters

    Returns:
        int: Number of rows deleted

    Raises:
        KeyError: If a column is not found in the table
        ValueError: If no filter conditions are provided
    """

    is_client_table = isinstance(tb, ClientTable)

    # Build WHERE conditions
    conditions = []
    params = {}

    if ticker is not None:
        if "ticker" not in tb.c:
            raise KeyError(f"Column 'ticker' not found in table {tb.name}")
        if is_client_table:
            conditions.append("ticker = :ticker")
            params["ticker"] = ticker
        else:
            conditions.append(tb.c.ticker == ticker)

    if dt is not None:
        if "dt" not in tb.c:
            raise KeyError(f"Column 'dt' not found in table {tb.name}")
        if is_client_table:
            conditions.append("dt >= :dt")
            params["dt"] = dt
        else:
            conditions.append(tb.c.dt >= dt)

    for k, v in kwargs.items():
        if v is not None:
            if k not in tb.c:
                raise KeyError(f"Column '{k}' not found in table {tb.name}")
            if is_client_table:
                param_name = f"param_{k}"
                conditions.append(f"{k} = :{param_name}")
                params[param_name] = v
            else:
                conditions.append(tb.c[k] == v)

    if not conditions:
        raise ValueError(
            "At least one filter condition must be provided (ticker, dt, or kwargs)"
        )

    # Execute delete based on table type
    if is_client_table:
        # ClientTable: build SQL string and execute via connection_server
        where_clause = " AND ".join(conditions)
        query = f"DELETE FROM {tb.fullname} WHERE {where_clause}"

        result = tb.schema.db._client.execute(query, params, fetch=None)
        if "error" in result:
            logger.error(f"Failed to delete from {tb.fullname}: {result['error']}")
            raise Exception(result["error"])

        row_count = result["data"]["affected_rows"]
        logger.info(f"Deleted {row_count} rows from {tb.fullname} where {where_clause}")
        return row_count

    else:
        # TB: use SQLAlchemy
        if len(conditions) == 1:
            where_clause = conditions[0]
        else:
            where_clause = sa.and_(*conditions)

        with tb.engine.begin() as conn:
            result = conn.execute(sa.delete(tb.tb).where(where_clause))
            row_count = result.rowcount if hasattr(result, "rowcount") else result

            where_str = str(
                where_clause.compile(compile_kwargs={"literal_binds": True})
            )
            logger.info(f"Deleted {row_count} rows from {tb.name} where {where_str}")
            return row_count
