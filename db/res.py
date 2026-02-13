# -*- coding: utf-8 -*-
"""
Creator: Zhiyi Lu
Create time: 2026-02-09 13:47
Description:Database connection script for NFT RDS instances using SQLAlchemy
"""


import pandas as pd
import urllib.parse

from tqdm.auto import tqdm
from loguru import logger

import os
import sqlalchemy
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError


from .db_client import get_client
from .connection_server import NFT_RDS_CONFIG_LOCAL


datetime_col_alias = ["dt", "date", "raw_date"]
ticker_col_alias = ["ticker", "symbol", "bloomberg_ticker", "identifier"]
tk_dt_alt_map = {
    "memb": ("dt", "index_ticker"),
    "vmg": ("dt", "index_ticker"),
    "scraping.summary_count": ("raw_date", "identifier"),
}


def get_url(config: dict) -> str:
    db_type = config.get("db_type")
    if db_type == "postgres":
        prefix = "postgresql+psycopg2"
    elif db_type == "mysql":
        prefix = "mysql+pymysql"
    elif db_type == "mssql":
        prefix = "mssql+pymssql"
    else:
        msg = f"db_type: {db_type} is not supported yet."
        raise NotImplementedError(msg)

    pwd = urllib.parse.quote_plus(config["password"])

    port = config.get("port", "")
    port_str = f":{port}" if port else ""

    db = config.get("db", "")
    charset = "" if "charset" not in config else f"?charset={config['charset']}"

    url = f"{prefix}://{config['user']}:{pwd}@{config['host']}{port_str}/{db}" + charset
    return url


_pid = os.getpid()
_engine_created = {_pid: {}}


def get_engine(
    config: dict = None,
    url: str = None,
    one_off: bool = False,
) -> sqlalchemy.engine:
    if url is None:
        url = get_url(config=config)

    if one_off:
        e = sqlalchemy.create_engine(url, poolclass=NullPool, future=True)
    else:
        pid = os.getpid()
        e = _engine_created.get(pid, {}).get(url)
        if e is None:
            e = sqlalchemy.create_engine(url, future=True)
            if pid not in _engine_created:
                _engine_created[pid] = {}
            _engine_created[pid][url] = e
    return e


def px_callback(df, **kwargs):
    """used for load, TT etc."""
    if "adj_fac_px" in df.columns:
        adj_fac_px = df.adj_fac_px.copy()

        if "close" in df.columns:
            adj_fac_px.iat[-1] = (
                df.close.iat[-1] / df.orig_close.iat[-1]
            )  # when df3 is not empty
        else:
            if pd.isna(adj_fac_px.iat[-1]):
                adj_fac_px.iat[-1] = 1
        adj_px = adj_fac_px.iloc[::-1].cumprod()
        for c in ["close", "open", "high", "low"]:
            if f"orig_{c}" in df.columns:
                df[c] = df[f"orig_{c}"] * adj_px
    if "adj_fac_vol" in df.columns:
        adj_fac_vol = df.adj_fac_vol.copy()
        if "volumn" in df.columns:
            adj_fac_vol.iat[-1] = (
                df.volume.iat[-1] / df.orig_volume.iat[-1]
            )  # when df3 is not empty
        else:
            if pd.isna(adj_fac_vol.iat[-1]):
                adj_fac_vol.iat[-1] = 1
        adj_vol = adj_fac_vol.iloc[::-1].fillna(1).cumprod()
        df["volume"] = (df["orig_volume"] * adj_vol).fillna(0)
    return df


class S:
    def __init__(
        self,
        s: sqlalchemy.sql.Select,
        tb,
        dt_col=None,
        tk_col=None,
    ):
        self.s = s
        self.tb = tb
        self.dt_col = dt_col or datetime_col_alias[0]
        self.tk_col = tk_col or ticker_col_alias[0]

    def symbol(self, sl=None):

        if isinstance(sl, str):
            s = self.s.where(self.tb.c[self.tk_col] == sl)
        elif isinstance(sl, list):
            s = self.s.where(self.tb.c[self.tk_col].in_(sl))
        else:
            s = self.s
        new_obj = self.__class__(s, self.tb, self.dt_col, self.tk_col)
        return new_obj

    def sl(self, sl=None):
        return self.symbol(sl=sl)

    def on(self, dt):
        s = self.s
        if dt is not None:
            s = self.s.where(self.tb.c[self.dt_col] == dt)
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def before(self, dt):
        if dt is not None:
            s = self.s.where(self.tb.c[self.dt_col] < dt)
        else:
            s = self.s
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def after(self, dt):
        if dt is not None:
            s = self.s.where(self.tb.c[self.dt_col] > dt)
        else:
            s = self.s
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def on_before(self, dt):
        if dt is not None:
            s = self.s.where(self.tb.c[self.dt_col] <= dt)
        else:
            s = self.s
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def on_after(self, dt):
        if dt is not None:
            s = self.s.where(self.tb.c[self.dt_col] >= dt)
        else:
            s = self.s
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def first(
        self,
        n=1,
        order_by=None,
    ):
        s = self.s
        if order_by is None:
            s = s.order_by(self.tb.c[self.dt_col].asc()).limit(n)
        else:
            if isinstance(order_by, (tuple, list)):
                s = s.order_by(*order_by).limit(n)
            else:
                s = s.order_by(order_by).limit(n)
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def last(self, n=1, order_by=None):
        s = self.s
        if order_by is None:
            s = s.order_by(self.tb.c[self.dt_col].desc()).limit(n)
        else:
            if isinstance(order_by, (tuple, list)):
                s = s.order_by(*[sqlalchemy.desc(c) for c in order_by]).limit(n)
            else:
                s = s.order_by(sqlalchemy.desc(order_by)).limit(n)
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def limit(self, n=1, order_by=None):
        s = self.s
        if order_by is None:
            s = s.limit(n)
        else:
            if isinstance(order_by, (tuple, list)):
                s = s.order_by(*order_by).limit(n)
            else:
                s = s.order_by(order_by).limit(n)
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def where(self, clause=None, **kwargs):
        s = self.s
        if isinstance(clause, str):
            s = s.where(sqlalchemy.text(clause))
        else:
            if clause is None:
                clause = sqlalchemy.true()
            for k, v in kwargs.items():
                clause = clause & (self.tb.c[k] == v)
            s = s.where(clause)
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def distinct(self, field):
        s = self.s.distinct(field)
        return self.__class__(s, self.tb, self.dt_col, self.tk_col)

    def get(self, callback=None, force_raw=False, **kwargs):
        with self.tb.engine.connect() as conn:
            # with self.tb as conn:
            df = pd.read_sql(self.s, conn, **kwargs)

        if not df.empty:
            sort_cols = []
            if self.tk_col in df.columns:
                sort_cols.append(self.tk_col)
            if self.dt_col in df.columns:
                sort_cols.append(self.dt_col)
            if sort_cols:
                df = df.sort_values(sort_cols, ascending=False)

            if force_raw:
                return df
            else:
                if callback is not None:
                    df = callback(df, **kwargs)
                else:
                    if self.tb.name == "px":
                        df = px_callback(df, **kwargs)
        return df

    def __repr__(self):
        # return str(self.s)
        return self.s.compile(
            dialect=sqlalchemy.dialects.postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        ).string


class S2(S):
    """suitable for tables like memb"""

    def __init__(
        self,
        s: sqlalchemy.sql.Select,
        tb,
        dt_col=None,
        tk_col=None,
    ):
        if dt_col is None or tk_col is None:
            default_dt, default_tk = tk_dt_alt_map[tb.name]
            dt_col = dt_col or default_dt
            tk_col = tk_col or default_tk
        super().__init__(s, tb, dt_col, tk_col)

    def first(self, n=1, verbose=True):
        tk = self.tb.c[self.tk_col]
        dt = self.tb.c[self.dt_col]
        q = (
            sqlalchemy.select(tk, dt)
            .where(self.s.whereclause)
            .group_by(tk, dt)
            .order_by(tk, dt.asc())
            .limit(n)
        )
        with self.tb.bind.connect() as conn:
            df = pd.read_sql(q, conn)
            if verbose:
                print(df)

        if not df.empty:
            dt_res = df[self.dt_col]
            start_dt, end_dt = dt_res.iat[0], dt_res.iat[-1]
            start_dt = start_dt.strftime("%Y-%m-%d")
            end_dt = end_dt.strftime("%Y-%m-%d")
            return self.on_after(start_dt).on_before(end_dt)
        else:
            if verbose:
                print(f"no data! Maybe change the criteria or check the params.")
            return self

    def last(self, n=1, verbose=True):
        tk = self.tb.c[self.tk_col]
        dt = self.tb.c[self.dt_col]
        q = (
            sqlalchemy.select(tk, dt)
            .where(self.s.whereclause)
            .group_by(tk, dt)
            .order_by(tk, dt.desc())
            .limit(n)
        )
        with self.tb.engine.connect() as conn:
            df = pd.read_sql(q, conn)
            if verbose:
                print(df)

        if not df.empty:
            dt_res = df[self.dt_col]
            start_dt, end_dt = dt_res.iat[-1], dt_res.iat[0]
            start_dt = start_dt.strftime("%Y-%m-%d")
            end_dt = end_dt.strftime("%Y-%m-%d")
            return self.on_after(start_dt).on_before(end_dt)
        else:
            if verbose:
                print(f"no data! Maybe change the criteria or check the params.")
            return self


class TB:
    tb = None
    schema = None
    db = None
    engine = None

    __dt_col = None

    def init_from_table(self, tb: sqlalchemy.Table, engine):
        self.tb = tb
        self.engine = engine
        self.db = DB(engine=self.engine, reflect=False)
        self.schema = self.db.schema(tb.schema, reflect=False)

    def init_from_engine(self, schema_dot_tb: str, engine: sqlalchemy.engine.Engine):
        self.engine = engine
        self.db = DB(engine=engine, reflect=False)
        schema_name, tb_name = schema_dot_tb.split(".")
        self.schema = self.db.schema(schema_name, reflect=False)
        self.tb = self.schema.tb(tb_name, in_custom_cls=False)

    def __init__(self, tb, engine):
        if isinstance(tb, sqlalchemy.Table):
            self.init_from_table(tb, engine)
        else:
            self.init_from_engine(tb, engine)

        self.c = self.tb.c
        for dt_col in datetime_col_alias:
            if dt_col in self.tb.c:
                self.__dt_col = dt_col
                break
        else:
            # print(f"No datetime field found in {self.tb.fullname}")
            self.__dt_col = None

        for tk_col in ticker_col_alias:
            if tk_col in self.tb.c:
                self.__tk_col = tk_col
                break
        else:
            # print(f"No ticker field found in {self.tb.fullname}")
            self.__tk_col = None

    @property
    def dt_col(self):
        return self.__dt_col

    #
    # @dt_col.setter
    # def dt_col(self, f):
    #     if f not in self.tb.c:
    #         warnings.warn(f"The datetime field: {f} does not exist in {self.tb.name}.")
    #     else:
    #         self.__dt_col = f
    #
    @property
    def tk_col(self):
        return self.__tk_col

    def list_ticker(self, ticker_list=None, since=None, pattern=None):
        s = self.fields([self.tk_col])

        if ticker_list is not None:
            s = s.sl(ticker_list)

        if self.tk_col is not None and since is not None:
            s = s.on_after(since)

        if pattern is not None:
            s = s.where(self.tb.c[self.tk_col].like(pattern))

        df = s.distinct(self.tk_col)
        return df

    def list_dt(self, ticker=None, start=None, end=None):
        s = self.fields([self.dt_col])
        if ticker is not None:
            s = s.sl(ticker)
        if start is not None:
            s = s.on_after(start)
        if end is not None:
            s = s.on_before(end)
        df = s.distinct(self.dt_col)
        return df

    #
    # @tk_col.setter
    # def tk_col(self, f):
    #     if f not in self.tb.c:
    #         warnings.warn(
    #             f"The symbol field: {self.tk_col} does not exist in {self.tb.name}."
    #         )
    #     else:
    #         self.__tk_col = f

    def fields(self, f=None, vld=True, **kwargs):
        if f is None:
            s = self.tb.select()
        else:
            if isinstance(f, str):
                f = [f]
            bad_f = [i for i in f if i not in self.tb.c]
            if bad_f:
                raise KeyError(f"These columns are not in {self.tb.name}:\n\t{bad_f}")
            s = sqlalchemy.select(*[self.tb.c[i] for i in f])

        if "vld" in self.tb.c:
            if vld is True:
                if f is None or "vld" not in f:
                    s = s.where(self.tb.c["vld"] == True)

        if self.name in tk_dt_alt_map:
            return S2(s=s, tb=self)
        else:
            return S(s=s, tb=self, dt_col=self.dt_col, tk_col=self.tk_col)

    # default settings, pks are: ticker, dt
    s = property(fields)

    def update(self):
        # to be implemented by inherited class
        pass

    def __getitem__(self, item):
        if isinstance(item, tuple):
            if any([isinstance(i, slice) for i in item]):
                sl = []
                start = end = None
                for i in item:
                    if isinstance(i, slice):
                        start = i.start
                        end = i.stop
                    else:
                        sl.append(i)
                if len(sl) == 1:
                    sl = sl[0]
                return self.s.on_before(end).on_after(start).symbol(sl)
            else:
                tar_dt = []
                sl = []
                for i in item:
                    try:
                        ts = pd.Timestamp(i)
                        tar_dt.append(i)
                    except:
                        sl.append(i)
                s = self.s
                if len(tar_dt) > 1:
                    raise ValueError(
                        f"Can only accommodate one date, but received: \n\t{tar_dt}"
                    )
                elif len(tar_dt) == 1:
                    s = s.on(tar_dt[0])
                else:
                    pass

                if len(sl) == 1:
                    sl = sl[0]
                s = s.symbol(sl)
                return s
        else:
            if isinstance(item, str):
                try:
                    ts = pd.Timestamp(item)
                    return self.s.on(item)
                except:
                    return self.s.symbol(item)
            elif isinstance(item, list):
                return self.s.symbol(item)
            elif isinstance(item, slice):
                return self.s.on_before(item.stop).on_after(item.start)
            else:
                raise ValueError(f"Unrecognized item: {item}")

    def __getattr__(self, item):
        if item in self.tb.__dict__:
            return self.tb.__getattribute__(item)
        elif item in [
            "symbol",
            "sl",
            "on",
            "before",
            "after",
            "on_before",
            "on_after",
            "first",
            "last",
            "limit",
            "where",
            "distinct",
            "get",
        ]:
            return self.s.__getattribute__(item)
        else:
            return super().__getattribute__(item)

    def __repr__(self):
        return f"{self.fullname} with db: {self.engine.url.render_as_string(hide_password=False)}"

    # SIMPLIFIED INSERT FUNCTION WITH ROW-BY-ROW PROCESSING
    def insert(self, df, col_rename_dict=None, on_duplicate="replace"):
        """
        Simple insert function: try insert row by row, handle duplicates as specified.

        Args:
            df: DataFrame to insert
            col_rename_dict: Column renaming dictionary
            on_duplicate: How to handle duplicates ("skip", "replace")

        Returns:
            dict: Statistics about the operation
        """

        if col_rename_dict is not None:
            df = df.rename(columns=col_rename_dict)

        df = df[[c for c in df.columns if c in self.tb.c]]
        pk_cols = [i.name for i in self.tb.primary_key]
        df = df.dropna(subset=pk_cols)

        if df.empty:
            logger.warning(f"No valid data to insert into {self.name}")
            return {"inserted": 0, "skipped": 0, "errors": 0}

        stats = {"inserted": 0, "skipped": 0, "errors": 0}

        try:
            # Try bulk insert first (fastest path)
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    df.to_sql(
                        self.tb.name,
                        conn,
                        schema=self.schema.name,
                        if_exists="append",
                        index=False,
                    )
                    trans.commit()
                    stats["inserted"] = len(df)
                    logger.info(f"Bulk inserted {len(df)} rows into {self.name}")
                    return stats
                except Exception as e:
                    trans.rollback()
                    # Fall through to row-by-row processing
                    pass

        except Exception:
            pass  # Fall through to row-by-row processing

        # Row-by-row processing for duplicates
        logger.info(f"Bulk insert failed, processing row by row for {self.name}")

        with self.engine.connect() as conn:
            for _, row in tqdm(
                df.iterrows(), desc=f"Inserting to {self.name}", total=len(df)
            ):
                try:
                    # Try to insert the row
                    trans = conn.begin()
                    conn.execute(sqlalchemy.insert(self.tb).values(**row.to_dict()))
                    trans.commit()
                    stats["inserted"] += 1

                except sqlalchemy.exc.IntegrityError:
                    # Handle duplicate based on mode
                    trans.rollback()

                    if on_duplicate == "skip":
                        stats["skipped"] += 1
                        logger.debug(
                            f"Skipped duplicate row with PK: {row.loc[pk_cols].to_dict()}"
                        )

                    elif on_duplicate == "replace":
                        try:
                            # Delete existing row and insert new one
                            trans = conn.begin()

                            # Delete existing row
                            delete_stmt = sqlalchemy.delete(self.tb).filter_by(
                                **row.loc[pk_cols].to_dict()
                            )
                            conn.execute(delete_stmt)

                            # Insert new row
                            conn.execute(
                                sqlalchemy.insert(self.tb).values(**row.to_dict())
                            )
                            trans.commit()
                            stats["inserted"] += 1

                        except Exception as e:
                            trans.rollback()
                            logger.error(
                                f"Failed to replace row {row.loc[pk_cols].to_dict()}: {e}"
                            )
                            stats["errors"] += 1

                    else:
                        trans.rollback()
                        raise ValueError(
                            f"Unsupported on_duplicate mode: {on_duplicate}. Use 'skip' or 'replace'."
                        )

                except Exception as e:
                    trans.rollback()
                    logger.error(
                        f"Failed to insert row {row.loc[pk_cols].to_dict()}: {e}"
                    )
                    stats["errors"] += 1

        logger.info(f"Insert completed for {self.name}: {stats}")
        return stats

    def insert_safe(self, df, col_rename_dict=None, on_duplicate="replace"):
        """
        Safe version that returns success status instead of raising exceptions.

        Returns:
            tuple: (success: bool, stats: dict, error_message: str or None)
        """
        try:
            stats = self.insert(df, col_rename_dict, on_duplicate)
            return True, stats, None
        except Exception as e:
            return (
                False,
                {"inserted": 0, "skipped": 0, "errors": 0},
                str(e),
            )


class SCHEMA:
    def __init__(
        self,
        name=None,
        db=None,
        engine: sqlalchemy.engine.Engine = None,
        reflect: bool = True,
    ):
        self.db = db
        if engine is None:
            self.engine = self.db.engine
        else:
            self.engine = engine

        self.name = name
        self.meta = sqlalchemy.MetaData(schema=name)
        if reflect:
            self.reflect()
        else:
            self.tb_list = None

    def reflect(self, views=True):
        self.meta.reflect(bind=self.engine, views=views)

        reflected_tables = []
        tb_list = []
        for k, v in self.meta.tables.items():
            tb_name = k.split(".")[-1]
            tb_ = TB(v, engine=self.engine)
            super().__setattr__(tb_name, tb_)

            tb_list.append(tb_)
            reflected_tables.append(tb_name)

        self.tb_list = tb_list
        return reflected_tables

    def tb(self, name, in_custom_cls=True):
        """get a TB instance, but do not save into the class"""
        target = sqlalchemy.Table(name, self.meta, autoload_with=self.engine)
        engine = self.engine
        if in_custom_cls:
            return TB(target, engine=engine)
        else:
            return target

    def __getattr__(self, item):
        try:
            # get the TB and save into the class
            table = TB(
                sqlalchemy.Table(item, self.meta, autoload_with=self.engine),
                self.engine,
            )
            super().__setattr__(item, table)
            return table
        except Exception as e:
            raise e

    # def __getitem__(self, item):
    #     return self.__getattr__(item)

    def __repr__(self):
        return f"SCHEMA {self.name} with db: {self.engine.url.render_as_string(hide_password=False)}"


class DB:
    def __init__(self, config=None, engine=None, url=None, reflect=True):
        if engine is None:
            self.engine = get_engine(config=config, url=url)
        else:
            self.engine = engine

        self.name = self.engine.url.database

        if reflect:
            self.reflect()
        else:
            self.schema_list = None

    def reflect(self):
        """reflect schemas"""
        insp = sqlalchemy.inspect(self.engine)
        schema_list_reflected = insp.get_schema_names()

        reflected_schemas = []
        schema_list = []
        for schema_name in schema_list_reflected:
            if schema_name in ["public", "information_schema"]:
                continue
            # if db_name in allowed_dbs:
            schema_ = self.schema(schema_name)
            super().__setattr__(schema_name, schema_)
            reflected_schemas.append(schema_name)
            schema_list.append(schema_)
        self.schema_list = schema_list
        return reflected_schemas

    def schema(self, name, **kwargs):
        return SCHEMA(
            name=name,
            db=self,
            engine=self.engine,
            **kwargs,
        )

    def __getattr__(self, item):
        try:
            schema = self.schema(name=item)
            super().__setattr__(item, schema)
            return schema
        except Exception as e:
            raise e

    def __getitem__(self, item):
        return self.__getattr__(item)

    def __repr__(self):
        return f"DB {self.name} with url: {self.engine.url.render_as_string(hide_password=False)}"


db = DB(engine=get_engine(NFT_RDS_CONFIG_LOCAL))
# rds = DB(engine=get_engine(NFT_RDS_CONFIG))
# bbg = rds.bbg

# ============================================================================
# Cross-process reusable instances using connection_server
# ============================================================================


class ClientEngine:
    """
    Minimal engine-like object that uses connection_server for queries.

    This provides a subset of engine interface needed by DB, SCHEMA, TB classes.
    """

    def __init__(self, config_type: str = "remote"):
        self.config_type = config_type
        self._client = get_client(config_type, auto_start=True)

    def connect(self):
        return ClientConnection(self._client)

    def dispose(self):
        pass

    @property
    def url(self):
        # Fake URL for __repr__ methods
        class FakeURL:
            def __init__(self, config_type):
                self.config_type = config_type

            @property
            def database(self):
                return "nft"

            def render_as_string(self, hide_password=False):
                if self.config_type == "remote":
                    return "postgresql://user:***@rds.amazonaws.com/nft (via connection_server)"
                else:
                    return "postgresql://user:***@localhost/nft (via connection_server)"

        return FakeURL(self.config_type)


class ClientConnection:
    """
    Connection-like object that executes queries via connection_server.
    """

    def __init__(self, client):
        self._client = client
        self._closed = False

    def execute(self, query, params=None):
        """Execute query and return a result-like object."""
        # Compile SQLAlchemy query to string if needed
        if hasattr(query, "compile"):
            compiled = query.compile(
                dialect=sqlalchemy.dialects.postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
            query_str = str(compiled)
            # Convert to bound parameters format
            params = params or {}
        else:
            query_str = str(query)
            params = params or {}

        result = self._client.execute(query_str, params, fetch="all")
        if "error" in result:
            raise Exception(f"Query error: {result['error']}")
        return ClientResult(result.get("data", []))

    def close(self):
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class ClientResult:
    """
    Result-like object compatible with S.get() expectations.
    """

    def __init__(self, data):
        self._data = data if isinstance(data, list) else []
        self._index = 0

    def fetchall(self):
        return self._data

    def fetchone(self):
        if self._index < len(self._data):
            row = self._data[self._index]
            self._index += 1
            return row
        return None

    @property
    def rowcount(self):
        return len(self._data)

    def keys(self):
        if self._data:
            return self._data[0].keys()
        return []

    @property
    def _mapping(self):
        return self._data[0] if self._data else {}


# Client-based DB class that uses simple queries for reflection
class ClientDB:
    """
    DB-like class that uses connection_server for all operations.
    Uses simple queries instead of SQLAlchemy reflection.
    """

    def __init__(self, config_type: str = "remote"):

        self.config_type = config_type
        self._client = get_client(config_type, auto_start=True)
        self.name = "nft"
        self.engine = ClientEngine(config_type)
        self._schemas = {}

    def _list_schemas(self):
        """List all non-system schemas."""
        query = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('public', 'information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """
        result = self._client.execute(query, fetch="all")
        if "error" in result:
            return []
        return [row["schema_name"] for row in result.get("data", [])]

    def _list_tables(self, schema_name: str):
        """List all tables in a schema."""
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema_name
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        result = self._client.execute(
            query, params={"schema_name": schema_name}, fetch="all"
        )
        if "error" in result:
            return []
        return [row["table_name"] for row in result.get("data", [])]

    def schema(self, name: str, reflect: bool = False):
        """Get or create a ClientSchema."""
        if name not in self._schemas:
            self._schemas[name] = ClientSchema(name, self, reflect=reflect)
        return self._schemas[name]

    def __getattr__(self, item):
        return self.schema(item)

    def __getitem__(self, item):
        return self.schema(item)

    def __repr__(self):
        return f"ClientDB(config_type='{self.config_type}')"


class ClientSchema:
    """
    Schema-like class that lazily creates ClientTable instances.
    """

    def __init__(self, name: str, db: ClientDB, reflect: bool = False):
        self.name = name
        self.db = db
        self._tables = {}

        if reflect:
            self.reflect()

    def reflect(self):
        """Reflect all tables in the schema as attributes."""
        table_names = self.db._list_tables(self.name)
        for table_name in table_names:
            ctb = ClientTable(table_name, self)
            self._tables[table_name] = ctb
            super().__setattr__(table_name, ctb)

    def tb(self, table_name: str):
        """Get or create a ClientTable."""
        if table_name not in self._tables:
            self._tables[table_name] = ClientTable(table_name, self)
        return self._tables[table_name]

    def __getattr__(self, item):
        return self.tb(item)

    def __repr__(self):
        return f"ClientSchema('{self.name}')"


class ClientTable:
    """
    Table-like class that builds queries and executes via connection_server.
    """

    def __init__(self, name: str, schema: ClientSchema):
        self.name = name
        self.schema = schema
        self.engine = schema.db.engine
        self.db = schema.db
        self.bind = schema.db.engine

        # Get column info for this table
        self._columns = self._get_columns()
        self.c = self._build_c()

        # Detect dt and tk columns
        if self.fullname in tk_dt_alt_map:
            self.__dt_col, self.__tk_col = tk_dt_alt_map[self.fullname]
        else:
            self.__dt_col = None
            self.__tk_col = None
            for col in datetime_col_alias:
                if col in self._columns:
                    self.__dt_col = col
                    break
            for col in ticker_col_alias:
                if col in self._columns:
                    self.__tk_col = col
                    break

    @property
    def dt_col(self):
        return self.__dt_col

    @property
    def tk_col(self):
        return self.__tk_col

    @property
    def fullname(self):
        return f"{self.schema.name}.{self.name}"

    def _get_columns(self):
        """Get column names for this table."""
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema_name
            AND table_name = :table_name
            ORDER BY ordinal_position
        """
        result = self.schema.db._client.execute(
            query,
            params={"schema_name": self.schema.name, "table_name": self.name},
            fetch="all",
        )
        if "error" in result:
            return []
        return [row["column_name"] for row in result.get("data", [])]

    def _build_c(self):
        """Build a simple column accessor."""

        class ColumnAccessor:
            def __init__(self, columns):
                self._columns = columns

            def __contains__(self, item):
                return item in self._columns

            def __getitem__(self, item):
                class ColumnRef:
                    def __init__(self, name):
                        self.name = name

                    def __repr__(self):
                        return f"Column('{self.name}')"

                    def like(self, pattern):
                        return f"{self.name} LIKE '{pattern}'"

                if item in self._columns:
                    return ColumnRef(item)
                raise KeyError(f"Column '{item}' not found")

            def __getattr__(self, item):
                return self[item]

        return ColumnAccessor(self._columns)

    def select(self):
        """Create a SELECT query for this table."""
        return ClientS(self)

    def fields(self, f=None, vld=True):
        """Create a SELECT query with specific fields."""
        s = self.select()
        if f:
            if isinstance(f, str):
                f = [f]
            s._columns = f
        return s

    @property
    def s(self):
        return self.select()

    def insert(self, df, col_rename_dict=None):
        """
        Bulk insert DataFrame into the table.

        Args:
            df: DataFrame to insert
            col_rename_dict: Optional column renaming dictionary

        Returns:
            dict: Statistics about the operation (inserted, errors)
        """
        if col_rename_dict is not None:
            df = df.rename(columns=col_rename_dict)

        # Keep only columns that exist in the table
        valid_cols = [c for c in df.columns if c in self._columns]
        if not valid_cols:
            logger.warning(f"No valid columns to insert into {self.fullname}")
            return {"inserted": 0, "errors": 0}

        df = df[valid_cols].copy()
        df = df.dropna(how="all")  # Remove fully empty rows

        if df.empty:
            logger.warning(f"No valid data to insert into {self.fullname}")
            return {"inserted": 0, "errors": 0}

        # Build bulk INSERT query
        cols = ", ".join(valid_cols)
        placeholders = ", ".join([f":param_{i}" for i in range(len(valid_cols))])

        query = f"""
            INSERT INTO {self.fullname} ({cols})
            VALUES ({placeholders})
        """

        # Execute bulk insert via server
        # Send all rows as a batch
        stats = {"inserted": 0, "errors": 0}

        # Use a multi-value INSERT for better performance
        batch_size = 1000
        for start_idx in range(0, len(df), batch_size):
            batch = df.iloc[start_idx : start_idx + batch_size]

            # Build multi-value INSERT
            values_list = []
            params = {}
            param_idx = 0

            for _, row in batch.iterrows():
                row_placeholders = []
                for col in valid_cols:
                    param_name = f"p{param_idx}"
                    row_placeholders.append(f":{param_name}")
                    params[param_name] = row[col]
                    param_idx += 1
                values_list.append(f"({', '.join(row_placeholders)})")

            multi_query = f"""
                INSERT INTO {self.fullname} ({cols})
                VALUES {', '.join(values_list)}
            """

            result = self.schema.db._client.execute(multi_query, params, fetch=None)
            if "error" in result:
                logger.error(
                    f"Error inserting batch into {self.fullname}: {result['error']}"
                )
                stats["errors"] += len(batch)
            else:
                stats["inserted"] += len(batch)

        logger.info(f"Insert completed for {self.fullname}: {stats}")
        return stats

    def insert_safe(self, df, col_rename_dict=None):
        """
        Safe version that returns success status instead of raising exceptions.

        Returns:
            tuple: (success: bool, stats: dict, error_message: str or None)
        """
        try:
            stats = self.insert(df, col_rename_dict)
            return True, stats, None
        except Exception as e:
            return False, {"inserted": 0, "errors": 0}, str(e)

    def insert_fast(self, df, col_rename_dict=None) -> dict:
        """
        Fast bulk insert using psycopg2.extras.execute_values.
        Uses server-side bulk insert for better performance (10-50x faster).

        Args:
            df: DataFrame to insert
            col_rename_dict: Optional column renaming dictionary

        Returns:
            dict: Statistics about the operation (inserted, errors)

        Note:
            This method uses psycopg2's execute_values which is much faster
            than the standard insert() method. Use this for large datasets.
        """
        if col_rename_dict is not None:
            df = df.rename(columns=col_rename_dict)

        # Keep only columns that exist in the table
        valid_cols = [c for c in df.columns if c in self._columns]
        if not valid_cols:
            logger.warning(f"No valid columns to insert into {self.fullname}")
            return {"inserted": 0, "errors": 0}

        df = df[valid_cols].copy()
        df = df.dropna(how="all")  # Remove fully empty rows

        if df.empty:
            logger.warning(f"No valid data to insert into {self.fullname}")
            return {"inserted": 0, "errors": 0}

        # Replace NaN with None (NULL in SQL) for all columns
        # This is required because psycopg2 can't convert float NaN to integer columns
        df = df.replace({float("nan"): None})

        # Convert DataFrame to list of tuples (much faster than iterrows)
        # Use itertuples for speed (5-10x faster than iterrows)
        rows = [tuple(row) for row in df.itertuples(index=False)]

        # Execute bulk insert via server
        result = self.schema.db._client.bulk_insert(
            table=self.fullname,
            columns=valid_cols,
            rows=rows,
        )

        if "error" in result:
            logger.error(f"Error in fast insert to {self.fullname}: {result['error']}")
            return {"inserted": 0, "errors": len(df)}

        inserted = result.get("data", {}).get("affected_rows", len(df))
        logger.info(f"Fast insert completed for {self.fullname}: {inserted} rows")
        return {"inserted": inserted, "errors": 0}

    def insert_fast_safe(self, df, col_rename_dict=None):
        """
        Safe version of insert_fast that returns success status.

        Returns:
            tuple: (success: bool, stats: dict, error_message: str or None)
        """
        try:
            stats = self.insert_fast(df, col_rename_dict)
            return True, stats, None
        except Exception as e:
            return False, {"inserted": 0, "errors": 0}, str(e)

    def __repr__(self):
        return f"ClientTable('{self.fullname}')"

    def __getitem__(self, item):
        if isinstance(item, tuple):
            if any([isinstance(i, slice) for i in item]):
                sl = []
                start = end = None
                for i in item:
                    if isinstance(i, slice):
                        start = i.start
                        end = i.stop
                    else:
                        sl.append(i)
                if len(sl) == 1:
                    sl = sl[0]
                return self.s.on_before(end).on_after(start).symbol(sl)
            else:
                tar_dt = []
                sl = []
                for i in item:
                    try:
                        ts = pd.Timestamp(i)
                        tar_dt.append(i)
                    except:
                        sl.append(i)
                s = self.s
                if len(tar_dt) > 1:
                    raise ValueError(
                        f"Can only accommodate one date, but received: \n\t{tar_dt}"
                    )
                elif len(tar_dt) == 1:
                    s = s.on(tar_dt[0])
                else:
                    pass

                if len(sl) == 1:
                    sl = sl[0]
                s = s.symbol(sl)
                return s
        else:
            if isinstance(item, str):
                try:
                    ts = pd.Timestamp(item)
                    return self.s.on(item)
                except:
                    return self.s.symbol(item)
            elif isinstance(item, list):
                return self.s.symbol(item)
            elif isinstance(item, slice):
                return self.s.on_before(item.stop).on_after(item.start)
            else:
                raise ValueError(f"Unrecognized item: {item}")

    def __getattr__(self, item):
        if item in [
            "symbol",
            "sl",
            "on",
            "before",
            "after",
            "on_before",
            "on_after",
            "first",
            "last",
            "limit",
            "where",
            "distinct",
            "get",
        ]:
            return self.s.__getattribute__(item)
        else:
            return super().__getattribute__(item)


class ClientS:
    """
    Query builder for ClientTable - creates SQL and executes via client.
    Mimics the S class interface.
    """

    def __init__(self, tb: ClientTable, columns: list = None):
        self.tb = tb
        self._columns = columns
        self._where_clauses = []
        self._order_by = None
        self._limit_val = None
        self._params = {}
        self.dt_col = tb.dt_col
        self.tk_col = tb.tk_col

    def _compile(self):
        """Build the SQL query string."""
        cols = ", ".join(self._columns) if self._columns else "*"
        query = f"SELECT {cols} FROM {self.tb.fullname}"

        if self._where_clauses:
            query += " WHERE " + " AND ".join(self._where_clauses)

        if self._order_by:
            query += f" ORDER BY {self._order_by}"

        if self._limit_val:
            query += f" LIMIT {self._limit_val}"

        return query

    def where(self, clause=None, **kwargs):
        """Add WHERE clause."""
        if isinstance(clause, str):
            self._where_clauses.append(clause)
        for k, v in kwargs.items():
            param_name = f"param_{len(self._params)}"
            self._where_clauses.append(f"{k} = :{param_name}")
            self._params[param_name] = v
        return self

    def symbol(self, sl=None):
        """Filter by ticker/symbol."""
        if sl and self.tk_col:
            if isinstance(sl, str):
                self.where(f"{self.tk_col} = '{sl}'")
            elif isinstance(sl, list):
                sl_str = "', '".join(sl)
                self.where(f"{self.tk_col} IN ('{sl_str}')")
        return self

    def sl(self, sl=None):
        return self.symbol(sl)

    def on(self, dt):
        """Filter by date."""
        if dt and self.dt_col:
            self.where(f"{self.dt_col} = '{dt}'")
        return self

    def before(self, dt):
        """Filter before date."""
        if dt and self.dt_col:
            self.where(f"{self.dt_col} < '{dt}'")
        return self

    def after(self, dt):
        """Filter after date."""
        if dt and self.dt_col:
            self.where(f"{self.dt_col} > '{dt}'")
        return self

    def on_before(self, dt):
        """Filter on or before date."""
        if dt and self.dt_col:
            self.where(f"{self.dt_col} <= '{dt}'")
        return self

    def on_after(self, dt):
        """Filter on or after date."""
        if dt and self.dt_col:
            self.where(f"{self.dt_col} >= '{dt}'")
        return self

    def limit(self, n=1, order_by=None):
        """Add LIMIT."""
        self._limit_val = n
        if order_by:
            self._order_by = order_by
        return self

    def distinct(self, field):
        """Select distinct values."""
        self._columns = [f"DISTINCT {field}"]
        return self

    def get(self, callback=None, force_raw=False, **kwargs):
        """Execute query and return DataFrame."""
        query = self._compile()

        # Execute via client
        result = self.tb.schema.db._client.execute(query, self._params, fetch="all")
        if "error" in result:
            raise Exception(f"Query error: {result['error']}")

        data = result.get("data", [])

        # Build DataFrame
        if data:
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame()

        if not df.empty:
            # Parse datetime columns (server returns them as strings via JSON)
            # Check for common datetime column names
            datetime_cols = [
                col
                for col in [
                    self.dt_col,
                    "update_time",
                    "created_at",
                    "updated_at",
                ]
                if col in df.columns
            ]
            for col in datetime_cols:
                df[col] = pd.to_datetime(df[col])

            # Sort by dt_col and tk_col if present
            sort_cols = []
            if self.dt_col and self.dt_col in df.columns:
                sort_cols.append(self.dt_col)
            if self.tk_col and self.tk_col in df.columns:
                sort_cols.append(self.tk_col)
            if sort_cols:
                df = df.sort_values(sort_cols)

            # Apply callback
            if callback:
                df = callback(df, **kwargs)
            elif self.tb.name == "px" and not force_raw:
                df = px_callback(df, **kwargs)

        return df

    def __repr__(self):
        return self._compile()


# Cross-process reusable instances
rds = ClientDB(config_type="remote")
bbg = rds.schema("bbg", reflect=True)

if __name__ == "__main__":
    pass
