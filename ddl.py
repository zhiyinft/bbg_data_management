# -*- coding: utf-8 -*-
"""
DDL utilities for NFT database
Creator: Zhiyi Lu
Create time: 2026-01-21
Updated: 2026-02-25

Provides tools for:
1. Introspecting a local table's schema and generating its CREATE TABLE DDL
2. Applying DDL to the remote database via connection_server

Naming convention
-----------------
The local database has multiple schemas; the remote has a single schema.
By default a local table is mapped to the remote as follows:

    local  ``revision.is_eps``  →  remote  ``bbg.revision_is_eps``

i.e. the remote table name is ``{local_schema}_{local_table}`` inside the
single remote schema (``bbg`` by default).  Any function that accepts a
``remote_table`` parameter lets you override this.

Typical workflow
----------------
    from ddl import sync_table_to_remote

    # Preview what would be applied (no DB writes)
    sync_table_to_remote("revision", "is_eps", dry_run=True)

    # Create the table on remote
    sync_table_to_remote("revision", "is_eps")
"""

import sqlalchemy
from sqlalchemy import String, TIMESTAMP, text as sa_text
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql
from typing import Dict, List, Optional, Set
from loguru import logger


# Remote database has a single schema that receives all synced tables.
REMOTE_SCHEMA = "bbg"

# All column names are lowercased in the remote DDL.
# The local database may store them in upper-case; this flag normalises them
# on the way out without touching the source data.
REMOTE_LOWERCASE_COLUMNS = True

# ---------------------------------------------------------------------------
# Column-exclusion configuration
#
# Columns in DEFAULT_REMOTE_EXCLUDE are stripped from every table's DDL
# before it is applied to the remote database.
#
# To override for a specific table, add an entry to TABLE_REMOTE_EXCLUDE
# using the key "local_schema.local_table".  The entry completely replaces
# DEFAULT_REMOTE_EXCLUDE for that table, giving you full control:
#
#   • exclude *more* columns than the default → list them all
#   • exclude *fewer* (keep a column that's in the default) → omit it
#   • exclude *nothing* → use an empty set()
#
# Column names are matched case-insensitively.
# ---------------------------------------------------------------------------

DEFAULT_REMOTE_EXCLUDE: Set[str] = {
    "id",  # redundant with ticker
    "as_of_date",  # redundant with dt
    "bql_fld_str",  # BQL query-string log, not needed on remote
    "partial errors",
}

TABLE_REMOTE_EXCLUDE: dict = {
    # Examples:
    # "est.is_eps": {"id", "as_of_date", "bql_fld_str", "extra_col"},
    # "bbg.px":     set(),   # keep all columns for this table
    "bbg.pe_ratio_f1y": {
        "id",
        "as_of_date",
        "bql_fld_str",
        "PERIOD_OFFSET",
        "REVISION_DATE",
        "ACT_EST_DATA",
        "PERIOD",
    }
}

# ---------------------------------------------------------------------------
# Column type-mapping configuration
#
# Local tables often use unbounded TEXT for string columns because storage
# is not a concern.  On the remote RDS, bounded VARCHAR saves space and
# improves index performance.
#
# COLUMN_TYPE_MAP defines a global mapping from column name → SQLAlchemy
# type that is applied to every table whose column name matches (case-
# insensitive).  Only TEXT/VARCHAR columns need entries here; numeric,
# boolean, and timestamp types are left as-is.
#
# TABLE_COLUMN_TYPE_MAP lets you override or extend the global map for a
# specific table using the key "local_schema.local_table".  Entries are
# *merged* on top of COLUMN_TYPE_MAP (per-table wins on conflict).
#
# To skip the type override for a column in one table, map it to None:
#   TABLE_COLUMN_TYPE_MAP["est.is_eps"] = {"ticker": None}  # keep as-is
# ---------------------------------------------------------------------------

COLUMN_TYPE_MAP: Dict[str, sqlalchemy.types.TypeEngine] = {
    # ---- identifiers / codes -----------------------------------------------
    "ticker": String(32),  # BBG ticker, e.g. "AAPL US Equity"
    "fpt": String(4),  # period-type code: 'Q', 'A', 'FY', 'LTM'
    "fpr": String(16),
    "fpo": String(16),
    "dollar": String(4),  # currency indicator: 'lc', 'usd'
    # ---- period / time codes -----------------------------------------------
    "period": String(8),  # e.g. '2024Q1', '2024FY'
    "period_offset": String(4),  # e.g. '+0', '-1', '+4'
    # ---- estimate / data flags ---------------------------------------------
    "act_est_data": String(10),  # 'ACTUAL', 'ESTIMATE'
    # ---- financial / market metadata ---------------------------------------
    "currency": String(8),  # ISO currency code: 'USD', 'EUR', 'GBP'
    # ---- descriptive fields ------------------------------------------------
    "name": String(256),  # company / instrument name
    "custom_sector": String(128),  # sector classification
    "cluster": String(32),  # portfolio cluster identifier
    "watching": String(32),  # watching-status label
}

TABLE_COLUMN_TYPE_MAP: Dict[str, Dict[str, sqlalchemy.types.TypeEngine]] = {
    # Examples:
    # "est.is_eps": {"ticker": String(64)},   # wider ticker for this table
    # "est.is_eps": {"ticker": None},         # skip override, keep local type
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_local_engine() -> sqlalchemy.engine.Engine:
    """Return a SQLAlchemy engine connected to the local database."""
    from nft_db import get_engine, NFT_RDS_CONFIG_LOCAL

    return get_engine(config=NFT_RDS_CONFIG_LOCAL)


def remote_table_name(local_schema: str, local_table: str) -> str:
    """
    Return the default remote table name for a given local schema + table.

    Convention: ``{local_schema}_{local_table}``

    Examples:
        >>> remote_table_name("revision", "is_eps")
        'revision_is_eps'
        >>> remote_table_name("bbg", "px")
        'bbg_px'
    """
    if local_schema == "bbg":
        return local_table
    return f"{local_schema}_{local_table}"


# ---------------------------------------------------------------------------
# Core utilities
# ---------------------------------------------------------------------------


def _effective_exclude(local_schema: str, local_table: str) -> Set[str]:
    """
    Return the set of column names (lowercase) to exclude for a given table.

    Checks ``TABLE_REMOTE_EXCLUDE`` first; falls back to
    ``DEFAULT_REMOTE_EXCLUDE`` if no per-table override is found.
    """
    key = f"{local_schema}.{local_table}"
    cols = TABLE_REMOTE_EXCLUDE.get(key, DEFAULT_REMOTE_EXCLUDE)
    return {c.lower() for c in cols}


def _effective_type_map(
    local_schema: str, local_table: str
) -> Dict[str, sqlalchemy.types.TypeEngine]:
    """
    Return the effective column → type mapping for a given table.

    Merges ``COLUMN_TYPE_MAP`` (global defaults) with any entry in
    ``TABLE_COLUMN_TYPE_MAP`` for this table (per-table wins on conflict).
    Entries mapped to ``None`` in the per-table dict suppress the global
    override, leaving the column's local type intact.
    """
    key = f"{local_schema}.{local_table}"
    per_table = TABLE_COLUMN_TYPE_MAP.get(key, {})
    merged = {**COLUMN_TYPE_MAP, **per_table}
    # Drop None entries — they signal "keep the local type"
    return {k: v for k, v in merged.items() if v is not None}


def _ensure_primary_key(
    local_table: sqlalchemy.Table,
    engine: sqlalchemy.engine.Engine,
    primary_key: list = None,
    modify_local: bool = True,
) -> sqlalchemy.Table:
    """
    Guarantee that ``local_table`` has a primary key before DDL generation.

    If the table already has a primary key, return it unchanged.

    If there is no primary key:

    * When ``primary_key`` is given, use those columns directly (no prompt).
    * Otherwise, display the column list and prompt the user interactively.

    When ``modify_local=True`` the chosen columns are applied to the local
    database via ``ALTER TABLE … ADD PRIMARY KEY`` and the table is
    re-reflected so the constraint is persisted.

    When ``modify_local=False`` (e.g. dry-run) an in-memory Table object
    is returned with the chosen columns marked as PK — the local DB is
    left untouched.

    Args:
        local_table:  Reflected SQLAlchemy Table.
        engine:       Engine connected to the local database.
        primary_key:  Column name(s) to use as PK.  When supplied the
                      interactive prompt is skipped entirely.
        modify_local: Whether to ALTER TABLE on the local DB.

    Returns:
        A SQLAlchemy Table guaranteed to have a primary key.
    """
    if local_table.primary_key.columns:
        return local_table  # already has a PK

    schema_name = local_table.schema
    table_name = local_table.name
    full_name = f"{schema_name}.{table_name}"
    col_names = [col.name for col in local_table.columns]

    logger.warning(f"{full_name} has no primary key defined.")

    if primary_key is not None:
        # Caller supplied PK — validate immediately.
        invalid = [c for c in primary_key if c not in col_names]
        if invalid:
            raise ValueError(
                f"Supplied primary_key columns {invalid} do not exist in "
                f"{full_name}. Available columns: {col_names}"
            )
        chosen_pk = list(primary_key)
    else:
        # Interactive prompt.
        print(f"\n{'─' * 60}")
        print(f"  Table {full_name} has no primary key.")
        print(f"  Columns: {', '.join(col_names)}")
        print(f"{'─' * 60}")

        chosen_pk = None
        while chosen_pk is None:
            raw = input(
                "  Enter primary key column(s), comma-separated\n"
                "  (or 'skip' to proceed without a PK): "
            ).strip()

            if raw.lower() == "skip":
                logger.warning(f"Proceeding without a primary key for {full_name}.")
                return local_table

            candidates = [c.strip() for c in raw.split(",") if c.strip()]
            invalid = [c for c in candidates if c not in col_names]
            if invalid:
                print(f"  Column(s) not found: {invalid} — try again.\n")
                continue
            chosen_pk = candidates

    pk_list_str = ", ".join(chosen_pk)

    if modify_local:
        alter_sql = f"ALTER TABLE {full_name} ADD PRIMARY KEY ({pk_list_str});"
        print(f"\n  Applying to local DB:\n    {alter_sql}")
        try:
            with engine.begin() as conn:
                conn.execute(sqlalchemy.text(alter_sql))
            logger.info(f"Added primary key ({pk_list_str}) to {full_name}")
        except Exception as exc:
            raise RuntimeError(
                f"Failed to add primary key to {full_name}: {exc}"
            ) from exc

        # Re-reflect so the PK constraint is visible in the returned Table.
        new_meta = sqlalchemy.MetaData(schema=schema_name)
        return sqlalchemy.Table(table_name, new_meta, autoload_with=engine)

    else:
        # Dry-run: build an in-memory copy with PK columns flagged.
        pk_set = {c.lower() for c in chosen_pk}
        new_meta = sqlalchemy.MetaData(schema=schema_name)
        cols = [
            sqlalchemy.Column(
                col.name.lower() if REMOTE_LOWERCASE_COLUMNS else col.name,
                col.type,
                primary_key=col.name.lower() in pk_set,
                nullable=col.nullable,
                server_default=col.server_default,
                comment=col.comment,
            )
            for col in local_table.columns
        ]
        logger.info(
            f"Dry-run: using in-memory PK ({pk_list_str}) for {full_name} "
            "(local DB unchanged)"
        )
        return sqlalchemy.Table(table_name, new_meta, *cols)


def _build_filtered_table(
    local_table: sqlalchemy.Table,
    remote_schema: str,
    remote_name: str,
    exclude: Set[str],
    type_map: Dict[str, sqlalchemy.types.TypeEngine] = None,
    lean: List[str] = None,
) -> sqlalchemy.Table:
    """
    Build a new SQLAlchemy Table for the remote DB with column filtering and
    type remapping applied.

    Two filtering modes are supported:

    **Exclusion mode** (default, ``lean=None``):
      Every column is kept unless its lowercase name appears in ``exclude``.

    **Lean mode** (``lean=[...]``):
      Only PK columns, the explicitly listed *lean* columns, and ``update_time``
      are kept — in that order.  ``exclude`` is ignored in this mode.
      If ``update_time`` exists in the local table it is copied; otherwise a
      new ``TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP`` column is appended.

    Raises:
        ValueError: If any primary-key column would be excluded (exclusion mode),
                    or if a lean column does not exist in the local table,
                    or if no columns remain after filtering.
    """
    type_map = type_map or {}

    # Precompute a lowercase-keyed lookup of all source columns.
    src_map: Dict[str, sqlalchemy.Column] = {
        col.name.lower(): col for col in local_table.columns
    }
    pk_lower: Set[str] = {col.name.lower() for col in local_table.primary_key.columns}

    new_meta = sqlalchemy.MetaData(schema=remote_schema)

    def _make_col(src_col: sqlalchemy.Column, force_pk: bool = False) -> sqlalchemy.Column:
        name = src_col.name.lower() if REMOTE_LOWERCASE_COLUMNS else src_col.name
        override = type_map.get(src_col.name.lower())
        return sqlalchemy.Column(
            name,
            override if override is not None else src_col.type,
            primary_key=force_pk or src_col.primary_key,
            nullable=src_col.nullable,
            server_default=src_col.server_default,
            comment=src_col.comment,
        )

    # ------------------------------------------------------------------
    # Lean mode: PK cols → lean cols → update_time
    # ------------------------------------------------------------------
    if lean is not None:
        lean_lower = [c.lower() for c in lean]
        lean_set = set(lean_lower)

        # Validate: lean columns must exist in the source table.
        missing = lean_set - src_map.keys()
        if missing:
            raise ValueError(
                f"Lean column(s) {missing} do not exist in "
                f"{local_table.schema}.{local_table.name}."
            )

        kept_cols = []
        placed: Set[str] = set()

        # 1. PK columns in their natural table order.
        for col in local_table.primary_key.columns:
            kept_cols.append(_make_col(col, force_pk=True))
            placed.add(col.name.lower())

        # 2. Lean columns in user-specified order (skip if already placed as PK).
        for col_lower in lean_lower:
            if col_lower in placed:
                continue
            kept_cols.append(_make_col(src_map[col_lower]))
            placed.add(col_lower)

        # 3. update_time — copy from source if present, else synthesise.
        ut_name = "update_time"
        if ut_name not in placed:
            if ut_name in src_map:
                kept_cols.append(_make_col(src_map[ut_name]))
            else:
                kept_cols.append(
                    sqlalchemy.Column(
                        ut_name,
                        TIMESTAMP(),
                        nullable=False,
                        server_default=sa_text("CURRENT_TIMESTAMP"),
                    )
                )

        return sqlalchemy.Table(remote_name, new_meta, *kept_cols)

    # ------------------------------------------------------------------
    # Exclusion mode (original behaviour)
    # ------------------------------------------------------------------
    excluded_pk = pk_lower & exclude
    if excluded_pk:
        table_ref = f"{local_table.schema}.{local_table.name}"
        raise ValueError(
            f"Cannot exclude primary-key column(s) {excluded_pk} "
            f"from {table_ref}. "
            f'Update DEFAULT_REMOTE_EXCLUDE or TABLE_REMOTE_EXCLUDE["{table_ref}"].'
        )

    kept_cols = []
    for col in local_table.columns:
        if col.name.lower() in exclude:
            continue
        kept_cols.append(_make_col(col))

    if not kept_cols:
        raise ValueError(
            f"All columns were excluded for {local_table.schema}.{local_table.name}. "
            "Check DEFAULT_REMOTE_EXCLUDE / TABLE_REMOTE_EXCLUDE."
        )

    return sqlalchemy.Table(remote_name, new_meta, *kept_cols)


def introspect_table_ddl(
    local_schema: str,
    local_table: str,
    remote_schema: str = REMOTE_SCHEMA,
    remote_table: str = None,
    engine: sqlalchemy.engine.Engine = None,
    exclude_columns: Set[str] = None,
    lean: List[str] = None,
    type_overrides: Dict[str, sqlalchemy.types.TypeEngine] = None,
    primary_key: list = None,
    modify_local: bool = True,
) -> str:
    """
    Generate a ``CREATE TABLE IF NOT EXISTS`` statement for the remote DB
    by reflecting a local table's schema.

    Column types, defaults, NOT NULL constraints, and primary keys are all
    derived from the live local schema.  String columns are remapped to bounded
    ``VARCHAR`` according to ``COLUMN_TYPE_MAP`` / ``TABLE_COLUMN_TYPE_MAP``.

    If the local table has no primary key, the user is prompted interactively
    (or ``primary_key`` can be supplied to skip the prompt).

    **Column-selection modes** (mutually exclusive):

    * ``lean=[...]`` — *whitelist*: keep only PK columns + the listed columns +
      ``update_time``.  Final column order is always:
      ``[PK cols] → [lean cols] → update_time``.
      This is preferred when the remote table should be kept minimal.

    * ``exclude_columns={...}`` — *blacklist*: keep everything except the listed
      columns (and whatever ``DEFAULT_REMOTE_EXCLUDE`` / ``TABLE_REMOTE_EXCLUDE``
      specifies when omitted).

    Args:
        local_schema:    Schema of the source table on the local DB (e.g. ``'revision'``).
        local_table:     Table name on the local DB              (e.g. ``'is_eps'``).
        remote_schema:   Schema to use on the remote DB          (default: ``'bbg'``).
        remote_table:    Table name to use on the remote DB.
                         Defaults to ``{local_schema}_{local_table}``.
        engine:          SQLAlchemy engine for the local DB.
                         Defaults to ``NFT_RDS_CONFIG_LOCAL``.
        exclude_columns: Blacklist of column names to exclude.
                         Overrides ``DEFAULT_REMOTE_EXCLUDE`` /
                         ``TABLE_REMOTE_EXCLUDE`` when provided.
                         Ignored when ``lean`` is supplied.
        lean:            Whitelist of *additional* non-PK columns to include.
                         When given, only PK cols + these cols + ``update_time``
                         are written to the remote table.
                         Takes priority over ``exclude_columns``.
        type_overrides:  Explicit ``{column_name: SQLAlchemy type}`` mapping.
                         Overrides ``COLUMN_TYPE_MAP`` /
                         ``TABLE_COLUMN_TYPE_MAP`` when provided.
        primary_key:     Column name(s) to use as primary key when the local
                         table has none.  Skips the interactive prompt.
        modify_local:    When ``True`` (default) and the local table has no PK,
                         an ``ALTER TABLE … ADD PRIMARY KEY`` is applied to the
                         local DB so the constraint is persisted.  Set to
                         ``False`` for dry-run mode (in-memory PK only).

    Returns:
        A complete DDL string ready to execute on the remote database.

    Raises:
        sqlalchemy.exc.NoSuchTableError: If the table does not exist locally.
        ValueError: If a PK column is excluded, or all columns would be excluded.

    Example:
        >>> print(introspect_table_ddl("revision", "is_eps"))
        CREATE TABLE IF NOT EXISTS bbg.revision_is_eps (
            ticker VARCHAR(32) NOT NULL,
            dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            period VARCHAR(8),
            ...
        )
    """
    if engine is None:
        engine = _get_local_engine()

    if remote_table is None:
        remote_table = remote_table_name(local_schema, local_table)

    # In lean mode the whitelist supersedes the blacklist entirely.
    if lean is not None:
        exclude = set()
    else:
        exclude = (
            {c.lower() for c in exclude_columns}
            if exclude_columns is not None
            else _effective_exclude(local_schema, local_table)
        )

    tmap = (
        {k.lower(): v for k, v in type_overrides.items()}
        if type_overrides is not None
        else _effective_type_map(local_schema, local_table)
    )

    if lean is not None:
        logger.debug(
            f"Lean mode for {local_schema}.{local_table}: keeping {lean}"
        )
    elif exclude:
        logger.debug(f"Excluding columns from {local_schema}.{local_table}: {exclude}")
    if tmap:
        logger.debug(
            f"Type overrides for {local_schema}.{local_table}: "
            + ", ".join(f"{k}→{v}" for k, v in tmap.items())
        )

    # Reflect the local table
    meta = sqlalchemy.MetaData(schema=local_schema)
    local_tb = sqlalchemy.Table(local_table, meta, autoload_with=engine)

    # Ensure a primary key exists (prompt user or apply supplied list)
    local_tb = _ensure_primary_key(
        local_tb, engine, primary_key=primary_key, modify_local=modify_local
    )

    # Build filtered + type-remapped Table and compile DDL
    remote_tb = _build_filtered_table(
        local_tb, remote_schema, remote_table, exclude, tmap, lean=lean
    )

    ddl = str(
        CreateTable(remote_tb, if_not_exists=True).compile(dialect=postgresql.dialect())
    )

    return ddl.strip()


def apply_ddl_to_remote(ddl: str, config_type: str = "remote") -> dict:
    """
    Execute a DDL statement on the target database via connection_server.

    Args:
        ddl:         SQL DDL string to execute.
        config_type: ``'remote'`` or ``'local'``.

    Returns:
        Response dict from the connection server
        (``{'success': True, 'data': …}`` or ``{'error': …}``).
    """
    from db.db_client import get_client

    client = get_client(config_type)
    result = client.execute(ddl, fetch=None)

    if "error" in result:
        logger.error(f"DDL failed on {config_type}: {result['error']}")
    else:
        logger.info(f"DDL applied successfully on {config_type}")

    return result


def ensure_schema(schema_name: str, config_type: str = "remote") -> dict:
    """
    Create a schema on the target database if it does not already exist.

    Args:
        schema_name: PostgreSQL schema name.
        config_type: ``'remote'`` or ``'local'``.

    Returns:
        Response dict from the connection server.
    """
    ddl = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    logger.info(f"Ensuring schema '{schema_name}' exists on {config_type}")
    return apply_ddl_to_remote(ddl, config_type)


def sync_table_to_remote(
    local_schema: str,
    local_table: str,
    remote_schema: str = REMOTE_SCHEMA,
    remote_table: str = None,
    local_engine: sqlalchemy.engine.Engine = None,
    config_type: str = "remote",
    dry_run: bool = False,
    exclude_columns: Set[str] = None,
    lean: List[str] = None,
    type_overrides: Dict[str, sqlalchemy.types.TypeEngine] = None,
    primary_key: list = None,
) -> Optional[str]:
    """
    Introspect a local table and create the equivalent table on the remote DB.

    The remote table name defaults to ``{local_schema}_{local_table}`` inside
    ``remote_schema``.  Pass ``remote_table`` to override this.

    **Column-selection modes** (mutually exclusive):

    * ``lean=[...]`` — *whitelist*: the remote table contains only PK columns +
      the listed columns + ``update_time``, in that order.  Preferred when you
      want a minimal remote schema.

    * ``exclude_columns={...}`` — *blacklist*: remove named columns and keep
      everything else.  Falls back to ``DEFAULT_REMOTE_EXCLUDE`` /
      ``TABLE_REMOTE_EXCLUDE`` when omitted.  Ignored when ``lean`` is given.

    Steps performed:
        1. Reflect the local table.
        2. Apply column selection and type remapping; compile DDL.
        3. Ensure ``remote_schema`` exists on the target.
        4. Execute the DDL on the target database.

    Args:
        local_schema:     Source schema on the local DB  (e.g. ``'revision'``).
        local_table:      Source table  on the local DB  (e.g. ``'is_eps'``).
        remote_schema:    Target schema on the remote DB (default: ``'bbg'``).
        remote_table:     Target table name on the remote DB.
                          Defaults to ``{local_schema}_{local_table}``.
        local_engine:     SQLAlchemy engine for the local DB.
        config_type:      ``'remote'`` (default) or ``'local'``.
        dry_run:          When ``True``, print the DDL and return it without
                          executing anything.
        exclude_columns:  Blacklist of column names to strip.  Ignored when
                          ``lean`` is provided.
        lean:             Whitelist of non-PK columns to include on the remote.
                          Takes priority over ``exclude_columns``.
        type_overrides:   Explicit ``{column_name: SQLAlchemy type}`` mapping.
        primary_key:      PK columns to use when the local table has none.

    Returns:
        The DDL string when ``dry_run=True``, otherwise ``None``.

    Raises:
        RuntimeError: If DDL execution on the remote fails.

    Example:
        # revision.is_eps → bbg.revision_is_eps (default exclude list)
        sync_table_to_remote("revision", "is_eps")

        # lean mode: only PK + two value columns + update_time
        sync_table_to_remote(
            "est", "px_to_book_ratio",
            lean=["period_end_date", "px_to_book_ratio"],
        )

        # blacklist mode: ad-hoc exclusion
        sync_table_to_remote("est", "is_eps", exclude_columns={"id", "bql_fld_str"})
    """
    if remote_table is None:
        remote_table = remote_table_name(local_schema, local_table)

    logger.info(
        f"Introspecting {local_schema}.{local_table} "
        f"→ {remote_schema}.{remote_table} …"
    )
    ddl = introspect_table_ddl(
        local_schema,
        local_table,
        remote_schema=remote_schema,
        remote_table=remote_table,
        engine=local_engine,
        exclude_columns=exclude_columns,
        lean=lean,
        type_overrides=type_overrides,
        primary_key=primary_key,
        modify_local=not dry_run,
    )

    if dry_run:
        print(ddl)
        return ddl

    ensure_schema(remote_schema, config_type)

    logger.info(f"Applying DDL for {remote_schema}.{remote_table} to {config_type} …")
    result = apply_ddl_to_remote(ddl, config_type)

    if "error" in result:
        raise RuntimeError(
            f"Failed to create {remote_schema}.{remote_table} on {config_type}: "
            f"{result['error']}"
        )

    logger.info(f"Created {remote_schema}.{remote_table} on {config_type}")
    return None


def sync_schema_to_remote(
    local_schema: str,
    remote_schema: str = REMOTE_SCHEMA,
    local_engine: sqlalchemy.engine.Engine = None,
    config_type: str = "remote",
    dry_run: bool = False,
    exclude_columns: Set[str] = None,
    type_overrides: Dict[str, sqlalchemy.types.TypeEngine] = None,
    primary_key: list = None,
):
    """
    Sync every table from a local schema to the remote database.

    Each table ``{local_schema}.{table}`` becomes
    ``{remote_schema}.{local_schema}_{table}`` on the remote.
    Per-table overrides in ``TABLE_REMOTE_EXCLUDE`` are still respected;
    ``exclude_columns`` (if given) overrides everything for all tables in
    this batch.

    Args:
        local_schema:     Source schema to iterate over (e.g. ``'bbg'``).
        remote_schema:    Target schema on the remote DB (default: ``'bbg'``).
        local_engine:     SQLAlchemy engine for the local DB.
        config_type:      ``'remote'`` or ``'local'``.
        dry_run:          When ``True``, print DDLs without executing anything.
        exclude_columns:  Columns to exclude from every table in this batch.
    """
    engine = local_engine or _get_local_engine()
    insp = sqlalchemy.inspect(engine)
    table_names = insp.get_table_names(schema=local_schema)

    logger.info(
        f"Found {len(table_names)} tables in local '{local_schema}' schema: "
        f"{table_names}"
    )

    for table in table_names:
        try:
            sync_table_to_remote(
                local_schema,
                table,
                remote_schema=remote_schema,
                local_engine=engine,
                config_type=config_type,
                dry_run=dry_run,
                exclude_columns=exclude_columns,
                type_overrides=type_overrides,
                primary_key=primary_key,
            )
        except Exception as exc:
            logger.error(f"Failed to sync {local_schema}.{table}: {exc}")


# ---------------------------------------------------------------------------
# Temp-schema promotion
# ---------------------------------------------------------------------------

# Default PK candidates tried automatically when a temp table has no PK.
DEFAULT_TEMP_PK_CANDIDATES: list = ["dt", "ticker"]

# Columns excluded from every promoted table by default.
# Temp tables created by BBG data pulls often contain diagnostic fields that
# should not be persisted to a production schema.
# Column names are matched case-insensitively.
DEFAULT_TEMP_PROMOTE_EXCLUDE: Set[str] = {
    "partial_errors",  # BBG field name with underscore
    "partial errors",  # BBG field name with space
}

# Standard audit column added to every promoted table if not already present.
_UPDATE_TIME_COL = sqlalchemy.Column(
    "update_time",
    TIMESTAMP(),
    nullable=False,
    server_default=sa_text("CURRENT_TIMESTAMP"),
)

# Types used when a PK column does not exist in the source table and must be
# created.  Checked before COLUMN_TYPE_MAP.  If neither covers the column,
# promote_from_temp raises a clear error asking the caller to supply the type
# via the type_overrides argument.
NEW_PK_COLUMN_TYPES: Dict[str, sqlalchemy.types.TypeEngine] = {
    "dt": TIMESTAMP(),
    "date": TIMESTAMP(),
    # "ticker" is already covered by COLUMN_TYPE_MAP → String(32)
}


def _resolve_primary_key(
    table: sqlalchemy.Table,
    primary_key: list = None,
    allow_new_columns: bool = False,
) -> list:
    """
    Return a list of lowercase column names to use as the primary key.

    Resolution order:
      1. ``primary_key`` argument (validated immediately).
      2. Existing PK on the table (used as-is, info-logged).
      3. Columns from ``DEFAULT_TEMP_PK_CANDIDATES`` that exist in the table
         — offered to the user as a suggested default.
      4. Fully interactive prompt (column list printed, user types names).

    Args:
        allow_new_columns: When ``True``, column names not present in the
            table are accepted without error (the caller is responsible for
            adding them).  In the interactive prompt, such names are flagged
            with ``(new)`` so the user is aware.

    Returns an empty list if the user explicitly types ``'skip'``.
    """
    col_names_lower = [c.name.lower() for c in table.columns]
    full_name = f"{table.schema}.{table.name}"

    # --- 1. Caller-supplied ---
    if primary_key is not None:
        if not allow_new_columns:
            invalid = [c for c in primary_key if c.lower() not in col_names_lower]
            if invalid:
                raise ValueError(
                    f"PK column(s) {invalid} not found in {full_name}. "
                    f"Available: {col_names_lower}"
                )
        return [c.lower() for c in primary_key]

    # --- 2. Already has a PK ---
    if table.primary_key.columns:
        pk = [col.name.lower() for col in table.primary_key.columns]
        logger.info(f"Using existing PK for {full_name}: {pk}")
        return pk

    # --- 3. Default candidates (only existing columns) ---
    suggestions = [c for c in DEFAULT_TEMP_PK_CANDIDATES if c in col_names_lower]

    print(f"\n{'─' * 60}")
    print(f"  {full_name} has no primary key.")
    print(f"  Existing columns: {', '.join(col_names_lower)}")
    if allow_new_columns:
        print(
            "  (You may also name columns not yet in the table — they will be added.)"
        )

    if suggestions:
        print(f"  Suggested PK: {suggestions}")
        raw = input(
            "  Press Enter to accept, type custom column(s) comma-separated,\n"
            "  or 'skip' to proceed without a PK: "
        ).strip()
    else:
        raw = input(
            "  Enter PK column(s) comma-separated\n"
            "  (or 'skip' to proceed without a PK): "
        ).strip()

    print(f"{'─' * 60}")

    if raw.lower() == "skip":
        logger.warning(f"No primary key will be set for {full_name}.")
        return []

    if raw == "" and suggestions:
        return suggestions

    candidates = [c.strip().lower() for c in raw.split(",") if c.strip()]

    if not allow_new_columns:
        invalid = [c for c in candidates if c not in col_names_lower]
        if invalid:
            raise ValueError(
                f"Column(s) {invalid} not found in {full_name}. "
                f"Available: {col_names_lower}"
            )
    else:
        new_cols = [c for c in candidates if c not in col_names_lower]
        if new_cols:
            logger.info(f"PK column(s) {new_cols} not in {full_name} — will be added.")

    return candidates


def promote_from_temp(
    table_name: str,
    target_schema: str,
    source_schema: str = "temp",
    target_table: str = None,
    primary_key: list = None,
    add_update_time: bool = True,
    type_overrides: Dict[str, sqlalchemy.types.TypeEngine] = None,
    exclude_columns: Set[str] = None,
    engine: sqlalchemy.engine.Engine = None,
    dry_run: bool = False,
) -> str:
    """
    Promote a table from the ``temp`` schema to a proper target schema.

    Temp tables are typically created with the simplest possible DDL: no
    primary keys, no ``update_time``, TEXT columns everywhere, and sometimes
    missing standard fields.  This function builds a clean CREATE TABLE in
    the target schema that fills all those gaps.

    What is done automatically
    --------------------------
    * Existing column names are kept **exactly as they are** in the temp table
      (upper-case names stay upper-case).
    * TEXT/string columns are remapped via ``COLUMN_TYPE_MAP`` /
      ``TABLE_COLUMN_TYPE_MAP`` (same rules as the remote sync).
    * If no primary key exists, it is resolved in this order:

        1. ``primary_key`` argument (no prompt).
        2. Columns from ``DEFAULT_TEMP_PK_CANDIDATES`` (``dt``, ``ticker``)
           that are present in the table — offered as a suggested default.
        3. Fully interactive prompt.

      If a named PK column does **not** exist in the temp table it is
      **added** to the target table.  Its type is resolved from
      ``type_overrides`` → ``NEW_PK_COLUMN_TYPES`` → ``COLUMN_TYPE_MAP``.
      If none of these cover it, a ``ValueError`` is raised asking you to
      supply the type via ``type_overrides``.

    * Columns in ``DEFAULT_TEMP_PROMOTE_EXCLUDE`` (e.g. ``partial_errors``)
      are silently dropped.  Pass ``exclude_columns`` to override this set.
    * ``update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP`` is added
      unless the column already exists or ``add_update_time=False``.
    * PK columns are forced ``NOT NULL``.
    * The PRIMARY KEY constraint preserves the exact order in which you
      listed the columns.

    What is **not** done
    --------------------
    Data is never copied.  After the function returns it prints the
    ``INSERT … SELECT`` statement you can run manually.

    Args:
        table_name:       Table to promote from ``source_schema``.
        target_schema:    Destination schema in the local DB (e.g. ``'est'``).
        source_schema:    Source schema                      (default: ``'temp'``).
        target_table:     Name in the target schema.  Defaults to ``table_name``.
        primary_key:      Explicit PK column list; skips the interactive prompt.
        add_update_time:  Add ``update_time`` column if absent (default: ``True``).
        type_overrides:   Ad-hoc ``{col: SQLAlchemy type}`` map; merged on top of
                          ``COLUMN_TYPE_MAP`` / ``TABLE_COLUMN_TYPE_MAP``.
        exclude_columns:  Columns to drop from the target table.  Defaults to
                          ``DEFAULT_TEMP_PROMOTE_EXCLUDE``.  Pass an empty
                          ``set()`` to keep every column.
        engine:           Local SQLAlchemy engine (defaults to ``NFT_RDS_CONFIG_LOCAL``).
        dry_run:          Print the DDL without executing anything.

    Returns:
        The generated ``CREATE TABLE`` DDL string.

    Example:
        >>> promote_from_temp("signals", "est", dry_run=True)
        CREATE TABLE IF NOT EXISTS est.signals (
            ticker VARCHAR(32) NOT NULL,
            dt TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            signal_value DOUBLE PRECISION,
            update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, dt)
        )
        -- To copy data run:
        INSERT INTO est.signals (ticker, dt, signal_value)
        SELECT ticker, dt, signal_value FROM temp.signals;
    """
    if engine is None:
        engine = _get_local_engine()

    if target_table is None:
        target_table = table_name

    full_source = f"{source_schema}.{table_name}"
    full_target = f"{target_schema}.{target_table}"

    # 1. Reflect source table
    logger.info(f"Reflecting {full_source} …")
    meta = sqlalchemy.MetaData(schema=source_schema)
    try:
        source_tb = sqlalchemy.Table(table_name, meta, autoload_with=engine)
    except Exception as exc:
        raise ValueError(f"Could not reflect {full_source}: {exc}") from exc

    # 2. Resolve effective column-exclusion set
    exclude = (
        {c.lower() for c in exclude_columns}
        if exclude_columns is not None
        else {c.lower() for c in DEFAULT_TEMP_PROMOTE_EXCLUDE}
    )

    # 3. Resolve primary key (allow names not yet in the table)
    chosen_pk = _resolve_primary_key(
        source_tb, primary_key=primary_key, allow_new_columns=True
    )

    # 4. Resolve type map (same conventions as remote sync)
    tmap = (
        {k.lower(): v for k, v in type_overrides.items()}
        if type_overrides is not None
        else _effective_type_map(source_schema, table_name)
    )

    # 5. Snapshot lowercase column names to distinguish new vs existing PK cols
    existing_col_names_lower = {col.name.lower() for col in source_tb.columns}
    new_pk_cols = [pk for pk in chosen_pk if pk not in existing_col_names_lower]

    # 6. Build the target Table object
    #
    # Final column order (controls both the DDL field list and the PRIMARY KEY
    # clause, which follows primary_key=True column insertion order):
    #
    #   1. PK columns in user-specified order  (new ones created, existing ones
    #      taken from the source table)
    #   2. Non-PK source columns in original source order
    #   3. update_time  (audit column, always last)

    new_meta = sqlalchemy.MetaData(schema=target_schema)

    # Pre-build a lookup of source columns by lowercase name for step 6a.
    source_col_map: Dict[str, sqlalchemy.Column] = {
        col.name.lower(): col for col in source_tb.columns
    }
    excluded_names = []
    placed_pk: Set[str] = set()   # track which pk names have been emitted

    all_cols = []

    # 6a. PK columns first — in user-specified order
    for pk_name in chosen_pk:
        if pk_name in existing_col_names_lower:
            # Column exists in source: copy it with primary_key=True
            src_col = source_col_map[pk_name]
            override_type = tmap.get(pk_name)
            all_cols.append(
                sqlalchemy.Column(
                    src_col.name,                  # original case
                    override_type if override_type is not None else src_col.type,
                    primary_key=True,
                    nullable=False,
                    server_default=None,
                    comment=src_col.comment,
                )
            )
        else:
            # Column does not exist in source: create it
            col_type = (
                tmap.get(pk_name)
                or NEW_PK_COLUMN_TYPES.get(pk_name)
                or COLUMN_TYPE_MAP.get(pk_name)
            )
            if col_type is None:
                raise ValueError(
                    f"Cannot determine type for new PK column '{pk_name}'. "
                    f"Supply it via type_overrides={{'{pk_name}': <SQLAlchemy type>}}."
                )
            logger.info(
                f"Adding new PK column '{pk_name}' ({col_type}) to {full_target}."
            )
            all_cols.append(
                sqlalchemy.Column(pk_name, col_type, primary_key=True, nullable=False)
            )
        placed_pk.add(pk_name)

    # 6b. Non-PK source columns in original source order
    for col in source_tb.columns:
        col_lower = col.name.lower()
        if col_lower in placed_pk:
            continue                           # already emitted as PK
        if col_lower in exclude:
            excluded_names.append(col.name)
            continue
        override_type = tmap.get(col_lower)
        all_cols.append(
            sqlalchemy.Column(
                col.name,                      # original case preserved
                override_type if override_type is not None else col.type,
                nullable=col.nullable,
                server_default=col.server_default,
                comment=col.comment,
            )
        )

    if excluded_names:
        logger.info(f"Excluded columns from {full_source}: {excluded_names}")

    # 6c. update_time at the very end if absent
    if add_update_time and "update_time" not in existing_col_names_lower:
        all_cols.append(
            sqlalchemy.Column(
                "update_time",
                TIMESTAMP(),
                nullable=False,
                server_default=sa_text("CURRENT_TIMESTAMP"),
            )
        )
        logger.info("update_time column will be added.")

    target_tb = sqlalchemy.Table(target_table, new_meta, *all_cols)

    # 7. Compile DDL
    ddl = str(
        CreateTable(target_tb, if_not_exists=True).compile(
            dialect=postgresql.dialect()
        )
    ).strip()

    # 8. Build manual data-copy hint
    # Only include source columns that were not excluded; new PK cols and
    # update_time have no source data / carry server defaults.
    copy_cols = [
        col.name
        for col in source_tb.columns
        if col.name.lower() not in exclude
    ]
    copy_cols_str = ", ".join(copy_cols)
    copy_sql = (
        f"INSERT INTO {full_target} ({copy_cols_str})\n"
        f"SELECT {copy_cols_str}\n"
        f"FROM {full_source};"
    )

    if dry_run:
        print(ddl)
        print(f"\n-- To copy data run:\n{copy_sql}")
        return ddl

    # 9. Ensure target schema exists and execute DDL
    logger.info(f"Ensuring schema '{target_schema}' exists …")
    with engine.begin() as conn:
        conn.execute(sa_text(f"CREATE SCHEMA IF NOT EXISTS {target_schema}"))

    logger.info(f"Creating {full_target} …")
    with engine.begin() as conn:
        conn.execute(sa_text(ddl))

    logger.info(f"Table {full_target} created successfully.")
    print(f"\n{'─' * 60}")
    print(f"  {full_target} created.")
    if new_pk_cols:
        print(f"  New PK columns added (no source data): {new_pk_cols}")
    if excluded_names:
        print(f"  Excluded columns: {excluded_names}")
    print(f"  Copy data when ready:\n")
    print(f"  {copy_sql.replace(chr(10), chr(10) + '  ')}")
    print(f"{'─' * 60}\n")

    return ddl


# ---------------------------------------------------------------------------
# Manually curated DDLs
# Fallback for tables that do not exist in the local DB.
# Prefer introspect_table_ddl() / sync_table_to_remote() wherever possible.
# ---------------------------------------------------------------------------

_MANUAL_DDLS: dict = {
    "create_bbg_schema": "CREATE SCHEMA IF NOT EXISTS bbg;",
}


def get_manual_ddl(key: str) -> str:
    """Return a manually curated DDL by key, or raise ``KeyError``."""
    return _MANUAL_DDLS[key]


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Introspect a local table and apply its DDL to the remote DB.\n\n"
            "Naming: local <schema>.<table> → remote bbg.<schema>_<table>"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("local_schema", help="Local schema, e.g. revision")
    parser.add_argument("local_table", help="Local table,  e.g. is_eps")
    parser.add_argument(
        "--remote-schema",
        default=REMOTE_SCHEMA,
        help=f"Remote schema (default: {REMOTE_SCHEMA})",
    )
    parser.add_argument(
        "--remote-table",
        default=None,
        help="Remote table name (default: <local_schema>_<local_table>)",
    )
    parser.add_argument(
        "--target",
        default="remote",
        choices=["remote", "local"],
        help="Target database (default: remote)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the DDL without executing it",
    )
    parser.add_argument(
        "--all-tables",
        action="store_true",
        help="Sync all tables from local_schema (ignores local_table arg)",
    )

    args = parser.parse_args()

    if args.all_tables:
        sync_schema_to_remote(
            args.local_schema,
            remote_schema=args.remote_schema,
            config_type=args.target,
            dry_run=args.dry_run,
        )
    else:
        sync_table_to_remote(
            args.local_schema,
            args.local_table,
            remote_schema=args.remote_schema,
            remote_table=args.remote_table,
            config_type=args.target,
            dry_run=args.dry_run,
        )
