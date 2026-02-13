# -*- coding: utf-8 -*-
"""
Database module for NFT RDS instances

Provides:
- db: Local database instance (SQLAlchemy)
- bbg: Client-based bbg schema using connection_server
- get_client: Get connection server client
- rds: Client-based remote database instance
"""
from .res import (
    db,
    bbg,
    rds,
    ClientDB,
    ClientSchema,
    ClientTable,
    TB,
    S,
    S2,
    SCHEMA,
    DB,
)
from .db_client import (
    get_client,
    execute_query,
    execute_query_df,
    print_status,
    print_logs,
    disconnect_server,
    shutdown_server,
    restart_server,
)
from .procs import del_ticker_after
from .connection_server import run_server
from .presets import *

# __all__ = [
#     # Local/remote instances
#     "db",
#     "rds",
#     "bbg",
#     # Classes
#     "ClientDB",
#     "ClientSchema",
#     "ClientTable",
#     "TB",
#     "S",
#     "S2",
#     "SCHEMA",
#     "DB",
#     # Client functions
#     "get_client",
#     "execute_query",
#     "execute_query_df",
#     "print_status",
#     "print_logs",
#     # Procedures
#     "del_ticker_after",
# ]
