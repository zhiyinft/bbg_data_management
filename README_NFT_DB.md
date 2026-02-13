# nft_db.py

Single-file database module for NFT RDS instances. Copy this file to any project to use.

## Quick Start

```python
# Just import and use - server auto-starts on first query!
from nft_db import execute_query, execute_query_df, rds, bbg

# Execute a query (server auto-starts if not running)
result = execute_query("SELECT * FROM bbg.px WHERE ticker = :ticker", params={"ticker": "AAPL"})

# Query to DataFrame
df = execute_query_df("SELECT * FROM bbg.px LIMIT 10")

# Use the bbg schema directly
df = bbg.px.sl("AAPL").on_after("2024-01-01").get()
```

## Available Imports

```python
# Core database instances
from nft_db import db, rds, bbg

# Query functions
from nft_db import execute_query, execute_query_df

# Connection server (optional manual control)
from nft_db import get_client, run_server, print_status, shutdown_server

# Classes
from nft_db import DB, SCHEMA, TB, ClientDB, ClientSchema, ClientTable

# Procedures
from nft_db import del_ticker_after
```

## Server Management

Server auto-starts on first query, but you can also control it manually:

```python
# Start server manually (optional - auto-starts by default)
from nft_db import run_server
run_server()

# Or via command line:
# python nft_db.py

# Check server status
from nft_db import print_status
print_status()

# Shutdown server
from nft_db import shutdown_server
shutdown_server()
```

## Common Patterns

```python
from nft_db import bbg

# Query with symbol and date range
df = bbg.px.sl("AAPL").on_after("2024-01-01").on_before("2024-12-31").get()

# Query multiple symbols
df = bbg.px.sl(["AAPL", "MSFT", "GOOG"]).on_after("2024-01-01").get()

# Get first/last records
df = bbg.px.sl("AAPL").first(5)
df = bbg.px.sl("AAPL").last(5)

# Insert data
bbg.px.insert_fast(df)  # Fast bulk insert
bbg.px.insert(df)       # Standard insert with duplicate handling

# Delete data
from nft_db import del_ticker_after
del_ticker_after(bbg.px, ticker="AAPL", dt="2024-01-01")
```

## Configuration

Edit `NFT_RDS_CONFIG` and `NFT_RDS_CONFIG_LOCAL` in `nft_db.py` to change database credentials.

## Requirements

```
sqlalchemy
psycopg2
pandas
loguru
```
