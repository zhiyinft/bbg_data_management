# NFT Bloomberg Data Management

Database management tools for NFT Bloomberg data with persistent connection server.

## Module Structure

```
bbg_data_management/
├── db/
│   ├── __init__.py           # Module exports
│   ├── res.py                # Core database classes (DB, SCHEMA, TB, S, ClientDB, ClientSchema, ClientTable)
│   ├── connection_server.py  # Persistent connection server
│   ├── db_client.py          # Client for connecting to server
│   ├── procs.py              # Database procedures
│   ├── presets.py            # Convenience functions (get_px, get_indeed)
│   └── db_logs/              # Server logs (when running from db/ module)
├── nft_db.py                 # Standalone merged module (copy to other projects)
├── merge_db.py               # Script to merge db/ module into nft_db.py
├── README.md                 # This file (main project README)
├── README_NFT_DB.md          # Documentation for standalone nft_db.py
├── example_client_usage.py   # Usage examples for connection server
├── ddl.py                    # DDL scripts
└── db_logs/                  # Server logs (when running nft_db.py standalone)
```

## Features

- **Persistent Connection Server**: Maintains database connections across script runs
- **Auto-start**: Server starts automatically when first client connects
- **Shared Connections**: Multiple scripts share the same connection pool
- **Auto-shutdown**: Server shuts down after 1 hour of inactivity
- **Reflection**: Auto-discover schemas and tables
- **Fluent API**: Chain methods like `bbg.px.sl("AAPL").on_after("2024-01-01").get()`
- **Bulk Operations**: Fast bulk insert with batch_size=1000
- **Standalone Module**: `nft_db.py` can be copied to any project

## Quick Start

### Using the Module (Recommended)

```python
from db import bbg, rds, execute_query, execute_query_df

# Query using the fluent API
df = bbg.px.sl("AAPL").on_after("2024-01-01").get()

# Query multiple symbols
df = bbg.px.sl(["AAPL", "MSFT", "GOOG"]).on_after("2024-01-01").get()

# Get first/last records
df = bbg.px.sl("AAPL").first(5).get()
df = bbg.px.sl("AAPL").last(5).get()

# Insert data
bbg.px.insert_fast(df)  # Fast bulk insert
bbg.px.insert(df)       # Standard insert with duplicate handling
```

### Direct Query Execution

```python
from db import execute_query, execute_query_df

# Simple query
results = execute_query("SELECT * FROM bbg.px WHERE ticker = :ticker", 
                        params={"ticker": "AAPL"})

# Query to DataFrame
df = execute_query_df("SELECT * FROM bbg.px LIMIT 10")
```

### Server Management

```python
from db import print_status, test_connection, shutdown_server

# Check server status
print_status()

# Test database connection
test_connection("remote")

# Shutdown server
shutdown_server()
```

### Using Presets (Convenience Functions)

```python
from db import get_px, get_indeed

# Get price data for a ticker
df = get_px("AAPL", start="2024-01-01")

# Get Indeed job posting data
df = get_indeed("company_name", start="2024-01-01")
```

## API Reference

### Database Instances

- `db` - Local database (SQLAlchemy, localhost:5432)
- `rds` - Remote RDS database using connection_server
- `bbg` - Bloomberg schema on RDS (auto-reflected)

### Core Classes

#### For Local Database (SQLAlchemy)

- `DB` - Database wrapper with schema reflection
- `SCHEMA` - Schema with table reflection
- `TB` - Table with query builder and insert capabilities
- `S` - Query builder (SELECT statement wrapper)
- `S2` - Query builder for special tables (memb, vmg, etc.)

#### For Remote Database (Connection Server)

- `ClientDB` - Database wrapper using connection_server
- `ClientSchema` - Schema with lazy table loading
- `ClientTable` - Table with query builder and insert capabilities

### Query Builder Methods (S class and ClientTable)

- `sl(symbols)` or `symbol(symbols)` - Filter by ticker symbol(s)
- `on(date)` - Filter for exact date
- `before(date)` - Filter for dates before
- `after(date)` - Filter for dates after
- `on_before(date)` - Filter for dates on or before
- `on_after(date)` - Filter for dates on or after
- `first(n)` - Get first n records (ordered by date ascending)
- `last(n)` - Get last n records (ordered by date descending)
- `limit(n)` - Limit to n records
- `where(clause, **kwargs)` - Add WHERE conditions
- `distinct(field)` - Select distinct values
- `get(callback=None, force_raw=False)` - Execute query and return DataFrame

### Client Functions

- `get_client(config_type="remote")` - Get connection server client
- `execute_query(query, params, config_type, fetch)` - Execute SQL query
- `execute_query_df(query, params, config_type)` - Execute query and return DataFrame
- `test_connection(config_type)` - Test database connection
- `print_status()` - Print server status
- `print_logs(lines)` - Print recent server logs
- `disconnect_server(config_type)` - Disconnect database connection
- `shutdown_server()` - Shutdown the connection server
- `restart_server()` - Restart the connection server

### Procedures

- `del_ticker_after(tb, ticker=None, dt=None, **kwargs)` - Delete rows from table by conditions

### Presets

- `get_px(ticker, start=None, raw=False)` - Get price data for a ticker
- `get_indeed(company, start=None)` - Get Indeed job posting data

## Configuration

Edit database configs in `db/connection_server.py`:

```python
NFT_RDS_CONFIG = {
    "db_type": "postgres",
    "host": "your-rds-endpoint",
    "user": "postgres",
    "password": "your-password",
    "port": 5432,
    "db": "nft",
}
```

## Server Control

### From Python

```python
from db import run_server

# Start server manually
run_server(port=15500)

# Start local database server
run_server(port=15501)
```

### From Command Line

```bash
# Start server using the db module
python -m db.connection_server --port 15500

# Start local database server
python -m db.connection_server --local

# Custom idle timeout (in seconds)
python -m db.connection_server --idle-timeout 1800

# Run as daemon (Linux/Mac only)
python -m db.connection_server --daemon
```

### Using Standalone nft_db.py

```bash
# Start server using standalone module
python nft_db.py

# Start with custom port
python nft_db.py --port 15500
```

## Standalone Module (nft_db.py)

The `nft_db.py` file is a self-contained module that can be copied to any project:

```python
# In another project - just copy nft_db.py and import
from nft_db import db, rds, bbg, execute_query, execute_query_df, get_px

# Server auto-starts on first query
df = bbg.px.sl("AAPL").on_after("2024-01-01").get()
```

See `README_NFT_DB.md` for full documentation on the standalone module.

## Troubleshooting

### Check Server Logs

```python
from db import print_logs
print_logs(lines=50)
```

### Log File Locations

- When running from `db/` module: `db/db_logs/connection_server.log`
- When running `nft_db.py` standalone: `db_logs/connection_server.log`

### Common Issues

1. **Server not starting**: Check if port 15500 is already in use
2. **Connection refused**: Ensure the server is running or auto-start is enabled
3. **Authentication errors**: Check credentials in `connection_server.py`

## Requirements

```
sqlalchemy
psycopg2
pandas
loguru
tqdm
```

## Creating the Standalone Module

To regenerate `nft_db.py` from the `db/` module:

```bash
python merge_db.py
```

This will scan the `db/` directory and create a merged `nft_db.py` file.
