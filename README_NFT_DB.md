# nft_db.py

Single-file database module for NFT RDS instances. Copy this file to any project to use.

## Quick Start

```python
# Just import and use - server auto-starts on first query!
from nft_db import execute_query, execute_query_df, rds, bbg, get_px, get_indeed

# Most Popular: Get price data for a ticker (uses presets)
df = get_px("AAPL", start="2024-01-01")

# Execute a query (server auto-starts if not running)
result = execute_query("SELECT * FROM bbg.px WHERE ticker = :ticker", params={"ticker": "AAPL"})

# Query to DataFrame
df = execute_query_df("SELECT * FROM bbg.px LIMIT 10")

# Use the bbg schema directly with fluent API
df = bbg.px.sl("AAPL").on_after("2024-01-01").get()
```

## Most Popular: Presets (Convenience Functions)

Presets are the easiest way to get commonly used data. Import and use these functions directly:

### `get_px(ticker, start=None, raw=False)`

Get price data for a specific ticker. This is the most commonly used function.

```python
from nft_db import get_px

# Get adjusted price data for AAPL since 2024-01-01
df = get_px("AAPL", start="2024-01-01")
# Returns DataFrame with columns: ticker, close (adjusted)

# Get raw (unadjusted) price data
df = get_px("AAPL", start="2024-01-01", raw=True)
# Returns DataFrame with columns: ticker, orig_close

# Get all available price data (no start date)
df = get_px("MSFT")
```

### `get_indeed(company, start=None)`

Get Indeed job posting summary data for a company.

```python
from nft_db import get_indeed

# Get job posting data for a specific company since 2024-01-01
df = get_indeed("company_name", start="2024-01-01")

# Get all available data for a company
df = get_indeed("company_name")

# Get data for all companies since a date
df = get_indeed(None, start="2024-01-01")
```

## Available Imports

```python
# === Most Popular: Presets (Convenience Functions) ===
from nft_db import get_px, get_indeed

# Core database instances
from nft_db import db, rds, bbg

# Query functions
from nft_db import execute_query, execute_query_df

# Connection server (optional manual control)
from nft_db import get_client, run_server, print_status, shutdown_server, restart_server

# Classes
from nft_db import DB, SCHEMA, TB, S, S2, ClientDB, ClientSchema, ClientTable

# Procedures
from nft_db import del_ticker_after
```

## Fluent API: Query Builder Patterns

Use the fluent API for complex queries with method chaining:

```python
from nft_db import bbg

# Query with symbol and date range
df = bbg.px.sl("AAPL").on_after("2024-01-01").on_before("2024-12-31").get()

# Query multiple symbols
df = bbg.px.sl(["AAPL", "MSFT", "GOOG"]).on_after("2024-01-01").get()

# Get first/last records
df = bbg.px.sl("AAPL").first(5).get()
df = bbg.px.sl("AAPL").last(5).get()

# Get specific fields
df = bbg.px.fields(["dt", "close", "volume"]).sl("AAPL").on_after("2024-01-01").get()

# Get distinct tickers
df = bbg.px.fields(["ticker"]).distinct("ticker").get()

# Add WHERE conditions
df = bbg.px.sl("AAPL").where("close > 100").get()
```

### Query Builder Methods

Available methods when using the fluent API (e.g., `bbg.px.sl(...)`):

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

## Direct Query Execution

Execute raw SQL queries when you need full control:

```python
from nft_db import execute_query, execute_query_df

# Simple query returning list of dicts
results = execute_query("SELECT * FROM bbg.px WHERE ticker = :ticker", 
                        params={"ticker": "AAPL"})

# Query returning a single row
result = execute_query("SELECT * FROM bbg.px WHERE ticker = :ticker", 
                       params={"ticker": "AAPL"}, fetch="one")

# Query to DataFrame
df = execute_query_df("SELECT * FROM bbg.px LIMIT 10")

# Query to DataFrame with parameters
df = execute_query_df("SELECT * FROM bbg.px WHERE ticker = :ticker", 
                      params={"ticker": "AAPL"})

# Execute without fetching results (for INSERT/UPDATE/DELETE)
execute_query("DELETE FROM bbg.px WHERE ticker = :ticker", 
              params={"ticker": "AAPL"}, fetch=None)
```

## Data Insertion

Insert DataFrame data into tables:

```python
from nft_db import bbg
import pandas as pd

# Prepare DataFrame to insert
df = pd.DataFrame({
    "ticker": ["AAPL", "AAPL"],
    "dt": ["2024-01-01", "2024-01-02"],
    "close": [150.0, 152.0],
})

# Fast bulk insert (recommended for large datasets)
bbg.px.insert_fast(df)

# Standard insert with duplicate handling
bbg.px.insert(df, on_duplicate="replace")  # Replace existing rows
bbg.px.insert(df, on_duplicate="skip")     # Skip duplicates

# Safe insert (returns success status instead of raising)
success, stats, error = bbg.px.insert_safe(df)
```

## Data Deletion

Delete data from tables:

```python
from nft_db import del_ticker_after, bbg

# Delete all rows for a ticker after a specific date
del_ticker_after(bbg.px, ticker="AAPL", dt="2024-01-01")

# Delete all rows for a ticker (no date filter)
del_ticker_after(bbg.px, ticker="AAPL")

# Delete with additional filters
del_ticker_after(bbg.px, ticker="AAPL", dt="2024-01-01", vld=True)
```

## Server Management

The connection server auto-starts on first query, but you can also control it manually:

```python
from nft_db import run_server, print_status, shutdown_server, restart_server

# Start server manually (optional - auto-starts by default)
run_server()

# Start on custom port
run_server(port=15500)

# Check server status
print_status()

# Print recent server logs
from nft_db import print_logs
print_logs(lines=50)

# Shutdown server
shutdown_server()

# Restart server (useful after code updates)
restart_server()
```

### Command Line Usage

```bash
# Start server
python nft_db.py

# Start with custom port
python nft_db.py --port 15500

# Start local database server
python nft_db.py --local

# Custom idle timeout (in seconds)
python nft_db.py --idle-timeout 1800

# Run as daemon (Linux/Mac only)
python nft_db.py --daemon
```

## Database Instances

Three database instances are available:

```python
from nft_db import db, rds, bbg

# db - Local database (SQLAlchemy direct connection to localhost:5432)
df = db.bbg.px.sl("AAPL").get()

# rds - Remote RDS database (via connection_server)
df = rds.bbg.px.sl("AAPL").get()

# bbg - Convenience alias for rds.bbg (most commonly used)
df = bbg.px.sl("AAPL").get()
```

## Configuration

Edit `NFT_RDS_CONFIG` and `NFT_RDS_CONFIG_LOCAL` in `nft_db.py` to change database credentials:

```python
NFT_RDS_CONFIG = {
    "db_type": "postgres",
    "host": "your-rds-endpoint",
    "user": "postgres",
    "password": "your-password",
    "port": 5432,
    "db": "nft",
}

NFT_RDS_CONFIG_LOCAL = {
    "db_type": "postgres",
    "host": "localhost",
    "user": "postgres",
    "password": "nft2024",
    "port": 5432,
    "db": "nft",
}
```

## Requirements

```
sqlalchemy
psycopg2
pandas
loguru
tqdm
```

## Troubleshooting

### Check Server Logs

```python
from nft_db import print_logs, get_logs

# Print recent logs
print_logs(lines=50)

# Get logs as string
logs = get_logs(lines=50)
```

### Common Issues

1. **Server not starting**: Check if port 15500 is already in use
2. **Connection refused**: Ensure the server is running or auto-start is enabled
3. **Authentication errors**: Check credentials in `NFT_RDS_CONFIG`

### Log File Location

Logs are written to `db_logs/connection_server.log` (relative to where `nft_db.py` is located).

## Full Example

```python
from nft_db import get_px, bbg, execute_query_df
import pandas as pd

# Method 1: Use presets (easiest)
df_aapl = get_px("AAPL", start="2024-01-01")
df_msft = get_px("MSFT", start="2024-01-01")
df_combined = pd.concat([df_aapl, df_msft])

# Method 2: Use fluent API
df = bbg.px.sl(["AAPL", "MSFT"]).on_after("2024-01-01").get()

# Method 3: Use direct SQL
df = execute_query_df(
    "SELECT * FROM bbg.px WHERE ticker IN :tickers AND dt >= :start",
    params={"tickers": ["AAPL", "MSFT"], "start": "2024-01-01"}
)

print(f"Retrieved {len(df)} rows")
print(df.head())
```
