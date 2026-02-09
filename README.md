# NFT Bloomberg Data Management

Database management tools for NFT Bloomberg data with persistent connection server.

## Module Structure

```
bbg_data_management/
├── db/
│   ├── __init__.py           # Module exports
│   ├── res.py                # Core database classes (ClientDB, ClientSchema, ClientTable, ClientS)
│   ├── connection_server.py  # Persistent connection server
│   ├── db_client.py          # Client for connecting to server
│   ├── procs.py              # Database procedures
│   └── db_logs/              # Server logs
├── db_connector.py           # Legacy (moved to db/res.py)
├── ddl.py                    # DDL scripts
├── example_client_usage.py   # Usage examples
└── README.md                 # This file
```

## Features

- **Persistent Connection Server**: Maintains database connections across script runs
- **Auto-start**: Server starts automatically when first client connects
- **Shared Connections**: Multiple scripts share the same connection pool
- **Auto-shutdown**: Server shuts down after 1 hour of inactivity
- **Reflection**: Auto-discover tables in schemas
- **Bulk Operations**: Fast bulk insert with batch_size=1000

## Quick Start

### Basic Usage

```python
from db import bbg, rds

# Query using the fluent API
df = bbg.securities.get().df()
df = bbg.securities.select("ticker", "name").get().df()

# Query with filters
df = bbg.security_prices.filter(ticker="AAPL").get().df()

# Insert data (bulk mode)
df_to_insert = pd.DataFrame({"ticker": ["AAPL", "MSFT"], "price": [150, 250]})
bbg.securities.insert(df_to_insert)
```

### Direct Query Execution

```python
from db import execute_query, execute_query_df

# Simple query
results = execute_query("SELECT * FROM securities WHERE ticker = :ticker", params={"ticker": "AAPL"})

# Query to DataFrame
df = execute_query_df("SELECT * FROM security_prices LIMIT 10")
```

### Server Status

```python
from db import test_connection, print_status

test_connection("remote")  # Test database connection
print_status()             # Show server status
```

## API Reference

### Database Instances

- `db` - Local database (SQLAlchemy, localhost:5432)
- `rds` - Remote RDS database (connection_server)
- `bbg` - Bloomberg schema on RDS (auto-reflected)

### Classes

- `ClientDB` - Database wrapper using connection_server
- `ClientSchema` - Schema with table reflection
- `ClientTable` - Table with insert and query capabilities
- `ClientS` - Query builder (like SQLAlchemy's Select)

### Procedures

- `del_ticker_after(tb, ticker=None, dt=None, **kwargs)` - Delete rows from table by conditions

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

```bash
# Start server manually
python -m db.connection_server --port 15500

# Start local database server
python -m db.connection_server --local

# Custom idle timeout
python -m db.connection_server --idle-timeout 1800
```

## Troubleshooting

See logs in `db/db_logs/connection_server.log`.
