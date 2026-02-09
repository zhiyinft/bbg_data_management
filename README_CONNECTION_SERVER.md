# Connection Server for NFT RDS

A persistent connection server that maintains database connections and serves queries from client scripts via TCP, eliminating the overhead of reconnecting to RDS for each script execution.

## Architecture

```
┌─────────────┐      ┌──────────────────┐      ┌─────────────┐
│ script1.py  │─────▶│ Connection Server│─────▶│   RDS DB    │
├─────────────┤      │  (background     │      └─────────────┘
│ script2.py  │─────▶│   process)       │
├─────────────┤      │ Port: 15500      │
│ script3.py  │─────▶│ Idle timeout: 1h │
└─────────────┘      └──────────────────┘
```

## Features

- **Auto-start**: Server automatically starts when first client connects
- **Connection reuse**: All scripts share the same database connection
- **Fast queries**: No reconnection overhead after first use
- **Auto-shutdown**: Server shuts down after 1 hour of inactivity (configurable)
- **Multiple databases**: Supports both remote and local database configurations
- **Thread-safe**: Handles multiple concurrent requests

## Files

- `connection_server.py` - The server that runs in the background
- `db_client.py` - Client library for your scripts
- `example_client_usage.py` - Usage examples

## Quick Start

### 1. In Your Scripts

Simply import and use the `db_client` module:

```python
from db_client import execute_query, execute_query_df

# Execute a query
result = execute_query(
    "SELECT * FROM users WHERE id = :id",
    params={"id": 1},
    fetch="one"
)
print(result)

# Query to DataFrame
df = execute_query_df("SELECT * FROM products LIMIT 10")
print(df.head())
```

### 2. The server will auto-start

The first time you run a script, the connection server will automatically start in the background. Subsequent scripts will connect to the already-running server.

### 3. Test the connection

```python
from db_client import test_connection, print_status

test_connection("remote")  # Test remote database
print_status()             # Show server status
```

## Manual Server Control

If you want to run the server manually (e.g., as a service):

```bash
# Start server for remote database (port 15500)
python connection_server.py --port 15500

# Start server for local database (port 15501)
python connection_server.py --local

# Custom idle timeout (seconds)
python connection_server.py --idle-timeout 1800  # 30 minutes
```

## API Reference

### execute_query(query, params=None, config_type="remote", fetch="all")

Execute a SQL query and return results.

- **query**: SQL query string (use `:param_name` for parameters)
- **params**: Dictionary of parameters (for parameterized queries)
- **config_type**: "remote" or "local"
- **fetch**: "all" (returns list of dicts), "one" (returns single dict), or None

```python
# Fetch all rows
results = execute_query("SELECT * FROM users")

# Fetch one row
user = execute_query("SELECT * FROM users WHERE id = :id", params={"id": 1}, fetch="one")

# With parameters
results = execute_query(
    "SELECT * FROM orders WHERE date >= :start_date",
    params={"start_date": "2024-01-01"}
)
```

### execute_query_df(query, params=None, config_type="remote")

Execute a query and return a pandas DataFrame.

```python
df = execute_query_df("SELECT * FROM products WHERE price > :min_price", params={"min_price": 100})
```

### test_connection(config_type="remote")

Test the database connection.

```python
success = test_connection("remote")
```

### print_status()

Print the connection server status.

```python
print_status()
# Output:
# Connection Server Status
# ============================================================
# Running: True
# Port: 15500
# Idle Timeout: 3600s
# Time Since Last Request: 5.2s
# Connection Pools:
#   remote:
#     Connected: True
#     Idle: 5.2s
# ============================================================
```

### disconnect_server(config_type=None)

Disconnect the server's database connection.

```python
# Disconnect remote only
disconnect_server("remote")

# Disconnect all
disconnect_server()
```

## Configuration

### Server Configuration

Edit these in `connection_server.py`:

```python
SERVER_HOST = "127.0.0.1"      # Server bind address
DEFAULT_PORTS = {
    "remote": 15500,            # Port for remote database
    "local": 15501,             # Port for local database
}
IDLE_TIMEOUT = 3600             # Auto-shutdown after 1 hour
```

### Database Configuration

Edit these in either `connection_server.py` or `db_client.py`:

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

## Performance Comparison

| Method | First Query | Subsequent Queries |
|--------|-------------|-------------------|
| Direct connection (each script) | ~1500ms | ~1500ms |
| Connection server | ~1500ms | ~5-20ms |

The connection server eliminates the RDS handshake overhead for all scripts after the first one.

## Troubleshooting

### Port already in use

If you get "Port already in use", it means the server is already running. This is normal - just use the client as usual.

### Server not starting

Check that:
1. Python and required packages are installed (`sqlalchemy`, `psycopg2`, `loguru`)
2. The port is not blocked by a firewall
3. Database credentials are correct

### Force restart

To force restart the server:

```bash
# Kill existing server (Linux/Mac)
pkill -f connection_server.py

# Windows - find and kill the process
tasklist | findstr python
taskkill /PID <pid> /F
```

## Windows Notes

On Windows, the server runs in the same terminal (no daemon mode). To run it in the background:

1. Use `pythonw connection_server.py` to run without a console window
2. Run as a Windows Service (advanced setup)
