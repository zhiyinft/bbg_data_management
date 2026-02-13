# -*- coding: utf-8 -*-
"""
Client for the Persistent Connection Server

This module provides a simple API for connecting to the connection server
and executing queries without managing database connections directly.

Usage:
    from db import db_client

    # Execute a simple query
    result = db_client.execute_query("SELECT * FROM table WHERE id = :id", params={"id": 1})

    # Query directly to DataFrame
    df = db_client.execute_query_df("SELECT * FROM table")

Creator: Zhiyi Lu
Create time: 2026-02-09
"""

import os
import sys
import socket
import struct
import json
import time
from typing import Optional, Dict, Any, List
from loguru import logger
import subprocess


# Server configuration
SERVER_HOST = "127.0.0.1"
DEFAULT_PORTS = {
    "remote": 15500,
    "local": 15501,
}
CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 30.0


def _get_script_name() -> str:
    """Get the name of the currently running script"""
    return os.path.basename(sys.argv[0]) if len(sys.argv) > 0 else "unknown"


def _get_server_path() -> str:
    """Get the path to the connection_server module"""
    # Get the directory containing this module
    module_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(module_dir, "connection_server.py")


class ConnectionClient:
    """
    Client for communicating with the connection server.

    This class handles:
    - Connecting to the server
    - Sending requests
    - Receiving responses
    - Auto-retrying on connection failures
    """

    def __init__(
        self,
        host: str = SERVER_HOST,
        port: int = None,
        config_type: str = "remote",
        auto_start: bool = True,
    ):
        """
        Initialize the client.

        Args:
            host: Server host address
            port: Server port (default based on config_type)
            config_type: 'remote' or 'local'
            auto_start: If True, auto-start the server if not running
        """
        self.host = host
        self.port = port or DEFAULT_PORTS.get(config_type, DEFAULT_PORTS["remote"])
        self.config_type = config_type
        self.auto_start = auto_start
        self._socket: Optional[socket.socket] = None

    def connect(self) -> bool:
        """Connect to the server, optionally starting it if not running"""
        if self._is_server_running():
            return True

        if self.auto_start:
            logger.info(f"Connection server not running, attempting to start...")
            if self._start_server():
                # Wait for server to start
                for _ in range(50):  # Wait up to 5 seconds
                    time.sleep(0.1)
                    if self._is_server_running():
                        logger.info("Connection server started successfully")
                        return True
                logger.error("Failed to connect to server after auto-start")
                return False
            return False

        return False

    def _is_server_running(self) -> bool:
        """Check if the server is running"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect((self.host, self.port))
            sock.close()
            return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False

    def _start_server(self) -> bool:
        """Attempt to start the connection server"""
        try:
            server_path = _get_server_path()

            # Build the command
            cmd = ["python", server_path, "--port", str(self.port)]

            # Start the server as a subprocess
            if os.name == "nt":  # Windows
                # On Windows, use CREATE_NO_WINDOW to hide console
                # Note: Don't use DETACHED_PROCESS as it can cause issues
                subprocess.Popen(
                    cmd,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                    close_fds=False,  # Keep fds open for proper inheritance
                )
            else:  # Linux/Mac
                # On Unix, use start_new_session to detach from parent
                subprocess.Popen(
                    cmd,
                    start_new_session=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            return True
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            return False

    def _send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request to the server and get the response"""
        # Convert request to JSON
        data = json.dumps(request, default=str).encode("utf-8")
        length = struct.pack(">I", len(data))

        # Create new connection for each request
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONNECT_TIMEOUT)
        try:
            sock.connect((self.host, self.port))
            sock.settimeout(READ_TIMEOUT)

            # Send request
            sock.sendall(length + data)

            # Receive response length
            raw_length = self._recv_all(sock, 4)
            if not raw_length:
                raise ConnectionError("No response from server")

            response_length = struct.unpack(">I", raw_length)[0]

            # Receive response data
            response_data = self._recv_all(sock, response_length)
            if not response_data:
                raise ConnectionError("Incomplete response from server")

            return json.loads(response_data.decode("utf-8"))
        finally:
            sock.close()

    def _recv_all(self, sock: socket.socket, length: int) -> Optional[bytes]:
        """Receive exactly length bytes from socket"""
        data = bytearray()
        while len(data) < length:
            packet = sock.recv(length - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def execute(
        self,
        query: str,
        params: Dict[str, Any] = None,
        fetch: str = "all",
        config_type: str = None,
    ) -> Dict[str, Any]:
        """
        Execute a SQL query.

        Args:
            query: SQL query string
            params: Query parameters
            fetch: 'all', 'one', or None
            config_type: 'remote' or 'local' (overrides instance setting)

        Returns:
            Dictionary with 'success' and 'data' or 'error' keys
        """
        if not self.connect():
            return {"error": "Failed to connect to server"}

        request = {
            "action": "execute",
            "query": query,
            "params": params or {},
            "fetch": fetch,
            "config_type": config_type or self.config_type,
            "script_name": _get_script_name(),
        }

        try:
            return self._send_request(request)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return {"error": str(e)}

    def test(self, config_type: str = None) -> Dict[str, Any]:
        """Test the database connection"""
        if not self.connect():
            return {"error": "Failed to connect to server"}

        request = {
            "action": "test",
            "config_type": config_type or self.config_type,
        }

        try:
            return self._send_request(request)
        except Exception as e:
            return {"error": str(e)}

    def status(self) -> Dict[str, Any]:
        """Get server status"""
        if not self.connect():
            return {"error": "Failed to connect to server"}

        try:
            return self._send_request({"action": "status"})
        except Exception as e:
            return {"error": str(e)}

    def disconnect(self, config_type: str = None) -> Dict[str, Any]:
        """Disconnect the server's database connection"""
        if not self.connect():
            return {"error": "Failed to connect to server"}

        request = {"action": "disconnect"}
        if config_type:
            request["config_type"] = config_type

        try:
            return self._send_request(request)
        except Exception as e:
            return {"error": str(e)}

    def logs(self, lines: int = 50, log_type: str = "recent") -> Dict[str, Any]:
        """Get server logs"""
        if not self.connect():
            return {"error": "Failed to connect to server"}

        request = {
            "action": "logs",
            "lines": lines,
            "type": log_type,
        }

        try:
            return self._send_request(request)
        except Exception as e:
            return {"error": str(e)}

    def bulk_insert(
        self,
        table: str,
        columns: list,
        rows: list,
        config_type: str = None,
    ) -> Dict[str, Any]:
        """
        Fast bulk insert using psycopg2 execute_values.

        Args:
            table: Full table name (schema.table)
            columns: List of column names
            rows: List of lists/tuples containing row values
            config_type: 'remote' or 'local' (overrides instance setting)

        Returns:
            Dictionary with 'success' and 'data' or 'error' keys
        """
        if not self.connect():
            return {"error": "Failed to connect to server"}

        request = {
            "action": "bulk_insert",
            "table": table,
            "columns": columns,
            "rows": rows,
            "config_type": config_type or self.config_type,
        }

        try:
            return self._send_request(request)
        except Exception as e:
            return {"error": str(e)}

    def shutdown(self) -> Dict[str, Any]:
        """Shutdown the server"""
        if not self.connect():
            return {"error": "Failed to connect to server"}

        request = {"action": "shutdown"}

        try:
            return self._send_request(request)
        except Exception as e:
            return {"error": str(e)}


# Default clients for convenience
_default_clients: Dict[str, ConnectionClient] = {}


def get_client(config_type: str = "remote", auto_start: bool = True) -> ConnectionClient:
    """Get or create a client for the specified config type"""
    if config_type not in _default_clients:
        _default_clients[config_type] = ConnectionClient(
            config_type=config_type,
            auto_start=auto_start,
        )
    return _default_clients[config_type]


def execute_query(
    query: str,
    params: Dict[str, Any] = None,
    config_type: str = "remote",
    fetch: str = "all",
) -> Any:
    """
    Execute a SQL query.

    Args:
        query: SQL query string
        params: Query parameters
        config_type: 'remote' or 'local'
        fetch: 'all', 'one', or None

    Returns:
        Query results (list of dicts for 'all', single dict for 'one')

    Raises:
        ConnectionError: If server is not available
        Exception: For query errors

    Example:
        >>> results = execute_query("SELECT * FROM users WHERE id = :id", params={"id": 1})
        >>> for row in results:
        ...     print(row['name'])
    """
    client = get_client(config_type)
    response = client.execute(query, params, fetch)

    if "error" in response:
        raise Exception(f"Query error: {response['error']}")

    return response.get("data")


def execute_query_df(
    query: str,
    params: Dict[str, Any] = None,
    config_type: str = "remote",
):
    """
    Execute a SQL query and return results as a pandas DataFrame.

    Args:
        query: SQL query string
        params: Query parameters
        config_type: 'remote' or 'local'

    Returns:
        pandas DataFrame

    Example:
        >>> df = execute_query_df("SELECT * FROM users")
        >>> print(df.head())
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError(
            "pandas is required for this function. Install with: pip install pandas"
        )

    data = execute_query(query, params, config_type, fetch="all")

    if isinstance(data, list) and len(data) > 0:
        return pd.DataFrame(data)
    elif isinstance(data, list):
        return pd.DataFrame()
    else:
        raise ValueError(f"Unexpected data format: {type(data)}")


def test_connection(config_type: str = "remote") -> bool:
    """
    Test the database connection.

    Args:
        config_type: 'remote' or 'local'

    Returns:
        True if connection successful, False otherwise
    """
    client = get_client(config_type)
    response = client.test()

    if "error" in response:
        print(f"{config_type.upper()} connection failed: {response['error']}")
        return False

    print(f"{config_type.upper()} connection successful!")
    if "version" in response:
        print(f"PostgreSQL version: {response['version'][:50]}...")
    return True


def print_status():
    """Print the connection server status"""
    client = get_client()
    response = client.status()

    if "error" in response:
        print(f"Error getting status: {response['error']}")
        return

    print("\n" + "=" * 60)
    print("Connection Server Status")
    print("=" * 60)
    print(f"Running: {response.get('running')}")
    print(f"Address: {response.get('host')}:{response.get('port')}")
    print(f"Idle Timeout: {response.get('idle_timeout')}s")

    if "uptime" in response:
        uptime = response.get("uptime", 0)
        print(f"Uptime: {uptime:.1f}s ({uptime/60:.1f} minutes)")

    if "total_requests" in response:
        print(f"Total Requests: {response.get('total_requests')}")

    if "last_request" in response and response["last_request"]:
        idle = time.time() - response["last_request"]
        print(f"Time Since Last Request: {idle:.1f}s")

    if "active_clients" in response:
        print(f"Active Clients: {response.get('active_clients')}")

    # Show connected clients
    if "clients" in response and response["clients"]:
        print("\nConnected Clients:")
        for client_info in response["clients"]:
            print(f"  - {client_info['address']}")
            print(f"    Script: {client_info['script_name']}")
            print(f"    Connected: {client_info['connected_since']}")
            print(f"    Requests: {client_info['request_count']}")
            print(f"    Idle: {client_info['idle_time']:.1f}s")

    # Show connection pools
    if "pools" in response:
        print("\nConnection Pools:")
        for name, pool_info in response["pools"].items():
            print(f"  {name}:")
            print(f"    Connected: {pool_info.get('connected')}")
            if pool_info.get("idle_time"):
                print(f"    Idle: {pool_info['idle_time']:.1f}s")

    # Show log file location
    if "log_file" in response:
        print(f"\nLog File: {response.get('log_file')}")

    print("=" * 60 + "\n")


def print_logs(lines: int = 50):
    """Print recent server logs"""
    client = get_client()
    response = client.logs(lines=lines)

    if "error" in response:
        print(f"Error getting logs: {response['error']}")
        return

    print("\n" + "=" * 60)
    print(f"Recent Server Logs (last {lines} lines)")
    print("=" * 60)
    print(response.get("logs", ""))
    print("=" * 60 + "\n")


def get_logs(lines: int = 50) -> str:
    """Get recent server logs as a string"""
    client = get_client()
    response = client.logs(lines=lines)

    if "error" in response:
        return f"Error: {response['error']}"

    return response.get("logs", "")


def disconnect_server(config_type: str = None):
    """
    Disconnect the server's database connection.

    Args:
        config_type: 'remote', 'local', or None for all
    """
    client = get_client()
    response = client.disconnect(config_type)

    if "error" in response:
        print(f"Error disconnecting: {response['error']}")
    else:
        print(f"Disconnected: {response.get('disconnected')}")


def shutdown_server():
    """
    Shutdown the connection server gracefully.

    This will stop the server process. Any future queries will
    auto-start a new server instance.
    """
    client = get_client()
    response = client.shutdown()

    if "error" in response:
        print(f"Error shutting down server: {response['error']}")
    else:
        print(f"Server shutdown: {response.get('message', 'Shutdown complete')}")


def restart_server():
    """
    Restart the connection server.

    This shuts down the current server and starts a new one.
    Useful when server code has been updated.
    """
    print("Restarting connection server...")

    # First try to shutdown existing server
    client = get_client()
    response = client.shutdown()

    if "error" not in response:
        print("Server shutdown successfully")
    else:
        # Server might not be running, that's ok
        print(f"No existing server or shutdown completed: {response.get('error', '')}")

    # Wait a moment for port to be released
    time.sleep(0.5)

    # Clear the client cache so a new one will be created
    _default_clients.clear()

    # Start new server by creating a new client
    new_client = ConnectionClient(auto_start=True)
    _default_clients["remote"] = new_client

    # Wait for server to start
    for i in range(50):
        time.sleep(0.1)
        if new_client._is_server_running():
            print("Server restarted successfully")
            return

    print("Failed to restart server - please check logs")


if __name__ == "__main__":
    # Test the client
    import os

    print("Testing Connection Client")
    print("=" * 60)

    # Test remote connection
    print("\n1. Testing remote connection...")
    test_connection("remote")

    # Execute a query
    print("\n2. Executing a query...")
    result = execute_query(
        "SELECT current_database(), current_user, current_timestamp;",
        config_type="remote",
        fetch="one"
    )
    print(f"Database: {result.get('current_database')}")
    print(f"User: {result.get('current_user')}")

    # Print status
    print_status()

    # Test DataFrame query
    print("\n3. Testing DataFrame query...")
    try:
        df = execute_query_df(
            "SELECT * FROM information_schema.tables LIMIT 5",
            config_type="remote"
        )
        print(f"Retrieved {len(df)} rows")
        print(df[["table_schema", "table_name"]].to_string())
    except ImportError:
        print("pandas not installed, skipping DataFrame test")

    print("\n" + "=" * 60)
    print("Tests completed!")
    print("=" * 60)
