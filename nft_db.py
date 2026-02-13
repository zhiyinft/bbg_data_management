# -*- coding: utf-8 -*-
"""
NFT Database Module

Single-file database module for NFT RDS instances.
Contains: connection_server, db_client, res, procs, presets

To use in another project:
    1. Copy this file (nft_db.py) to your project
    2. Import and use - server auto-starts on first query:
         from nft_db import db, rds, bbg, execute_query, execute_query_df

    OR start server manually:
         python nft_db.py

Components:
    - connection_server: Background server maintaining DB connections
    - db_client: Client for communicating with connection_server (auto-starts server)
    - res: Database classes (DB, SCHEMA, TB, ClientDB, ClientSchema, ClientTable)
    - procs: Database procedures
    - presets: Convenience functions (get_px, get_indeed)

Creator: Zhiyi Lu
Created: 2026-02-09
Merged: 2026-02-11
Updated: 2026-02-13 (added presets, auto-scan, server auto-start)

"""
from __future__ import annotations


# ===========================================================================
# Source file: connection_server.py
# ===========================================================================

"""
Persistent Connection Server for NFT RDS

This module runs as a background server that maintains database connections
and serves queries from client scripts via a local socket.

The server:
- Maintains persistent connection pools to the database
- Listens on a local Unix socket (Linux/Mac) or named pipe (Windows)
- Auto-disconnects after configurable idle timeout
- Handles multiple concurrent client requests

Creator: Zhiyi Lu
Create time: 2026-02-09
"""

import os
import sys
import time
import socket
import struct
import threading
import signal
import atexit
import json
from typing import Optional, Dict, Any
from loguru import logger
import traceback

import sqlalchemy
from sqlalchemy import create_engine, text
import psycopg2.extras


# Connection configurations
NFT_RDS_CONFIG_LOCAL = {
    "db_type": "postgres",
    "host": "localhost",
    "user": "postgres",
    "password": "nft2024",
    "port": 5432,
    "db": "nft",
}

NFT_RDS_CONFIG = {
    "db_type": "postgres",
    "host": "serving-data-instance-2-cluster.cluster-clf3qkbc2hx2.us-east-2.rds.amazonaws.com",
    "user": "postgres",
    "password": "t9noEyiW8ArYXLqr0c52",
    "port": 5432,
    "db": "nft",
}


# Server configuration
SERVER_HOST = "127.0.0.1"
DEFAULT_PORTS = {
    "remote": 15500,
    "local": 15501,
}
IDLE_TIMEOUT = 3600  # 1 hour default

# Log file for server activity
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db_logs")
LOG_FILE = os.path.join(LOG_DIR, "connection_server.log")


class ConnectionPool:
    """Manages a connection pool for a specific database configuration"""

    def __init__(
        self, config: Dict[str, Any], config_type: str, idle_timeout: int = 3600
    ):
        self.config = config
        self.config_type = config_type
        self.idle_timeout = idle_timeout
        self._engine: Optional[sqlalchemy.engine.Engine] = None
        self._last_used: Optional[float] = None
        self._lock = threading.RLock()
        self._shutdown = False

    def _create_connection_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.config['user']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['db']}"
        )

    def _create_engine(self) -> sqlalchemy.engine.Engine:
        logger.info(f"Creating new engine for {self.config_type}")
        return create_engine(
            self._create_connection_url(),
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=300,
            echo=False,
        )

    def get_engine(self) -> sqlalchemy.engine.Engine:
        with self._lock:
            if self._shutdown:
                raise RuntimeError(f"Pool for {self.config_type} has been shut down")

            # Check idle timeout
            if self._engine is not None and self._last_used is not None:
                idle_time = time.time() - self._last_used
                if idle_time > self.idle_timeout:
                    logger.info(
                        f"{self.config_type} idle for {idle_time:.1f}s, disconnecting"
                    )
                    self._engine.dispose()
                    self._engine = None
                    self._last_used = None

            if self._engine is None:
                self._engine = self._create_engine()

            self._last_used = time.time()
            return self._engine

    def execute(
        self, query: str, params: Dict[str, Any] = None, fetch: str = "all"
    ) -> Any:
        """Execute a query and return results"""
        engine = self.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})

            if fetch == "all":
                rows = result.fetchall()
                # Convert Row objects to dicts
                return [dict(row._mapping) for row in rows]
            elif fetch == "one":
                row = result.fetchone()
                return dict(row._mapping) if row else None
            else:
                conn.commit()
                return {"affected_rows": result.rowcount}

    def bulk_insert(self, table: str, columns: list, rows: list) -> Dict[str, Any]:
        """
        Fast bulk insert using psycopg2.extras.execute_values.

        Args:
            table: Full table name (schema.table)
            columns: List of column names
            rows: List of lists/tuples containing row values

        Returns:
            Dict with affected_rows count
        """
        engine = self.get_engine()

        # Get raw psycopg2 connection from SQLAlchemy engine
        conn = engine.raw_connection()
        try:
            cursor = conn.cursor()
            cols_str = ", ".join(columns)
            # Use execute_values for fast bulk insert
            psycopg2.extras.execute_values(
                cursor,
                f"INSERT INTO {table} ({cols_str}) VALUES %s",
                rows,
                page_size=1000,
            )
            conn.commit()
            return {"affected_rows": len(rows)}
        finally:
            conn.close()

    def disconnect(self):
        with self._lock:
            self._shutdown = True
            if self._engine is not None:
                self._engine.dispose()
                self._engine = None


class ClientInfo:
    """Track information about connected clients"""

    def __init__(self, address: tuple, connect_time: float):
        self.address = address
        self.connect_time = connect_time
        self.last_request = connect_time
        self.request_count = 0
        self.script_name = "unknown"

    def update(self, script_name: str = None):
        self.last_request = time.time()
        self.request_count += 1
        if script_name:
            self.script_name = script_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "address": f"{self.address[0]}:{self.address[1]}",
            "connected_at": self.connect_time,
            "connected_since": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(self.connect_time)
            ),
            "last_request": self.last_request,
            "last_request_time": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(self.last_request)
            ),
            "idle_time": time.time() - self.last_request,
            "request_count": self.request_count,
            "script_name": self.script_name,
        }


class ConnectionServer:
    """
    The connection server that listens for client requests.

    Server runs as a background process and:
    - Maintains connection pools to remote databases
    - Listens on TCP port for client requests
    - Executes queries and returns results
    - Auto-shutdowns after idle timeout
    - Logs all client connections and queries
    """

    def __init__(
        self,
        host: str = SERVER_HOST,
        port: int = None,
        idle_timeout: int = IDLE_TIMEOUT,
    ):
        self.host = host
        self.port = port or DEFAULT_PORTS["remote"]
        self.idle_timeout = idle_timeout
        self._server_socket: Optional[socket.socket] = None
        self._running = False
        self._shutdown_event = threading.Event()
        self._pools: Dict[str, ConnectionPool] = {}
        self._pools_lock = threading.RLock()
        self._last_request: Optional[float] = None
        self._start_time = time.time()

        # Client tracking
        self._clients: Dict[tuple, ClientInfo] = {}
        self._clients_lock = threading.RLock()
        self._total_requests = 0

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Setup logging to file
        self._setup_logging()

        logger.info(f"ConnectionServer initialized on {host}:{port}")

    def _setup_logging(self):
        """Setup logging to both console and file"""
        # Create log directory if it doesn't exist
        os.makedirs(LOG_DIR, exist_ok=True)

        # Remove default handler
        logger.remove()

        # Add console handler with simpler format
        logger.add(
            sys.stderr,
            level="INFO",
            format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        )

        # Add file handler with detailed format
        logger.add(
            LOG_FILE,
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
            rotation="100 MB",
            retention="7 days",
            encoding="utf-8",
        )

        logger.info(f"Logging to: {LOG_FILE}")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown()
        sys.exit(0)

    def get_pool(self, config_type: str) -> ConnectionPool:
        """Get or create a connection pool for the specified config"""
        with self._pools_lock:
            if config_type not in self._pools:
                config = (
                    NFT_RDS_CONFIG if config_type == "remote" else NFT_RDS_CONFIG_LOCAL
                )
                self._pools[config_type] = ConnectionPool(
                    config=config,
                    config_type=config_type,
                    idle_timeout=self.idle_timeout,
                )
            return self._pools[config_type]

    def _handle_client(self, client_socket: socket.socket, address: tuple):
        """Handle a client connection"""
        self._last_request = time.time()

        # Register client
        with self._clients_lock:
            client_info = ClientInfo(address, time.time())
            self._clients[address] = client_info

        logger.info(f"[CONNECT] Client connected from {address[0]}:{address[1]}")

        try:
            while self._running:
                # Receive message length (4 bytes)
                raw_length = self._recv_all(client_socket, 4)
                if not raw_length:
                    break

                length = struct.unpack(">I", raw_length)[0]

                # Receive the message data
                data = self._recv_all(client_socket, length)
                if not data:
                    break

                self._last_request = time.time()

                # Parse request
                try:
                    request = json.loads(data.decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.warning(f"[JSON_ERROR] Client {address}: {e}")
                    response = {"error": f"Invalid JSON: {e}"}
                else:
                    # Update client info
                    with self._clients_lock:
                        self._clients[address].update(request.get("script_name"))
                        self._total_requests += 1

                    # Log request
                    action = request.get("action", "unknown")
                    query_preview = (
                        request.get("query", "")[:100] if request.get("query") else ""
                    )
                    logger.info(
                        f'[REQUEST] Client {address[0]}:{address[1]} - action={action}, query="{query_preview}"'
                    )

                    response = self._process_request(request, address)

                    # Log response
                    if "error" in response:
                        logger.error(
                            f"[ERROR] Client {address[0]}:{address[1]} - {response['error']}"
                        )
                    else:
                        logger.debug(
                            f"[RESPONSE] Client {address[0]}:{address[1]} - success"
                        )

                # Send response
                response_data = json.dumps(response, default=str).encode("utf-8")
                response_length = struct.pack(">I", len(response_data))
                client_socket.sendall(response_length + response_data)

        except Exception as e:
            logger.error(f"[ERROR] Client {address}: {e}\n{traceback.format_exc()}")
        finally:
            client_socket.close()
            # Unregister client
            with self._clients_lock:
                if address in self._clients:
                    info = self._clients[address].to_dict()
                    del self._clients[address]
                    logger.info(
                        f"[DISCONNECT] Client {address[0]}:{address[1]} - requests={info['request_count']}, duration={time.time()-info['connected_at']:.1f}s"
                    )

    def _recv_all(self, sock: socket.socket, length: int) -> Optional[bytes]:
        """Receive exactly length bytes from socket"""
        data = bytearray()
        while len(data) < length:
            packet = sock.recv(length - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def _process_request(
        self, request: Dict[str, Any], address: tuple = None
    ) -> Dict[str, Any]:
        """Process a client request"""
        action = request.get("action")

        if action == "execute":
            return self._execute_query(request, address)
        elif action == "test":
            return self._test_connection(request, address)
        elif action == "status":
            return self._get_status()
        elif action == "disconnect":
            return self._disconnect(request)
        elif action == "ping":
            return {"status": "pong"}
        elif action == "logs":
            return self._get_logs(request)
        elif action == "bulk_insert":
            return self._bulk_insert(request, address)
        elif action == "shutdown":
            return self._shutdown_server(request)
        else:
            return {"error": f"Unknown action: {action}"}

    def _execute_query(
        self, request: Dict[str, Any], address: tuple = None
    ) -> Dict[str, Any]:
        """Execute a SQL query"""
        try:
            query = request.get("query")
            params = request.get("params")
            fetch = request.get("fetch", "all")
            config_type = request.get("config_type", "remote")

            if not query:
                return {"error": "Missing 'query' parameter"}

            pool = self.get_pool(config_type)
            result = pool.execute(query, params, fetch)

            return {
                "success": True,
                "data": result,
                "config_type": config_type,
            }
        except Exception as e:
            logger.error(f"[QUERY_ERROR] {e}")
            return {"error": str(e)}

    def _test_connection(
        self, request: Dict[str, Any], address: tuple = None
    ) -> Dict[str, Any]:
        """Test database connection"""
        try:
            config_type = request.get("config_type", "remote")
            pool = self.get_pool(config_type)
            result = pool.execute("SELECT version();", fetch="one")
            return {
                "success": True,
                "version": result.get("version", "")[:50] if result else None,
            }
        except Exception as e:
            return {"error": str(e)}

    def _get_status(self) -> Dict[str, Any]:
        """Get server status"""
        with self._pools_lock:
            pools_status = {}
            for name, pool in self._pools.items():
                pools_status[name] = {
                    "connected": pool._engine is not None,
                    "idle_time": (
                        time.time() - pool._last_used if pool._last_used else None
                    ),
                }

        with self._clients_lock:
            clients_list = [info.to_dict() for info in self._clients.values()]

        return {
            "running": self._running,
            "port": self.port,
            "host": self.host,
            "idle_timeout": self.idle_timeout,
            "last_request": self._last_request,
            "uptime": time.time() - self._start_time,
            "total_requests": self._total_requests,
            "active_clients": len(self._clients),
            "clients": clients_list,
            "pools": pools_status,
            "log_file": LOG_FILE,
        }

    def _get_logs(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Get recent log entries"""
        try:
            lines = request.get("lines", 50)
            log_type = request.get("type", "recent")  # "recent" or "all"

            if not os.path.exists(LOG_FILE):
                return {"error": "Log file not found", "log_file": LOG_FILE}

            with open(LOG_FILE, "r", encoding="utf-8") as f:
                if log_type == "recent":
                    # Get last N lines
                    all_lines = f.readlines()
                    log_lines = (
                        all_lines[-lines:] if len(all_lines) > lines else all_lines
                    )
                else:
                    log_lines = f.readlines()

            return {
                "success": True,
                "log_file": LOG_FILE,
                "lines": len(log_lines),
                "logs": "".join(log_lines),
            }
        except Exception as e:
            return {"error": str(e)}

    def _disconnect(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Disconnect a specific pool"""
        config_type = request.get("config_type")
        with self._pools_lock:
            if config_type:
                if config_type in self._pools:
                    self._pools[config_type].disconnect()
                    return {"success": True, "disconnected": config_type}
            else:
                for pool in self._pools.values():
                    pool.disconnect()
                return {"success": True, "disconnected": "all"}
        return {"error": "Pool not found"}

    def _bulk_insert(
        self, request: Dict[str, Any], address: tuple = None
    ) -> Dict[str, Any]:
        """Fast bulk insert using psycopg2 execute_values"""
        try:
            table = request.get("table")
            columns = request.get("columns")
            rows = request.get("rows")
            config_type = request.get("config_type", "remote")

            if not table or not columns or rows is None:
                return {"error": "Missing required parameters: table, columns, rows"}

            pool = self.get_pool(config_type)
            result = pool.bulk_insert(table, columns, rows)

            return {
                "success": True,
                "data": result,
            }
        except Exception as e:
            logger.error(f"[BULK_INSERT_ERROR] {e}")
            return {"error": str(e)}

    def _shutdown_server(self, request: Dict[str, Any] = None) -> Dict[str, Any]:
        """Shutdown the server gracefully"""
        logger.info("[SHUTDOWN] Server shutdown requested by client")
        self.shutdown()
        return {"success": True, "message": "Server shutting down"}

    def _idle_monitor(self):
        """Background thread to monitor idle time and auto-shutdown"""
        while not self._shutdown_event.is_set():
            try:
                if self._last_request is not None:
                    idle = time.time() - self._last_request
                    if idle > self.idle_timeout and self._running:
                        logger.info(f"Server idle for {idle:.1f}s, shutting down...")
                        self.shutdown()
                        break
            except Exception as e:
                logger.error(f"Error in idle monitor: {e}")

            # Check every 30 seconds
            self._shutdown_event.wait(30)

    def start(self):
        """Start the connection server"""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(5)
        self._server_socket.settimeout(1.0)  # Allow periodic checking

        self._running = True
        self._last_request = time.time()

        # Start idle monitor thread
        monitor_thread = threading.Thread(target=self._idle_monitor, daemon=True)
        monitor_thread.start()

        logger.info(f"Connection server started on {self.host}:{self.port}")
        logger.info(f"Auto-shutdown after {self.idle_timeout}s of inactivity")
        logger.info(f"Log file: {LOG_FILE}")
        self._log_status("startup")

        try:
            while self._running:
                try:
                    client_socket, address = self._server_socket.accept()
                    # Handle each client in a new thread
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address),
                        daemon=True,
                    )
                    client_thread.start()
                except socket.timeout:
                    continue
                except OSError:
                    if self._running:
                        logger.error("Socket error, shutting down...")
                    break
        finally:
            self.shutdown()

    def shutdown(self):
        """Shutdown the server"""
        logger.info("Shutting down connection server...")
        self._log_status("shutdown")
        self._running = False
        self._shutdown_event.set()

        # Close server socket
        if self._server_socket:
            try:
                self._server_socket.close()
            except:
                pass

        # Shutdown all pools
        with self._pools_lock:
            for pool in self._pools.values():
                pool.disconnect()
            self._pools.clear()

    def _log_status(self, event: str = "status"):
        """Log current server status"""
        with self._clients_lock:
            clients_info = [
                f"{info.address[0]}:{info.address[1]} ({info.script_name}, {info.request_count} requests)"
                for info in self._clients.values()
            ]

        logger.info(
            f"[{event.upper()}] uptime={time.time()-self._start_time:.1f}s, "
            f"total_requests={self._total_requests}, "
            f"active_clients={len(self._clients)}, "
            f"clients={clients_info if clients_info else 'none'}"
        )


def run_server(
    host: str = SERVER_HOST,
    port: int = None,
    idle_timeout: int = IDLE_TIMEOUT,
    daemon: bool = False,
):
    """
    Run the connection server.

    Args:
        host: Server host address
        port: Server port (default: 15500 for remote, 15501 for local)
        idle_timeout: Auto-shutdown after this many seconds of inactivity
        daemon: If True, run as a background process
    """
    if port is None:
        port = DEFAULT_PORTS["remote"]

    if daemon:
        # Run as daemon (fork to background)
        try:
            pid = os.fork()
            if pid > 0:
                # Parent process
                print(f"Connection server started in background (PID: {pid})")
                print(f"Listening on {host}:{port}")
                return
        except AttributeError:
            # Windows doesn't have fork()
            # For Windows, the server should be run manually or as a service
            logger.warning(
                "Daemon mode not supported on Windows, running in foreground"
            )

    server = ConnectionServer(host=host, port=port, idle_timeout=idle_timeout)
    server.start()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NFT RDS Connection Server")
    parser.add_argument(
        "--host",
        type=str,
        default=SERVER_HOST,
        help=f"Server host (default: {SERVER_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORTS["remote"],
        help=f"Server port (default: {DEFAULT_PORTS['remote']} for remote)",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=IDLE_TIMEOUT,
        help=f"Idle timeout in seconds (default: {IDLE_TIMEOUT})",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run local database server (uses different port)",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run as background daemon (Linux/Mac only)",
    )

    args = parser.parse_args()

    if args.local:
        args.port = DEFAULT_PORTS["local"]

    # Setup logging
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    )

    run_server(
        host=args.host,
        port=args.port,
        idle_timeout=args.idle_timeout,
        daemon=args.daemon,
    )


# ===========================================================================
# Source file: db_client.py
# ===========================================================================

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
    """Get the path to this module (contains the server code)"""
    # Get the directory containing this module
    module_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(__file__)


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
            cmd = ["python", _get_server_path(), "--port", str(self.port)]

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


def get_client(
    config_type: str = "remote", auto_start: bool = True
) -> ConnectionClient:
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


# # __main__ block removed from merged file

# ===========================================================================
# Source file: res.py
# ===========================================================================

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


# # MERGED: from .db_client import get_client  # db_client defined above
# # MERGED: from .connection_server import NFT_RDS_CONFIG_LOCAL  # connection_server defined above


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
                for col in [self.dt_col, "update_time", "created_at", "updated_at"]
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

# # __main__ block removed from merged file

# ===========================================================================
# Source file: procs.py
# ===========================================================================

"""
Creator: Zhiyi Lu
Create time: 2026-02-09 13:35
Description: Database procedures
"""
import sqlalchemy as sa

from typing import Union
from loguru import logger

# # MERGED: from .res import TB, ClientTable  # res defined above


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


# ===========================================================================
# Source file: presets.py
# ===========================================================================

"""
Creator: Zhiyi Lu
Create time: 2026-02-13 11:20
Description: wrappers and presets for ease of use
"""
# # MERGED: from .res import rds, bbg  # dependencies defined above
# # MERGED: from .db_client import get_client  # dependencies defined above

c = get_client()


def get_px(ticker, start=None, raw=False):
    df = (
        bbg.px.fields(["dt", "orig_close", "adj_fac_px"])
        .symbol(ticker)
        .on_after(start)
        .get(force_raw=raw)
    )
    if not df.empty:
        df = df.set_index("dt").sort_index()
        df["ticker"] = ticker
        if raw:
            df = df[["ticker", "orig_close"]]
        else:
            df = df[["ticker", "close"]]
    return df


def get_indeed(company, start=None):
    s = rds.scraping.summary_count.fields(
        [
            "raw_date",
            "identifier",
            "count",
        ]
    ).on_after(start)
    if company is not None:
        s = s.symbol(company)

    df = s.get()
    if not df.empty:
        df = df.set_index(s.dt_col).sort_index()
    return df
