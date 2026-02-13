# -*- coding: utf-8 -*-
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

    def bulk_insert(
        self, table: str, columns: list, rows: list
    ) -> Dict[str, Any]:
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

    def _shutdown_server(
        self, request: Dict[str, Any] = None
    ) -> Dict[str, Any]:
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
