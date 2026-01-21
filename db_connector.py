# -*- coding: utf-8 -*-
"""
Database connection script for NFT RDS instances using SQLAlchemy
Creator: Zhiyi Lu
Create time: 2026-01-21
"""
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager
import resource


class DatabaseConnector:
    """Database connector for NFT RDS instances using SQLAlchemy"""

    _engines: Dict[str, Any] = {}
    _session_makers: Dict[str, sessionmaker] = {}

    @classmethod
    def _build_connection_url(cls, config: Dict[str, Any]) -> str:
        """Build SQLAlchemy connection URL from config dict"""
        return (
            f"postgresql+psycopg2://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['db']}"
        )

    @classmethod
    def get_engine(cls, config_type: str = "remote", pool_size: int = 5, max_overflow: int = 10):
        """
        Get or create SQLAlchemy engine

        Args:
            config_type: 'remote' or 'local'
            pool_size: Number of connections to maintain in the pool
            max_overflow: Additional connections allowed beyond pool_size

        Returns:
            SQLAlchemy Engine object
        """
        if config_type in cls._engines:
            return cls._engines[config_type]

        config = resource.NFT_RDS_CONFIG if config_type == "remote" else resource.NFT_RDS_CONFIG_LOCAL
        url = cls._build_connection_url(config)

        cls._engines[config_type] = create_engine(
            url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,  # Verify connections before using
            echo=False  # Set to True for SQL query logging
        )
        print(f"Engine created for {config_type} database")
        return cls._engines[config_type]

    @classmethod
    def get_session_maker(cls, config_type: str = "remote"):
        """
        Get or create a sessionmaker for the specified database

        Args:
            config_type: 'remote' or 'local'

        Returns:
            SQLAlchemy sessionmaker
        """
        if config_type in cls._session_makers:
            return cls._session_makers[config_type]

        engine = cls.get_engine(config_type)
        cls._session_makers[config_type] = sessionmaker(bind=engine)
        return cls._session_makers[config_type]

    @classmethod
    @contextmanager
    def get_session(cls, config_type: str = "remote"):
        """
        Context manager for SQLAlchemy session with automatic cleanup

        Args:
            config_type: 'remote' or 'local'

        Yields:
            SQLAlchemy Session object
        """
        session_maker = cls.get_session_maker(config_type)
        session = session_maker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @classmethod
    def get_engine_raw(cls, config_type: str = "remote"):
        """
        Get raw engine connection for Core-style queries

        Args:
            config_type: 'remote' or 'local'

        Returns:
            SQLAlchemy Engine object
        """
        return cls.get_engine(config_type)

    @classmethod
    def dispose_all_engines(cls):
        """Dispose all database engines and close connections"""
        for config_type, engine in cls._engines.items():
            engine.dispose()
            print(f"Engine for '{config_type}' disposed")
        cls._engines.clear()
        cls._session_makers.clear()

    @classmethod
    def test_connection(cls, config_type: str = "remote") -> bool:
        """
        Test database connection

        Args:
            config_type: 'remote' or 'local'

        Returns:
            True if connection successful, False otherwise
        """
        try:
            engine = cls.get_engine(config_type)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version();")).scalar()
                print(f"{config_type.upper()} connection successful!")
                print(f"PostgreSQL version: {result[:50]}...")
                return True
        except Exception as e:
            print(f"{config_type.upper()} connection failed: {e}")
            return False

    @classmethod
    def list_tables(cls, config_type: str = "remote") -> List[str]:
        """
        List all tables in the database

        Args:
            config_type: 'remote' or 'local'

        Returns:
            List of table names
        """
        engine = cls.get_engine(config_type)
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f"Tables in {config_type}: {tables}")
        return tables

    @classmethod
    def get_table_columns(cls, table_name: str, config_type: str = "remote") -> List[Dict[str, Any]]:
        """
        Get column information for a specific table

        Args:
            table_name: Name of the table
            config_type: 'remote' or 'local'

        Returns:
            List of column information dictionaries
        """
        engine = cls.get_engine(config_type)
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        return columns


def execute_query(query: str, params: Dict[str, Any] = None, config_type: str = "remote", fetch: str = "all"):
    """
    Execute a raw SQL query using SQLAlchemy Core

    Args:
        query: SQL query string
        params: Optional parameters for parameterized query
        config_type: 'remote' or 'local'
        fetch: 'all', 'one', or None - whether to fetch results

    Returns:
        Query results based on fetch parameter
    """
    engine = DatabaseConnector.get_engine(config_type)
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        if fetch == "all":
            return result.fetchall()
        elif fetch == "one":
            return result.fetchone()
        else:
            conn.commit()
            return None


def execute_query_session(query: str, params: Dict[str, Any] = None, config_type: str = "remote"):
    """
    Execute a SQL query using ORM session (useful for updates/deletes)

    Args:
        query: SQL query string
        params: Optional parameters for parameterized query
        config_type: 'remote' or 'local'

    Returns:
        Query results as list of tuples
    """
    with DatabaseConnector.get_session(config_type) as session:
        result = session.execute(text(query), params or {})
        session.commit()
        return result.fetchall()


def execute_query_df(query: str, params: Dict[str, Any] = None, config_type: str = "remote"):
    """
    Execute a SQL query and return results as a pandas DataFrame

    Args:
        query: SQL query string
        params: Optional parameters for parameterized query
        config_type: 'remote' or 'local'

    Returns:
        pandas DataFrame with query results
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas is required for this function. Install with: pip install pandas")

    engine = DatabaseConnector.get_engine(config_type)
    return pd.read_sql_query(text(query), engine, params=params)


# Example usage
if __name__ == "__main__":
    # Test local connection
    print("Testing LOCAL database connection...")
    DatabaseConnector.test_connection("local")

    print("\n" + "="*50 + "\n")

    # Test remote connection
    print("Testing REMOTE database connection...")
    DatabaseConnector.test_connection("remote")

    print("\n" + "="*50 + "\n")

    # Example query using context manager
    print("Running example query on REMOTE database...")
    try:
        result = execute_query("SELECT current_database(), current_user, current_timestamp;")
        for row in result:
            print(f"Database: {row[0]}, User: {row[1]}, Time: {row[2]}")
    except Exception as e:
        print(f"Query failed: {e}")

    print("\n" + "="*50 + "\n")

    # List tables example
    print("Listing tables in LOCAL database...")
    try:
        tables = DatabaseConnector.list_tables("local")
    except Exception as e:
        print(f"Failed to list tables: {e}")

    # Clean up
    DatabaseConnector.dispose_all_engines()
