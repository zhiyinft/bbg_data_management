# -*- coding: utf-8 -*-
"""
Example usage of the connection server and client.

This demonstrates how to use the db_client module in your scripts.

The connection server will be automatically started when you first run a script
that uses the db_client. Subsequent scripts will reuse the running server.

Creator: Zhiyi Lu
Create time: 2026-02-09
"""

from db import execute_query, execute_query_df, print_status
import time


def example_1_simple_query():
    """Example 1: Execute a simple SQL query"""
    print("\n=== Example 1: Simple Query ===")
    print("Note: First call will auto-start the connection server (~1-2 seconds)")

    start = time.time()
    result = execute_query(
        "SELECT current_database(), current_user, current_timestamp;",
        config_type="remote",
        fetch="one",
    )
    elapsed = time.time() - start
    print(f"Query completed in {elapsed*1000:.2f}ms")
    print(f"  Database: {result.get('current_database')}")
    print(f"  User: {result.get('current_user')}")
    print(f"  Time: {result.get('current_timestamp')}")


def example_2_parameterized_query():
    """Example 2: Query with parameters"""
    print("\n=== Example 2: Parameterized Query ===")

    # Query using information_schema (works on any database)
    df = execute_query_df(
        "SELECT table_schema, table_name FROM information_schema.tables LIMIT 5;",
        config_type="remote",
    )
    print(f"Found {len(df)} tables:")
    for _, row in df.iterrows():
        print(f"  {row['table_schema']}.{row['table_name']}")


def example_3_multiple_queries():
    """Example 3: Multiple queries reusing the connection"""
    print("\n=== Example 3: Multiple Queries (Fast Reuse) ===")

    queries = [
        "SELECT 1 as test1;",
        "SELECT 2 as test2;",
        "SELECT 3 as test3;",
    ]

    for i, query in enumerate(queries, 1):
        start = time.time()
        result = execute_query(query, config_type="remote", fetch="one")
        elapsed = time.time() - start
        print(f"Query {i}: {elapsed*1000:.2f}ms - Result: {result}")


def example_4_server_status():
    """Example 4: Check server status"""
    print("\n=== Example 4: Server Status ===")
    print_status()


def example_5_connection_reuse_demo():
    """Example 5: Demonstrating connection reuse across multiple function calls"""
    print("\n=== Example 5: Connection Reuse Across Functions ===")

    # These functions can be called from different parts of your code
    # They will all use the same server connection
    fetch_user_count()
    fetch_table_count()
    fetch_schema_count()


def fetch_user_count():
    """Simulates fetching user count from a users table"""
    start = time.time()
    result = execute_query(
        "SELECT count(*) as cnt FROM pg_user WHERE usename = current_user;",
        config_type="remote",
        fetch="one",
    )
    elapsed = time.time() - start
    print(
        f"User check: {elapsed*1000:.2f}ms - Current user exists: {result.get('cnt') > 0}"
    )


def fetch_table_count():
    """Simulates fetching table count"""
    start = time.time()
    result = execute_query(
        "SELECT count(*) as cnt FROM information_schema.tables;",
        config_type="remote",
        fetch="one",
    )
    elapsed = time.time() - start
    print(f"Table count: {elapsed*1000:.2f}ms - Found {result.get('cnt')} tables")


def fetch_schema_count():
    """Simulates fetching schema count"""
    start = time.time()
    result = execute_query(
        "SELECT count(*) as cnt FROM information_schema.schemata;",
        config_type="remote",
        fetch="one",
    )
    elapsed = time.time() - start
    print(f"Schema count: {elapsed*1000:.2f}ms - Found {result.get('cnt')} schemas")


if __name__ == "__main__":
    print("=" * 60)
    print("Connection Server & Client - Usage Examples")
    print("=" * 60)
    print("\nThe connection server will be auto-started on first use.")
    print("All scripts will share the same server connection.")

    # Run examples
    example_1_simple_query()
    example_2_parameterized_query()
    example_3_multiple_queries()
    example_4_server_status()
    example_5_connection_reuse_demo()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
    print(
        "\nNote: The connection server will auto-shutdown after 1 hour of inactivity."
    )
    print("To manually stop it, call disconnect_server() or kill the process.")
