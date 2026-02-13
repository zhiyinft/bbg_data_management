#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Merge the db module into a single standalone file.

This script:
1. Scans all Python files in the db module
2. Removes relative imports and adjusts the code
3. Concatenates in proper dependency order
4. Creates a single nft_db.py file

Usage:
    python merge_db.py
"""

import os
import re
from pathlib import Path


# Define the CORE order of files to merge (dependencies first)
# Additional .py files found in db/ will be appended after these
MERGE_ORDER_CORE = [
    "connection_server.py",
    "db_client.py",
    "res.py",
    "procs.py",
    "presets.py",
]

# Files to exclude from scanning
EXCLUDE_FILES = {"__init__.py"}


def scan_db_dir(db_dir):
    """
    Scan the db/ directory and return list of .py files to merge.

    Returns:
        list: Full list of files in merge order (core + newly discovered)
    """
    # Start with core order (filter out files that don't exist)
    merge_order = [f for f in MERGE_ORDER_CORE if os.path.exists(os.path.join(db_dir, f))]

    # Scan for new .py files
    existing_files = set(os.listdir(db_dir))
    py_files = {f for f in existing_files if f.endswith('.py') and f not in EXCLUDE_FILES}

    # Find new files not in core order
    known_files = set(MERGE_ORDER_CORE) | EXCLUDE_FILES
    new_files = sorted(py_files - known_files)

    if new_files:
        print(f"Found new files in db/: {', '.join(new_files)}")
        merge_order.extend(new_files)

    return merge_order


def read_file(filepath):
    """Read file content."""
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()


def process_imports(content, filename, all_imports):
    """
    Process imports in a file.

    - Remove relative imports from same module (they'll be in same file)
    - Keep external imports (os, sys, sqlalchemy, etc.)
    """
    lines = content.split("\n")
    result_lines = []

    for line in lines:
        stripped = line.strip()

        # Skip relative imports from same module
        # from .xxx import -> remove (will be available in merged file)
        # from . import -> remove
        if re.match(r"^from \.\w+\s+import", stripped):
            result_lines.append(f"# # REMOVED: {line}")
            continue
        elif re.match(r"^from \.\s+import", stripped):
            result_lines.append(f"# # REMOVED: {line}")
            continue

        result_lines.append(line)

    return "\n".join(result_lines)


def process_res_imports(content):
    """
    Special handling for res.py - convert relative imports from db_client.

    res.py has:
        from .db_client import get_client
        from .connection_server import NFT_RDS_CONFIG_LOCAL

    These need to be kept as comments since those modules will be
    defined earlier in the merged file.
    """
    lines = content.split("\n")
    result_lines = []

    for line in lines:
        stripped = line.strip()

        # Handle these specific relative imports in res.py
        if re.match(r"^from \.db_client import", stripped):
            result_lines.append(f"# # MERGED: {line}  # db_client defined above")
            continue
        elif re.match(r"^from \.connection_server import", stripped):
            result_lines.append(f"# # MERGED: {line}  # connection_server defined above")
            continue

        result_lines.append(line)

    return "\n".join(result_lines)


def process_procs_imports(content):
    """
    Special handling for procs.py.
    """
    lines = content.split("\n")
    result_lines = []

    for line in lines:
        stripped = line.strip()

        # from .res import TB, ClientTable
        if re.match(r"^from \.res import", stripped):
            result_lines.append(f"# # MERGED: {line}  # res defined above")
            continue

        result_lines.append(line)

    return "\n".join(result_lines)


def process_presets_imports(content):
    """
    Special handling for presets.py.
    """
    lines = content.split("\n")
    result_lines = []

    for line in lines:
        stripped = line.strip()

        # from .res import rds, bbg
        # from .db_client import get_client
        if re.match(r"^from \.\w+ import", stripped):
            result_lines.append(f"# # MERGED: {line}  # dependencies defined above")
            continue

        result_lines.append(line)

    return "\n".join(result_lines)


def preserve_connection_server_main(content):
    """
    Special handling for connection_server.py - keep its __main__ block.
    This allows the standalone file to run as a server directly.
    """
    lines = content.split("\n")
    result_lines = []
    skip_main = False

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Detect __main__ block - keep it for connection_server.py
        if re.match(r"^if __name__\s*==\s*['\"]__main__['\"]\s*:", stripped):
            skip_main = True
            result_lines.append(line)  # Keep the __main__ block line
            continue

        if skip_main:
            result_lines.append(line)  # Keep all lines inside __main__ block
            # Check if we've exited the __main__ block
            current_indent = len(line) - len(line.lstrip())
            if stripped and current_indent == 0:
                skip_main = False
            continue

        result_lines.append(line)

    return "\n".join(result_lines)


def remove_docstring_main(content):
    """Remove if __name__ == '__main__' blocks (except for connection_server.py)."""
    lines = content.split("\n")
    result_lines = []
    skip = False
    indent_level = 0

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Detect __main__ block
        if re.match(r"^if __name__\s*==\s*['\"]__main__['\"]\s*:", stripped):
            skip = True
            # Add a comment indicating this was removed
            result_lines.append(f"# # __main__ block removed from merged file")
            # Check if next line is indented (actual block content)
            if i + 1 < len(lines):
                next_indent = len(lines[i + 1]) - len(lines[i + 1].lstrip())
                if next_indent > 0:
                    indent_level = next_indent
            continue

        if skip:
            # Check if we've exited the __main__ block
            current_indent = len(line) - len(line.lstrip())
            if stripped and current_indent == 0:
                skip = False
                result_lines.append(line)
            # else: skip the line inside __main__ block
            continue

        result_lines.append(line)

    return "\n".join(result_lines)


def add_file_separator(content, filename):
    """Add a separator comment before each file's content."""
    separator = f'\n# {"=" * 75}\n'
    separator += f"# Source file: {filename}\n"
    separator += f'# {"=" * 75}\n\n'
    return separator + content


def post_process_standalone(content):
    """
    Post-process the merged file to fix standalone-specific issues:
    1. Change _get_server_path() to return __file__ instead of connection_server.py
    2. Fix _start_server() command to use standalone file itself
    3. Keep __main__ block in connection_server.py for server auto-start
    """
    # Fix _get_server_path to return __file__ instead of looking for connection_server.py
    content = re.sub(
        r'def _get_server_path\(\) -> str:\s*"""Get the path to the connection_server module"""\s+# Get the directory containing this module\s+module_dir = os\.path\.dirname\(os\.path\.abstractmethod\(__file__\)\)\s+return os\.path\.join\(module_dir, "connection_server\.py"\)',
        '''def _get_server_path() -> str:
    """Get the path to this module (contains the server code)"""
    return os.path.abspath(__file__)''',
        content,
        flags=re.DOTALL
    )

    # Also handle simpler version
    content = re.sub(
        r'return os\.path\.join\(module_dir, "connection_server\.py"\)',
        'return os.path.abspath(__file__)',
        content
    )

    # Update _get_server_path docstring
    content = re.sub(
        r'"""Get the path to the connection_server module"""',
        '"""Get the path to this module (contains the server code)"""',
        content
    )

    # Fix _start_server command to use standalone file itself
    # Change: cmd = ["python", server_path, "--port", str(self.port)]
    # To: cmd = ["python", _get_server_path(), "--port", str(self.port)]
    content = re.sub(
        r'cmd = \["python", server_path, "--port", str\(self\.port\)\]',
        'cmd = ["python", _get_server_path(), "--port", str(self.port)]',
        content
    )

    return content


def create_standalone_module(source_dir, output_file):
    """Create a standalone module file."""
    print(f"Creating standalone module from {source_dir}...")

    # Scan for files to merge
    merge_order = scan_db_dir(source_dir)
    print(f"Files to merge ({len(merge_order)}): {', '.join(merge_order)}")

    merged_parts = []

    # Add module header with __main__ capability
    header = '''# -*- coding: utf-8 -*-
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

'''
    merged_parts.append(header)

    # Process each file in order
    for filename in merge_order:
        filepath = os.path.join(source_dir, filename)

        if not os.path.exists(filepath):
            print(f"Warning: {filename} not found, skipping...")
            continue

        print(f"  Processing {filename}...")

        content = read_file(filepath)

        # Remove shebang and encoding line (already in header)
        lines = content.split("\n")
        if lines and lines[0].startswith("#!"):
            lines = lines[1:]
        if lines and (lines[0].startswith("# -*- coding:") or lines[0].startswith("# coding:")):
            lines = lines[1:]
        content = "\n".join(lines)

        # Process imports based on file type
        if filename == "res.py":
            content = process_res_imports(content)
        elif filename == "procs.py":
            content = process_procs_imports(content)
        elif filename == "presets.py":
            content = process_presets_imports(content)
        else:
            content = process_imports(content, filename, {})

        # Handle __main__ blocks - keep for connection_server.py, remove for others
        if filename == "connection_server.py":
            content = preserve_connection_server_main(content)
        else:
            content = remove_docstring_main(content)

        # Add file separator
        content = add_file_separator(content, filename)

        merged_parts.append(content)

    # Combine all parts
    output_content = "\n".join(merged_parts)

    # Post-process: disable auto_start and fix server path
    output_content = post_process_standalone(output_content)

    # Write output
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(output_content)

    print(f"\nCreated: {output_file}")
    print(f"Size: {len(output_content):,} bytes ({len(output_content)//1024} KB)")

    # Create a simple test to verify the merged file
    print("\n" + "=" * 50)
    print("Verifying merged file...")
    print("=" * 50)

    try:
        # Try to import the merged module
        import sys
        import importlib.util

        spec = importlib.util.spec_from_file_location("db_standalone", output_file)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)

            # Check if it loads without syntax errors
            with open(output_file, 'r') as f:
                compile(f.read(), output_file, 'exec')

            print("Syntax check: PASSED")
        else:
            print("Warning: Could not verify module structure")
    except SyntaxError as e:
        print(f"Syntax error: {e}")
        return 1
    except Exception as e:
        print(f"Warning: {e}")

    print("\nTo use in another project:")
    print(f"  1. Copy {output_file} to your project")
    print("  2. Import: from nft_db import db, rds, bbg, execute_query")

    return 0


def main():
    """Main entry point."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(script_dir, "db")
    output_file = os.path.join(script_dir, "nft_db.py")

    if not os.path.exists(db_dir):
        print(f"Error: db directory not found at {db_dir}")
        return 1

    return create_standalone_module(db_dir, output_file)


if __name__ == "__main__":
    exit(main())
