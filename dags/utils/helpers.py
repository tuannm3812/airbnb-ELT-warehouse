"""Shared Bronze-loading helpers for the Airbnb/Census Airflow DAGs.

These functions are used by both `airbnb_census_pipeline.py` and
`initial_bronze_load.py`. They are kept free of a module-level Airflow
import so this module can be unit tested without installing Airflow;
`PostgresHook` is only imported inside the functions that instantiate it.
"""

from __future__ import annotations

import csv
import logging
import os
import re
import shutil
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Iterable, List, Optional

from psycopg2.errors import DuplicateSchema, UniqueViolation

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

BRONZE_SCHEMA = "bronze"
PG_CONN_ID = "postgres"
TMP_DIR = "/tmp"


def timestamp_suffix() -> str:
    """Return a UTC timestamp suffix for archived file names."""
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def sanitize_identifier(name: str) -> str:
    """Convert a source column name into a safe Postgres identifier.

    Args:
        name: Raw source column name.

    Returns:
        A lowercase, underscore-separated Postgres-safe identifier.
    """
    safe = name.strip().lower()
    safe = re.sub(r"[^a-z0-9_]+", "_", safe)
    safe = re.sub(r"__+", "_", safe).strip("_")

    if re.match(r"^[0-9]", safe):
        safe = f"c_{safe}"

    return safe or "col"


def read_csv_header(file_path: str) -> List[str]:
    """Read the header row from a CSV file.

    Args:
        file_path: Path to the CSV file.

    Returns:
        The raw header values.
    """
    with open(file_path, "r", encoding="utf-8") as csv_file:
        return next(csv.reader(csv_file))


def ensure_table_from_csv_header(
    pg_hook: "PostgresHook",
    schema: str,
    table: str,
    file_path: str,
) -> List[str]:
    """Create a text-only Bronze table from a CSV header when needed.

    The function keeps existing tables when the column count still matches
    the incoming file. That allows the pipeline to append monthly extracts
    without unnecessary DDL churn while still recovering from simple schema
    drift.

    Args:
        pg_hook: Active Postgres hook.
        schema: Target schema name.
        table: Target table name.
        file_path: Source CSV used for schema inference.

    Returns:
        Sanitized column names in load order.
    """
    full_name = f"{schema}.{table}"
    header_cols = [
        sanitize_identifier(col) for col in read_csv_header(file_path)
    ]

    exists = pg_hook.get_first(
        "SELECT to_regclass(%s)",
        parameters=(full_name,),
    )
    if exists and exists[0]:
        rows = pg_hook.get_records(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            parameters=(schema, table),
        )
        current_cols = [row[0] for row in rows]

        if len(current_cols) == len(header_cols):
            return current_cols

        pg_hook.run(f"DROP TABLE {full_name};")

    cols_ddl = ", ".join(f"{column} TEXT" for column in header_cols)
    pg_hook.run(f"CREATE TABLE {full_name} ({cols_ddl});")
    return header_cols


def ensure_table_with_columns(
    pg_hook: "PostgresHook",
    schema: str,
    table: str,
    columns: Iterable[str],
) -> None:
    """Create a text-only Bronze table for a known fixed schema.

    Args:
        pg_hook: Active Postgres hook.
        schema: Target schema name.
        table: Target table name.
        columns: Required columns in file order.
    """
    full_name = f"{schema}.{table}"
    exists = pg_hook.get_first(
        "SELECT to_regclass(%s)",
        parameters=(full_name,),
    )

    if exists and exists[0]:
        rows = pg_hook.get_records(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            parameters=(schema, table),
        )
        current_cols = [row[0] for row in rows]

        if list(columns) == current_cols:
            return

        pg_hook.run(f"DROP TABLE {full_name};")

    cols_ddl = ", ".join(f"{column} TEXT" for column in columns)
    pg_hook.run(f"CREATE TABLE {full_name} ({cols_ddl});")


def clip_csv_columns(src_path: str, dst_path: str, keep_cols: int) -> None:
    """Write a copy of a CSV with only the first N columns.

    Args:
        src_path: Source CSV path.
        dst_path: Cleaned CSV path.
        keep_cols: Number of leading columns to retain.
    """
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    with open(src_path, "r", encoding="utf-8", newline="") as source_file:
        with open(
            dst_path,
            "w",
            encoding="utf-8",
            newline="",
        ) as target_file:
            reader = csv.reader(source_file)
            writer = csv.writer(target_file)
            for row in reader:
                writer.writerow(row[:keep_cols])


def copy_csv(
    pg_hook: "PostgresHook",
    schema: str,
    table: str,
    file_path: str,
    columns: Optional[Iterable[str]] = None,
    delimiter: str = ",",
    header: bool = True,
    truncate: bool = True,
) -> None:
    """Load a CSV file into Postgres with COPY FROM STDIN.

    Args:
        pg_hook: Active Postgres hook.
        schema: Target schema name.
        table: Target table name.
        file_path: Path to the CSV file.
        columns: Optional ordered columns for the COPY command.
        delimiter: CSV delimiter.
        header: Whether the source CSV includes a header row.
        truncate: Whether to truncate the target table before loading.

    Raises:
        FileNotFoundError: If the source CSV does not exist.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing file: {file_path}")

    if truncate:
        pg_hook.run(f"TRUNCATE TABLE {schema}.{table};")

    col_list = f"({', '.join(columns)})" if columns else ""
    copy_sql = (
        f"COPY {schema}.{table} {col_list} FROM STDIN WITH "
        f"(FORMAT CSV, HEADER {'TRUE' if header else 'FALSE'}, "
        f"DELIMITER '{delimiter}');"
    )
    pg_hook.copy_expert(sql=copy_sql, filename=file_path)


def archive_input(file_path: str, archive_dir: str) -> None:
    """Move a processed input file into an archive folder.

    Args:
        file_path: Source file path.
        archive_dir: Destination archive directory.
    """
    os.makedirs(archive_dir, exist_ok=True)

    base_name = os.path.basename(file_path)
    stem, ext = os.path.splitext(base_name)
    dst_path = os.path.join(archive_dir, f"{stem}_{timestamp_suffix()}{ext}")

    shutil.move(file_path, dst_path)
    logging.info("Archived %s -> %s", file_path, dst_path)


def ensure_schema(pg_hook: "PostgresHook", schema: str) -> None:
    """Create a schema if it does not already exist.

    Args:
        pg_hook: Active Postgres hook.
        schema: Schema name.
    """
    try:
        pg_hook.run(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    except (DuplicateSchema, UniqueViolation):
        logging.info("Schema %s already exists; continuing.", schema)


def run_sql_file(file_path: str) -> None:
    """Execute a SQL file against the configured Postgres connection.

    Args:
        file_path: Path to the SQL file.

    Raises:
        FileNotFoundError: If the SQL file does not exist.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing SQL file: {file_path}")

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with open(file_path, "r", encoding="utf-8") as sql_file:
        pg_hook.run(sql_file.read())


def load_with_autocreate(
    table: str,
    file_path: str,
    fixed_columns: Optional[Iterable[str]] = None,
    truncate: bool = True,
    clip_to_cols: Optional[int] = None,
    archive_dir: Optional[str] = None,
) -> None:
    """Load a CSV into Bronze, creating or repairing the target table.

    Args:
        table: Target Bronze table name.
        file_path: Source CSV path.
        fixed_columns: Optional fixed schema for known source files.
        truncate: Whether to truncate the target table before loading.
        clip_to_cols: Optional number of columns to retain in a temp CSV.
        archive_dir: If set, archive `file_path` here after a successful
            load. Leave unset when the caller archives as a separate,
            independently observable Airflow task.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    ensure_schema(pg_hook, BRONZE_SCHEMA)

    source_path = file_path
    if clip_to_cols:
        os.makedirs(TMP_DIR, exist_ok=True)
        cleaned_name = f"__clip_{os.path.basename(file_path)}"
        cleaned_path = os.path.join(TMP_DIR, cleaned_name)
        clip_csv_columns(file_path, cleaned_path, clip_to_cols)
        source_path = cleaned_path

    if fixed_columns:
        ensure_table_with_columns(
            pg_hook,
            BRONZE_SCHEMA,
            table,
            fixed_columns,
        )
        copy_csv(
            pg_hook,
            BRONZE_SCHEMA,
            table,
            source_path,
            columns=fixed_columns,
            truncate=truncate,
        )
    else:
        cols = ensure_table_from_csv_header(
            pg_hook,
            BRONZE_SCHEMA,
            table,
            source_path,
        )
        copy_csv(
            pg_hook,
            BRONZE_SCHEMA,
            table,
            source_path,
            columns=tuple(cols),
            truncate=truncate,
        )

    if clip_to_cols and os.path.exists(source_path):
        os.remove(source_path)

    if archive_dir and os.path.exists(file_path):
        archive_input(file_path, archive_dir)
