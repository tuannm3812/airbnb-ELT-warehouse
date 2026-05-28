# ============================================================================
# DAG: airbnb_census_initial_bronze_load
# Purpose: Load initial raw Airbnb (05_2020), ABS Census (G01/G02),
#          NSW LGA code, and LGA-to-suburb mapping into Postgres Bronze.
# ============================================================================

# Standard library
import csv
import os
import re
import shutil
from datetime import datetime
from typing import Iterable, List, Optional, Tuple

# Third-party
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.errors import DuplicateSchema, UniqueViolation

# ================================
# Config / Constants
# ================================

AIRFLOW_DATA: str = os.environ.get(
    "AIRFLOW_DATA_PATH",
    Variable.get("AIRFLOW_DATA_PATH", "/home/airflow/gcs/data"),
)
BRONZE_SCHEMA: str = "bronze"
PG_CONN_ID: str = "postgres"
SQL_FILE_PATH: str = os.environ.get(
    "BRONZE_INIT_SQL_PATH",
    Variable.get("BRONZE_INIT_SQL_PATH", "sql/init_bronze_schema.sql"),
)

# Folders
AIRBNB_DIR: str = os.path.join(AIRFLOW_DATA, "airbnb")
CENSUS_G01_DIR: str = os.path.join(AIRFLOW_DATA, "census", "G01")
CENSUS_G02_DIR: str = os.path.join(AIRFLOW_DATA, "census", "G02")
MAPPINGS_DIR: str = os.path.join(AIRFLOW_DATA, "mappings")
# Optimization: Use local container disk for temp ops, not GCS
TMP_DIR: str = "/tmp"

# Files
AIRBNB_052020_PATH: str = os.path.join(AIRBNB_DIR, "05_2020.csv")
G01_PATH: str = os.path.join(CENSUS_G01_DIR, "2016Census_G01_NSW_LGA.csv")
G02_PATH: str = os.path.join(CENSUS_G02_DIR, "2016Census_G02_NSW_LGA.csv")
LGA_CODE_PATH: str = os.path.join(MAPPINGS_DIR, "NSW_LGA_CODE.csv")
LGA_SUBURB_PATH: str = os.path.join(MAPPINGS_DIR, "NSW_LGA_SUBURB.csv")

# Archives
AIRBNB_ARCHIVE_DIR: str = os.path.join(AIRBNB_DIR, "archive")
G01_ARCHIVE_DIR: str = os.path.join(CENSUS_G01_DIR, "archive")
G02_ARCHIVE_DIR: str = os.path.join(CENSUS_G02_DIR, "archive")
MAPPINGS_ARCHIVE_DIR: str = os.path.join(MAPPINGS_DIR, "archive")

# Fixed Column Definitions (Tuples for immutability)
AIRBNB_COLS: Tuple[str, ...] = (
    "listing_id",
    "scrape_id",
    "scraped_date",
    "host_id",
    "host_name",
    "host_since",
    "host_is_superhost",
    "host_neighbourhood",
    "listing_neighbourhood",
    "property_type",
    "room_type",
    "accommodates",
    "price",
    "has_availability",
    "availability_30",
    "number_of_reviews",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_value",
)

LGA_CODE_COLS: Tuple[str, ...] = ("lga_code", "lga_name")
LGA_SUBURB_COLS: Tuple[str, ...] = ("lga_name", "suburb_name")


# ================================
# Helper Functions
# ================================

def _ts_suffix() -> str:
    """Generate a timestamp suffix for archiving files."""
    return datetime.utcnow().strftime("%Y%m%d-%H%M%S")


def sanitize_identifier(name: str) -> str:
    """Sanitize a string to be a safe Postgres identifier.

    Args:
        name (str): The raw column name.

    Returns:
        str: Lowercase, alphanumeric, underscore-separated identifier.
    """
    safe = name.strip().lower()
    safe = re.sub(r"[^a-z0-9_]+", "_", safe)
    safe = re.sub(r"__+", "_", safe).strip("_")
    if re.match(r"^[0-9]", safe):
        safe = f"c_{safe}"
    return safe or "col"


def read_csv_header(file_path: str) -> List[str]:
    """Read the first row (header) of a CSV file.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        List[str]: List of column names.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        return next(reader)


def ensure_table_from_csv_header(
    pg_hook: PostgresHook, schema: str, table: str, file_path: str
) -> List[str]:
    """Create or replace a table based on CSV header (all TEXT columns).

    This is 'self-healing' logic for dynamic schemas (e.g., Census data).

    Args:
        pg_hook (PostgresHook): Active Postgres connection.
        schema (str): Target schema (e.g., 'bronze').
        table (str): Target table name.
        file_path (str): Path to CSV to infer schema from.

    Returns:
        List[str]: The list of column names used to create the table.
    """
    full_name = f"{schema}.{table}"
    header_cols = [sanitize_identifier(c) for c in read_csv_header(file_path)]

    # Check if table exists
    exists = pg_hook.get_first(
        "SELECT to_regclass(%s)", parameters=(full_name,)
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
        current_cols = [r[0] for r in rows]
        # If schema matches exactly, return and skip drop
        if len(current_cols) == len(header_cols):
            return current_cols
        pg_hook.run(f"DROP TABLE {full_name};")

    # Create table with all TEXT columns
    cols_ddl = ", ".join(f"{c} TEXT" for c in header_cols)
    pg_hook.run(f"CREATE TABLE {full_name} ({cols_ddl});")
    return header_cols


def ensure_table_with_columns(
    pg_hook: PostgresHook, schema: str, table: str, columns: Iterable[str]
) -> None:
    """Ensure a table exists with a specific fixed schema (all TEXT).

    Args:
        pg_hook (PostgresHook): Active Postgres connection.
        schema (str): Target schema.
        table (str): Target table name.
        columns (Iterable[str]): Exact list of required columns.
    """
    full_name = f"{schema}.{table}"
    exists = pg_hook.get_first(
        "SELECT to_regclass(%s)", parameters=(full_name,)
    )

    recreate = True
    if exists and exists[0]:
        rows = pg_hook.get_records(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
            """,
            parameters=(schema, table),
        )
        current = [r[0] for r in rows]
        recreate = list(columns) != current
        if not recreate:
            return
        pg_hook.run(f"DROP TABLE {full_name};")

    cols_ddl = ", ".join(f"{c} TEXT" for c in columns)
    pg_hook.run(f"CREATE TABLE {full_name} ({cols_ddl});")


def clip_csv_columns(src_path: str, dst_path: str, keep_cols: int) -> None:
    """Write the first N columns of a CSV to a new file.

    Used to clean dirty CSVs (e.g. trailing empty columns).

    Args:
        src_path (str): Input CSV path.
        dst_path (str): Output CSV path.
        keep_cols (int): Number of columns to keep.
    """
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    with open(src_path, "r", encoding="utf-8", newline="") as fin, open(
        dst_path, "w", encoding="utf-8", newline=""
    ) as fout:
        rdr = csv.reader(fin)
        wtr = csv.writer(fout)
        for row in rdr:
            wtr.writerow(row[:keep_cols])


def copy_csv(
    pg_hook: PostgresHook,
    schema: str,
    table: str,
    file_path: str,
    columns: Optional[Iterable[str]] = None,
    delimiter: str = ",",
    header: bool = True,
    truncate: bool = True,
) -> None:
    """Perform a Postgres COPY FROM STDIN operation.

    Args:
        pg_hook (PostgresHook): Active connection.
        schema (str): Schema name.
        table (str): Table name.
        file_path (str): Local path to CSV on the worker.
        columns (Optional[Iterable[str]]): Specific columns to import.
        delimiter (str): CSV delimiter.
        header (bool): Whether CSV has a header.
        truncate (bool): Whether to TRUNCATE table before loading.
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
    """Move processed file to archive directory with timestamp.

    Args:
        file_path (str): Source file path.
        archive_dir (str): Destination directory.
    """
    os.makedirs(archive_dir, exist_ok=True)
    base = os.path.basename(file_path)
    stem, ext = os.path.splitext(base)
    dst = os.path.join(archive_dir, f"{stem}_{_ts_suffix()}{ext}")
    shutil.move(file_path, dst)


def ensure_schema(pg_hook: PostgresHook, schema: str) -> None:
    try:
        pg_hook.run(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    except (DuplicateSchema, UniqueViolation):
        pass


def run_sql_file(file_path: str) -> None:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing SQL file: {file_path}")
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with open(file_path, "r", encoding="utf-8") as sql_file:
        pg_hook.run(sql_file.read())


def load_with_autocreate(
    table: str,
    file_path: str,
    archive_dir: Optional[str] = None,
    fixed_columns: Optional[Iterable[str]] = None,
    clip_to_cols: Optional[int] = None,
) -> None:
    """Orchestrate the load: create table, clean file, COPY, archive.

    Args:
        table (str): Target table name in Bronze.
        file_path (str): Source CSV path.
        archive_dir (Optional[str]): Archive directory (optional).
        fixed_columns (Optional[Iterable[str]]): Enforce specific columns.
        clip_to_cols (Optional[int]): Pre-process CSV to keep N cols.
    """
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    ensure_schema(pg_hook, BRONZE_SCHEMA)
    source_path = file_path

    # Handle dirty CSVs using local temp storage
    if clip_to_cols:
        os.makedirs(TMP_DIR, exist_ok=True)
        cleaned_name = f"__clip_{os.path.basename(file_path)}"
        cleaned_path = os.path.join(TMP_DIR, cleaned_name)
        clip_csv_columns(file_path, cleaned_path, clip_to_cols)
        source_path = cleaned_path

    # Create/Enforce Table Schema
    if fixed_columns:
        ensure_table_with_columns(
            pg_hook, BRONZE_SCHEMA, table, fixed_columns
        )
        copy_csv(
            pg_hook,
            BRONZE_SCHEMA,
            table,
            source_path,
            columns=fixed_columns,
        )
    else:
        # Dynamic load (e.g. Census data with many columns)
        inferred = ensure_table_from_csv_header(
            pg_hook, BRONZE_SCHEMA, table, source_path
        )
        copy_csv(pg_hook, BRONZE_SCHEMA, table, source_path, columns=inferred)

    # Clean up temp files
    if clip_to_cols and os.path.exists(source_path):
        os.remove(source_path)

    # Archive original file after a successful load.
    if archive_dir and os.path.exists(file_path):
        archive_input(file_path, archive_dir)


# ================================
# DAG Definition
# ================================

with DAG(
    dag_id="airbnb_census_initial_bronze_load",
    description="Load Airbnb (May 2020), Census, and Mappings to Bronze.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["airbnb", "census", "bronze"],
) as dag:

    run_part1_sql_task = PythonOperator(
        task_id="run_part1_sql",
        python_callable=run_sql_file,
        op_kwargs={"file_path": SQL_FILE_PATH},
    )

    load_airbnb_task = PythonOperator(
        task_id="load_airbnb_05_2020",
        python_callable=load_with_autocreate,
        op_kwargs={
            "table": "airbnb_listings_raw",
            "file_path": AIRBNB_052020_PATH,
            "archive_dir": AIRBNB_ARCHIVE_DIR,
            "fixed_columns": AIRBNB_COLS,
        },
    )

    load_census_g01_task = PythonOperator(
        task_id="load_census_g01",
        python_callable=load_with_autocreate,
        op_kwargs={
            "table": "census_g01_raw",
            "file_path": G01_PATH,
            "archive_dir": G01_ARCHIVE_DIR,
            # No fixed_columns: Infer from header
        },
    )

    load_census_g02_task = PythonOperator(
        task_id="load_census_g02",
        python_callable=load_with_autocreate,
        op_kwargs={
            "table": "census_g02_raw",
            "file_path": G02_PATH,
            "archive_dir": G02_ARCHIVE_DIR,
            # No fixed_columns: Load wide, transform in dbt
        },
    )

    load_lga_code_task = PythonOperator(
        task_id="load_lga_code",
        python_callable=load_with_autocreate,
        op_kwargs={
            "table": "nsw_lga_code_raw",
            "file_path": LGA_CODE_PATH,
            "archive_dir": MAPPINGS_ARCHIVE_DIR,
            "fixed_columns": LGA_CODE_COLS,
        },
    )

    load_lga_suburb_task = PythonOperator(
        task_id="load_lga_suburb",
        python_callable=load_with_autocreate,
        op_kwargs={
            "table": "nsw_lga_suburb_raw",
            "file_path": LGA_SUBURB_PATH,
            "archive_dir": MAPPINGS_ARCHIVE_DIR,
            "fixed_columns": LGA_SUBURB_COLS,
            "clip_to_cols": 2,  # Handle trailing empty columns
        },
    )

    # Dependency Flow
    run_part1_sql_task >> [
        load_airbnb_task,
        load_census_g01_task,
        load_census_g02_task,
        load_lga_code_task,
        load_lga_suburb_task,
    ]
