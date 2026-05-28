"""Airflow DAG for the local Airbnb and Census ELT pipeline.

The DAG loads the baseline Airbnb extract, ABS Census reference files, NSW LGA
mapping files, and then processes the remaining Airbnb monthly extracts in
chronological order. Bronze loading is handled in Airflow; transformations and
SCD2 snapshots are delegated to dbt.
"""

import csv
import logging
import math
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from psycopg2.errors import DuplicateSchema, UniqueViolation

AIRFLOW_DATA = os.environ.get(
    "AIRFLOW_DATA_PATH",
    Variable.get("AIRFLOW_DATA_PATH", "/home/airflow/gcs/data"),
)
TMP_DIR = "/tmp"

BRONZE_SCHEMA = "bronze"
PG_CONN_ID = "postgres"
SQL_FILE_PATH = os.environ.get(
    "BRONZE_INIT_SQL_PATH",
    Variable.get("BRONZE_INIT_SQL_PATH", "sql/init_bronze_schema.sql"),
)

AIRBNB_DIR = os.path.join(AIRFLOW_DATA, "airbnb")
CENSUS_G01_DIR = os.path.join(AIRFLOW_DATA, "census", "G01")
CENSUS_G02_DIR = os.path.join(AIRFLOW_DATA, "census", "G02")
MAPPINGS_DIR = os.path.join(AIRFLOW_DATA, "mappings")

AIRBNB_052020_PATH = os.path.join(AIRBNB_DIR, "05_2020.csv")
G01_PATH = os.path.join(CENSUS_G01_DIR, "2016Census_G01_NSW_LGA.csv")
G02_PATH = os.path.join(CENSUS_G02_DIR, "2016Census_G02_NSW_LGA.csv")
LGA_CODE_PATH = os.path.join(MAPPINGS_DIR, "NSW_LGA_CODE.csv")
LGA_SUBURB_PATH = os.path.join(MAPPINGS_DIR, "NSW_LGA_SUBURB.csv")

AIRBNB_ARCHIVE = os.path.join(AIRBNB_DIR, "archive")
G01_ARCHIVE = os.path.join(CENSUS_G01_DIR, "archive")
G02_ARCHIVE = os.path.join(CENSUS_G02_DIR, "archive")
MAPPINGS_ARCHIVE = os.path.join(MAPPINGS_DIR, "archive")

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

# RUN_PART1 is retained as a backwards-compatible legacy variable name.
RUN_INITIAL_LOAD = Variable.get(
    "RUN_INITIAL_LOAD",
    Variable.get("RUN_PART1", "true"),
).lower() == "true"


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
    pg_hook: PostgresHook,
    schema: str,
    table: str,
    file_path: str,
) -> List[str]:
    """Create a text-only Bronze table from a CSV header when needed.

    The function keeps existing tables when the column count still matches the
    incoming file. That allows the pipeline to append monthly extracts without
    unnecessary DDL churn while still recovering from simple schema drift.

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
    pg_hook: PostgresHook,
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
    pg_hook: PostgresHook,
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


def ensure_schema(pg_hook: PostgresHook, schema: str) -> None:
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

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with open(file_path, "r", encoding="utf-8") as sql_file:
        pg_hook.run(sql_file.read())


def load_with_autocreate(
    table: str,
    file_path: str,
    fixed_columns: Optional[Iterable[str]] = None,
    truncate: bool = True,
    clip_to_cols: Optional[int] = None,
) -> None:
    """Load a CSV into Bronze, creating or repairing the target table.

    Args:
        table: Target Bronze table name.
        file_path: Source CSV path.
        fixed_columns: Optional fixed schema for known source files.
        truncate: Whether to truncate the target table before loading.
        clip_to_cols: Optional number of columns to retain in a temp CSV.
    """
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


def discover_month_files() -> List[str]:
    """Find monthly Airbnb extracts that still need to be processed.

    Returns:
        Paths to `MM_YYYY.csv` files sorted chronologically, excluding the
        May 2020 baseline file that belongs to the initial load.
    """
    if not os.path.isdir(AIRBNB_DIR):
        logging.warning("Airbnb directory not found: %s", AIRBNB_DIR)
        return []

    pattern = re.compile(r"^(?P<mm>\d{2})_(?P<yyyy>\d{4})\.csv$")
    month_files = []

    for file_name in os.listdir(AIRBNB_DIR):
        match = pattern.match(file_name)
        if not match or file_name == "05_2020.csv":
            continue

        sort_key = (int(match.group("yyyy")), int(match.group("mm")))
        month_files.append((sort_key, os.path.join(AIRBNB_DIR, file_name)))

    month_files.sort(key=lambda item: item[0])
    paths = [file_path for _, file_path in month_files]
    logging.info("Discovered %d monthly listing files: %s", len(paths), paths)
    return paths


def process_monthly_listing_files() -> None:
    """Load each remaining monthly Airbnb file and run dbt after each load."""
    month_files = discover_month_files()
    if not month_files:
        logging.info("No monthly listing files found to process.")
        return

    for file_path in month_files:
        base_name = os.path.basename(file_path)
        logging.info("Processing monthly listing file: %s", base_name)

        load_with_autocreate(
            table="airbnb_listings_raw",
            file_path=file_path,
            fixed_columns=AIRBNB_COLS,
            truncate=False,
        )
        trigger_dbt_cloud_job_and_wait(
            params={"cause": f"Loading {base_name}"},
        )
        archive_single(file_path)


def run_local_dbt_build(**kwargs: object) -> str:
    """Run configured dbt commands inside the Airflow container.

    Args:
        **kwargs: Airflow task context. The optional `params.cause` value is
            logged to make local runs easier to trace.

    Returns:
        A stable task result string for Airflow logs.

    Raises:
        AirflowException: If the dbt project is missing or a command fails.
    """
    project_dir = os.environ.get(
        "DBT_PROJECT_DIR",
        Variable.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"),
    )
    profiles_dir = os.environ.get(
        "DBT_PROFILES_DIR",
        Variable.get("DBT_PROFILES_DIR", "/opt/airflow/.dbt"),
    )
    commands = os.environ.get(
        "DBT_LOCAL_COMMANDS",
        Variable.get("DBT_LOCAL_COMMANDS", "dbt build"),
    )

    params = kwargs.get("params", {})
    cause = params.get("cause", "Triggered via Airflow")
    logging.info("Running local dbt for cause: %s", cause)

    if not os.path.isdir(project_dir):
        raise AirflowException(
            f"dbt project directory not found: {project_dir}",
        )

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = profiles_dir

    for command in [cmd.strip() for cmd in commands.split("&&")]:
        if not command:
            continue

        logging.info("Running dbt command: %s", command)
        result = subprocess.run(
            command,
            cwd=project_dir,
            env=env,
            shell=True,
            text=True,
            capture_output=True,
            check=False,
        )

        if result.stdout:
            logging.info(result.stdout)
        if result.stderr:
            logging.warning(result.stderr)
        if result.returncode != 0:
            raise AirflowException(
                "Local dbt command failed with exit code "
                f"{result.returncode}: {command}",
            )

    return "local-dbt-build"


def trigger_dbt_cloud_job_and_wait(**kwargs: object) -> object:
    """Trigger dbt Cloud or run local dbt based on `DBT_RUN_MODE`.

    Args:
        **kwargs: Airflow task context. `params.cause` is used as the dbt run
            cause when present.

    Returns:
        The dbt Cloud run ID in cloud mode, or a local result string in local
        mode.

    Raises:
        AirflowException: If the dbt run fails, is cancelled, or times out.
    """
    dbt_run_mode = os.environ.get(
        "DBT_RUN_MODE",
        Variable.get("DBT_RUN_MODE", "cloud"),
    ).lower()

    if dbt_run_mode == "local":
        return run_local_dbt_build(**kwargs)

    dbt_cloud_url = Variable.get("DBT_CLOUD_URL", "cloud.getdbt.com")
    dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
    dbt_cloud_job_id = Variable.get("DBT_CLOUD_JOB_ID")
    dbt_cloud_token = Variable.get("DBT_CLOUD_API_TOKEN")
    timeout_s = int(Variable.get("DBT_CLOUD_WAIT_TIMEOUT_SEC", "3600"))

    params = kwargs.get("params", {})
    cause = params.get("cause", "Triggered via Airflow")
    headers = {
        "Authorization": f"Token {dbt_cloud_token}",
        "Content-Type": "application/json",
    }

    base_url = (
        f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}"
    )
    run_url = f"{base_url}/jobs/{dbt_cloud_job_id}/run/"

    logging.info("Triggering dbt Cloud job %s.", dbt_cloud_job_id)
    response = requests.post(
        run_url,
        headers=headers,
        json={"cause": cause},
        timeout=30,
    )
    response.raise_for_status()

    run_data = response.json()["data"]
    run_id = run_data["id"]
    logging.info(
        "dbt Cloud run triggered. ID: %s. URL: %s",
        run_id,
        run_data.get("href"),
    )

    status_url = f"{base_url}/runs/{run_id}/"
    started = time.time()
    attempts = 0

    while True:
        attempts += 1
        time.sleep(min(10 * math.log2(attempts + 1), 30))

        status_response = requests.get(
            status_url,
            headers=headers,
            timeout=30,
        )
        status_response.raise_for_status()

        data = status_response.json()["data"]
        status = int(data["status"])
        human_status = data.get("status_humanized", "unknown")
        logging.info(
            "Checking dbt Cloud run %s status: %s (%s)",
            run_id,
            human_status,
            status,
        )

        if status == 10:
            logging.info("dbt Cloud run %s succeeded.", run_id)
            return run_id

        if status in (20, 30):
            raise AirflowException(
                f"dbt run {run_id} failed/cancelled: {human_status}",
            )

        if time.time() - started > timeout_s:
            raise AirflowException(f"Timeout waiting for dbt run {run_id}")


def archive_single(file_path: str) -> None:
    """Archive one Airbnb input file.

    Args:
        file_path: File path to archive.
    """
    archive_input(file_path, AIRBNB_ARCHIVE)


with DAG(
    dag_id="airbnb_census_monthly_pipeline",
    description=(
        "Loads baseline Airbnb/Census data, then processes monthly Airbnb "
        "extracts sequentially."
    ),
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["airbnb", "census", "bronze", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    if RUN_INITIAL_LOAD:
        bootstrap_group = TaskGroup(group_id="bootstrap_bronze")
        reference_group = TaskGroup(group_id="load_reference_data")
        dbt_group = TaskGroup(group_id="initial_dbt_snapshot")

        init_bronze_schema = PythonOperator(
            task_id="init_bronze_schema",
            task_group=bootstrap_group,
            python_callable=run_sql_file,
            op_kwargs={"file_path": SQL_FILE_PATH},
        )

        load_airbnb_052020 = PythonOperator(
            task_id="load_airbnb_05_2020",
            task_group=bootstrap_group,
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "airbnb_listings_raw",
                "file_path": AIRBNB_052020_PATH,
                "fixed_columns": AIRBNB_COLS,
                "truncate": True,
            },
        )

        load_census_g01 = PythonOperator(
            task_id="load_census_g01",
            task_group=reference_group,
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "census_g01_raw",
                "file_path": G01_PATH,
                "truncate": True,
            },
        )
        archive_g01 = PythonOperator(
            task_id="archive_g01",
            task_group=reference_group,
            python_callable=archive_input,
            op_kwargs={
                "file_path": G01_PATH,
                "archive_dir": G01_ARCHIVE,
            },
        )

        load_census_g02 = PythonOperator(
            task_id="load_census_g02",
            task_group=reference_group,
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "census_g02_raw",
                "file_path": G02_PATH,
                "truncate": True,
            },
        )
        archive_g02 = PythonOperator(
            task_id="archive_g02",
            task_group=reference_group,
            python_callable=archive_input,
            op_kwargs={
                "file_path": G02_PATH,
                "archive_dir": G02_ARCHIVE,
            },
        )

        load_lga_code = PythonOperator(
            task_id="load_lga_code",
            task_group=reference_group,
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "nsw_lga_code_raw",
                "file_path": LGA_CODE_PATH,
                "truncate": True,
            },
        )
        archive_lga_code = PythonOperator(
            task_id="archive_lga_code",
            task_group=reference_group,
            python_callable=archive_input,
            op_kwargs={
                "file_path": LGA_CODE_PATH,
                "archive_dir": MAPPINGS_ARCHIVE,
            },
        )

        load_lga_suburb = PythonOperator(
            task_id="load_lga_suburb",
            task_group=reference_group,
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "nsw_lga_suburb_raw",
                "file_path": LGA_SUBURB_PATH,
                "truncate": True,
                "clip_to_cols": 2,
            },
        )
        archive_lga_suburb = PythonOperator(
            task_id="archive_lga_suburb",
            task_group=reference_group,
            python_callable=archive_input,
            op_kwargs={
                "file_path": LGA_SUBURB_PATH,
                "archive_dir": MAPPINGS_ARCHIVE,
            },
        )

        dbt_after_initial = PythonOperator(
            task_id="dbt_after_initial",
            task_group=dbt_group,
            python_callable=trigger_dbt_cloud_job_and_wait,
            provide_context=True,
            params={"cause": "Initial warehouse load"},
        )

        archive_052020 = PythonOperator(
            task_id="archive_airbnb_05_2020",
            task_group=dbt_group,
            python_callable=archive_single,
            op_kwargs={"file_path": AIRBNB_052020_PATH},
        )

        start >> init_bronze_schema
        init_bronze_schema >> [
            load_airbnb_052020,
            load_census_g01,
            load_census_g02,
            load_lga_code,
            load_lga_suburb,
        ]

        load_census_g01 >> archive_g01 >> dbt_after_initial
        load_census_g02 >> archive_g02 >> dbt_after_initial
        load_lga_code >> archive_lga_code >> dbt_after_initial
        load_lga_suburb >> archive_lga_suburb >> dbt_after_initial
        load_airbnb_052020 >> dbt_after_initial
        dbt_after_initial >> archive_052020

        previous_task = archive_052020
    else:
        previous_task = start

    monthly_group = TaskGroup(group_id="monthly_incremental_loads")

    process_monthly_files = PythonOperator(
        task_id="process_monthly_listing_files",
        task_group=monthly_group,
        python_callable=process_monthly_listing_files,
    )

    previous_task >> process_monthly_files >> end
