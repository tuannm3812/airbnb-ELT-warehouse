# ============================================================================
# DAG: bde_at3_part1_and_part3
# Purpose:
#   - Part 1: Bootstrap Bronze + initial loads (May 2020) + Dimensions.
#   - Part 3: Load remaining Airbnb months sequentially (Append Mode),
#             trigger dbt Cloud, and archive.
# ============================================================================

import csv
import logging
import math
import os
import re
import shutil
import time
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Tuple

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# -------------------------
# Config / Constants
# -------------------------
AIRFLOW_DATA = "/home/airflow/gcs/data"
TMP_DIR = "/tmp"  # Local disk for temp processing

BRONZE_SCHEMA = "bronze"
PG_CONN_ID = "postgres"
SQL_FILE_PATH = "sql/part_1.sql"

# Base folders
AIRBNB_DIR = os.path.join(AIRFLOW_DATA, "airbnb")
CENSUS_G01_DIR = os.path.join(AIRFLOW_DATA, "census", "G01")
CENSUS_G02_DIR = os.path.join(AIRFLOW_DATA, "census", "G02")
MAPPINGS_DIR = os.path.join(AIRFLOW_DATA, "mappings")

# Fixed-file paths (Part 1)
AIRBNB_052020_PATH = os.path.join(AIRBNB_DIR, "05_2020.csv")
G01_PATH = os.path.join(CENSUS_G01_DIR, "2016Census_G01_NSW_LGA.csv")
G02_PATH = os.path.join(CENSUS_G02_DIR, "2016Census_G02_NSW_LGA.csv")
LGA_CODE_PATH = os.path.join(MAPPINGS_DIR, "NSW_LGA_CODE.csv")
LGA_SUBURB_PATH = os.path.join(MAPPINGS_DIR, "NSW_LGA_SUBURB.csv")

# Archives
AIRBNB_ARCHIVE = os.path.join(AIRBNB_DIR, "archive")
G01_ARCHIVE = os.path.join(CENSUS_G01_DIR, "archive")
G02_ARCHIVE = os.path.join(CENSUS_G02_DIR, "archive")
MAPPINGS_ARCHIVE = os.path.join(MAPPINGS_DIR, "archive")

# Airbnb listings (Fixed Schema for Bronze)
AIRBNB_COLS: Tuple[str, ...] = (
    "listing_id", "scrape_id", "scraped_date", "host_id", "host_name", "host_since",
    "host_is_superhost", "host_neighbourhood", "listing_neighbourhood",
    "property_type", "room_type", "accommodates", "price", "has_availability",
    "availability_30", "number_of_reviews", "review_scores_rating",
    "review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin",
    "review_scores_communication", "review_scores_value",
)

# Gates
RUN_PART1 = Variable.get("RUN_PART1", "true").lower() == "true"

# -------------------------
# Helpers
# -------------------------
def _ts_suffix() -> str:
    return datetime.utcnow().strftime("%Y%m%d-%H%M%S")

def _sanitize_identifier(name: str) -> str:
    safe = name.strip().lower()
    safe = re.sub(r"[^a-z0-9_]+", "_", safe)
    safe = re.sub(r"__+", "_", safe).strip("_")
    if re.match(r"^[0-9]", safe):
        safe = f"c_{safe}"
    return safe or "col"

def read_csv_header(file_path: str) -> List[str]:
    with open(file_path, "r", encoding="utf-8") as f:
        return next(csv.reader(f))

def ensure_table_from_csv_header(pg: PostgresHook, schema: str, table: str, file_path: str) -> List[str]:
    full = f"{schema}.{table}"
    # Sanitize header
    header_cols = [_sanitize_identifier(c) for c in read_csv_header(file_path)]

    exists = pg.get_first("SELECT to_regclass(%s)", parameters=(full,))
    if exists and exists[0]:
        rows = pg.get_records(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            parameters=(schema, table),
        )
        current_cols = [r[0] for r in rows]
        # If columns match count, assume safe to append
        if len(current_cols) == len(header_cols):
            return current_cols
        pg.run(f"DROP TABLE {full};")

    cols_ddl = ", ".join(f"{c} TEXT" for c in header_cols)
    pg.run(f"CREATE TABLE {full} ({cols_ddl});")
    return header_cols

def clip_csv_columns(src: str, dst: str, keep_cols: int) -> None:
    """Pre-process CSV to remove trailing junk columns."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(src, "r", encoding="utf-8", newline="") as fin, \
         open(dst, "w", encoding="utf-8", newline="") as fout:
        rdr = csv.reader(fin)
        wtr = csv.writer(fout)
        for row in rdr:
            wtr.writerow(row[:keep_cols])

def copy_csv(pg: PostgresHook, schema: str, table: str, file_path: str,
             columns: Optional[Iterable[str]] = None,
             delimiter: str = ",", header: bool = True, truncate: bool = True) -> None:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing file: {file_path}")
    
    if truncate:
        pg.run(f"TRUNCATE TABLE {schema}.{table};")
    
    col_list = f"({', '.join(columns)})" if columns else ""
    copy_sql = (
        f"COPY {schema}.{table} {col_list} FROM STDIN WITH "
        f"(FORMAT CSV, HEADER {'TRUE' if header else 'FALSE'}, DELIMITER '{delimiter}');"
    )
    pg.copy_expert(sql=copy_sql, filename=file_path)

def archive_input(file_path: str, archive_dir: str) -> None:
    os.makedirs(archive_dir, exist_ok=True)
    base = os.path.basename(file_path)
    name, ext = os.path.splitext(base)
    dst = os.path.join(archive_dir, f"{name}_{_ts_suffix()}{ext}")
    shutil.move(file_path, dst)
    logging.info("Archived %s -> %s", file_path, dst)

def ensure_schema(pg: PostgresHook, schema: str) -> None:
    pg.run(f"CREATE SCHEMA IF NOT EXISTS {schema};")

def load_with_autocreate(table: str, file_path: str,
                         fixed_columns: Optional[Iterable[str]] = None,
                         truncate: bool = True,
                         clip_to_cols: Optional[int] = None) -> None:
    """Universal loader with auto-create, clipping, and fixed-schema support."""
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    ensure_schema(pg, BRONZE_SCHEMA)

    source_path = file_path
    
    # 1. Handle Clipping (Important for Suburb file)
    if clip_to_cols:
        os.makedirs(TMP_DIR, exist_ok=True)
        cleaned_path = os.path.join(TMP_DIR, f"__clip_{os.path.basename(file_path)}")
        clip_csv_columns(file_path, cleaned_path, clip_to_cols)
        source_path = cleaned_path

    # 2. Schema Management
    if fixed_columns:
        full = f"{BRONZE_SCHEMA}.{table}"
        exists = pg.get_first("SELECT to_regclass(%s)", parameters=(full,))
        
        # Recreate if missing
        if not (exists and exists[0]):
            cols_ddl = ", ".join(f"{c} TEXT" for c in fixed_columns)
            pg.run(f"CREATE TABLE {full} ({cols_ddl});")
        
        # Copy
        copy_csv(pg, BRONZE_SCHEMA, table, source_path, fixed_columns, truncate=truncate)
    else:
        # Dynamic Schema
        cols = ensure_table_from_csv_header(pg, BRONZE_SCHEMA, table, source_path)
        copy_csv(pg, BRONZE_SCHEMA, table, source_path, tuple(cols), truncate=truncate)

    # 3. Cleanup Temp
    if clip_to_cols and os.path.exists(source_path):
        os.remove(source_path)

def discover_month_files() -> List[str]:
    """Finds MM_YYYY.csv files, excluding May 2020 (handled in Part 1)."""
    if not os.path.isdir(AIRBNB_DIR):
        logging.warning("Airbnb directory not found during parse: %s", AIRBNB_DIR)
        return []
        
    pat = re.compile(r"^(?P<mm>\d{2})_(?P<yyyy>\d{4})\.csv$")
    files = []
    
    for fname in os.listdir(AIRBNB_DIR):
        m = pat.match(fname)
        if not m or fname == "05_2020.csv":
            continue
        files.append(((int(m.group("yyyy")), int(m.group("mm"))), os.path.join(AIRBNB_DIR, fname)))
    
    files.sort(key=lambda x: x[0]) # Sort by (Year, Month) tuple
    paths = [fp for _, fp in files]
    logging.info("Discovered %d monthly files for Part 3: %s", len(paths), paths)
    return paths

# ---- dbt Cloud Logic ----
def trigger_dbt_cloud_job_and_wait(**kwargs):
    """Triggers dbt Cloud Job and waits for Completion."""
    dbt_cloud_url        = Variable.get("DBT_CLOUD_URL", "cloud.getdbt.com")
    dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
    dbt_cloud_job_id     = Variable.get("DBT_CLOUD_JOB_ID")
    dbt_cloud_token      = Variable.get("DBT_CLOUD_API_TOKEN")
    timeout_s            = int(Variable.get("DBT_CLOUD_WAIT_TIMEOUT_SEC", "3600"))

    cause = kwargs.get("params", {}).get("cause", "Triggered via Airflow")
    
    # 1. Trigger
    base = f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}"
    run_url = f"{base}/jobs/{dbt_cloud_job_id}/run/"
    hdrs = {"Authorization": f"Token {dbt_cloud_token}", "Content-Type": "application/json"}
    
    logging.info("Triggering dbt Job %s...", dbt_cloud_job_id)
    r = requests.post(run_url, headers=hdrs, json={"cause": cause}, timeout=30)
    r.raise_for_status()
    run_data = r.json()["data"]
    run_id = run_data["id"]
    logging.info("dbt Run triggered. ID: %s. URL: %s", run_id, run_data.get("href"))

    # 2. Wait Loop
    status_url = f"{base}/runs/{run_id}/"
    started = time.time()
    attempts = 0

    while True:
        attempts += 1
        time.sleep(min(10 * math.log2(attempts + 1), 30)) # Backoff strategy

        s = requests.get(status_url, headers=hdrs, timeout=30)
        s.raise_for_status()
        data = s.json()["data"]
        
        status = int(data["status"])  # 10=Success, 20=Error, 30=Cancelled
        human_status = data.get("status_humanized", "unknown")
        
        logging.info("Checking run %s status: %s (%s)", run_id, human_status, status)

        if status == 10:
            logging.info("dbt Run %s Success!", run_id)
            return run_id
        elif status in (20, 30):
            raise AirflowException(f"dbt run {run_id} failed/cancelled. Status: {human_status}")
        
        if time.time() - started > timeout_s:
             raise AirflowException(f"Timeout waiting for dbt run {run_id}")

def archive_single(file_path: str) -> None:
    archive_input(file_path, AIRBNB_ARCHIVE)

# -------------------------
# DAG Definition
# -------------------------
with DAG(
    dag_id="bde_at3_part1_and_part3",
    description="Loads May 2020 + Dimensions (Part 1), then loads remaining months sequentially (Part 3).",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bde-at3", "bronze", "airbnb"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ==========================
    # PART 1: Initial Bootstrap
    # ==========================
    if RUN_PART1:
        run_part1_sql = PostgresOperator(
            task_id="run_part1_sql",
            postgres_conn_id=PG_CONN_ID,
            sql=SQL_FILE_PATH,
        )

        load_airbnb_052020 = PythonOperator(
            task_id="load_airbnb_05_2020",
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "airbnb_listings_raw",
                "file_path": AIRBNB_052020_PATH,
                "fixed_columns": AIRBNB_COLS,
                "truncate": True, # Always truncate for initial load
            },
        )

        load_census_g01 = PythonOperator(
            task_id="load_census_g01",
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "census_g01_raw",
                "file_path": G01_PATH,
                "truncate": True,
            },
        )
        archive_g01 = PythonOperator(
            task_id="archive_g01",
            python_callable=archive_input,
            op_kwargs={"file_path": G01_PATH, "archive_dir": G01_ARCHIVE},
        )

        # REFINED: No fixed columns for G02 (Dynamic Width)
        load_census_g02 = PythonOperator(
            task_id="load_census_g02",
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "census_g02_raw",
                "file_path": G02_PATH,
                # "fixed_columns": G02_COLS, <--- REMOVED
                "truncate": True,
            },
        )
        archive_g02 = PythonOperator(
            task_id="archive_g02",
            python_callable=archive_input,
            op_kwargs={"file_path": G02_PATH, "archive_dir": G02_ARCHIVE},
        )

        load_lga_code = PythonOperator(
            task_id="load_lga_code",
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "nsw_lga_code_raw",
                "file_path": LGA_CODE_PATH,
                "truncate": True,
            },
        )
        archive_lga_code = PythonOperator(
            task_id="archive_lga_code",
            python_callable=archive_input,
            op_kwargs={"file_path": LGA_CODE_PATH, "archive_dir": MAPPINGS_ARCHIVE},
        )

        # REFINED: Added clip_to_cols for Suburb file
        load_lga_suburb = PythonOperator(
            task_id="load_lga_suburb",
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "nsw_lga_suburb_raw",
                "file_path": LGA_SUBURB_PATH,
                "truncate": True,
                "clip_to_cols": 2, # Clean trailing commas
            },
        )
        archive_lga_suburb = PythonOperator(
            task_id="archive_lga_suburb",
            python_callable=archive_input,
            op_kwargs={"file_path": LGA_SUBURB_PATH, "archive_dir": MAPPINGS_ARCHIVE},
        )

        dbt_after_initial = PythonOperator(
            task_id="dbt_after_initial",
            python_callable=trigger_dbt_cloud_job_and_wait,
            provide_context=True,
            params={"cause": "Part 1 - Initial Load"},
        )

        archive_052020 = PythonOperator(
            task_id="archive_airbnb_05_2020",
            python_callable=archive_single,
            op_kwargs={"file_path": AIRBNB_052020_PATH},
        )

        start >> run_part1_sql >> [
            load_airbnb_052020,
            load_census_g01 >> archive_g01,
            load_census_g02 >> archive_g02,
            load_lga_code   >> archive_lga_code,
            load_lga_suburb >> archive_lga_suburb,
        ] >> dbt_after_initial >> archive_052020

        prev_task = archive_052020
    else:
        prev_task = start

    # ==========================
    # PART 3: Sequential Months
    # ==========================
    # Loop over remaining files (e.g., 06_2020 ... 04_2021)
    for fp in discover_month_files():
        base = os.path.basename(fp)
        safe_name = re.sub(r"[^a-zA-Z0-9_]+", "_", base).rstrip("_")

        load_task = PythonOperator(
            task_id=f"load_{safe_name}",
            python_callable=load_with_autocreate,
            op_kwargs={
                "table": "airbnb_listings_raw",
                "file_path": fp,
                "fixed_columns": AIRBNB_COLS,
                "truncate": False, # APPEND mode to preserve history for snapshots
            },
        )

        dbt_task = PythonOperator(
            task_id=f"dbt_after_{safe_name}",
            python_callable=trigger_dbt_cloud_job_and_wait,
            provide_context=True,
            params={"cause": f"Part 3 - Loading {base}"},
        )

        archive_task = PythonOperator(
            task_id=f"archive_{safe_name}",
            python_callable=archive_single,
            op_kwargs={"file_path": fp},
        )

        # Chain: Prev -> Load -> dbt -> Archive -> Next
        prev_task >> load_task >> dbt_task >> archive_task
        prev_task = archive_task

    prev_task >> end