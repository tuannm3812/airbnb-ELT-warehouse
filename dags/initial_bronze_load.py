"""Initial Bronze load DAG for the Airbnb and Census warehouse.

This DAG loads the baseline May 2020 Airbnb extract, ABS Census G01/G02 files,
NSW LGA codes, and LGA-to-suburb mapping data into the Bronze schema.
"""

import os
from datetime import datetime
from typing import Tuple

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.helpers import load_with_autocreate, run_sql_file

AIRFLOW_DATA: str = os.environ.get(
    "AIRFLOW_DATA_PATH",
    Variable.get("AIRFLOW_DATA_PATH", "/home/airflow/gcs/data"),
)
SQL_FILE_PATH: str = os.environ.get(
    "BRONZE_INIT_SQL_PATH",
    Variable.get("BRONZE_INIT_SQL_PATH", "sql/init_bronze_schema.sql"),
)

AIRBNB_DIR: str = os.path.join(AIRFLOW_DATA, "airbnb")
CENSUS_G01_DIR: str = os.path.join(AIRFLOW_DATA, "census", "G01")
CENSUS_G02_DIR: str = os.path.join(AIRFLOW_DATA, "census", "G02")
MAPPINGS_DIR: str = os.path.join(AIRFLOW_DATA, "mappings")

AIRBNB_052020_PATH: str = os.path.join(AIRBNB_DIR, "05_2020.csv")
G01_PATH: str = os.path.join(CENSUS_G01_DIR, "2016Census_G01_NSW_LGA.csv")
G02_PATH: str = os.path.join(CENSUS_G02_DIR, "2016Census_G02_NSW_LGA.csv")
LGA_CODE_PATH: str = os.path.join(MAPPINGS_DIR, "NSW_LGA_CODE.csv")
LGA_SUBURB_PATH: str = os.path.join(MAPPINGS_DIR, "NSW_LGA_SUBURB.csv")

AIRBNB_ARCHIVE_DIR: str = os.path.join(AIRBNB_DIR, "archive")
G01_ARCHIVE_DIR: str = os.path.join(CENSUS_G01_DIR, "archive")
G02_ARCHIVE_DIR: str = os.path.join(CENSUS_G02_DIR, "archive")
MAPPINGS_ARCHIVE_DIR: str = os.path.join(MAPPINGS_DIR, "archive")

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


with DAG(
    dag_id="airbnb_census_initial_bronze_load",
    description="Load Airbnb (May 2020), Census, and Mappings to Bronze.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["airbnb", "census", "bronze"],
) as dag:

    init_bronze_schema_task = PythonOperator(
        task_id="init_bronze_schema",
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
    init_bronze_schema_task >> [
        load_airbnb_task,
        load_census_g01_task,
        load_census_g02_task,
        load_lga_code_task,
        load_lga_suburb_task,
    ]
