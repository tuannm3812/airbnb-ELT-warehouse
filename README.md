# ELT Data Pipeline: Airbnb & Census Data Warehouse

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.0+-orange?logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Core%20%7C%20Cloud-FF694B?logo=dbt&logoColor=white)
![Postgres](https://img.shields.io/badge/PostgreSQL-13+-336791?logo=postgresql&logoColor=white)
![Status](https://img.shields.io/badge/Status-Complete-success)

## 📖 Executive Summary
This project implements a production-ready ELT (Extract, Load, Transform) pipeline to ingest, transform, and analyze Airbnb and Australian Census data for Sydney. The pipeline follows a **Medallion Architecture** (Bronze $\to$ Silver $\to$ Gold) to ensure data quality and traceability.

The final output is a Star Schema and Data Marts capable of answering key business questions regarding rental market dynamics, host behavior, and housing affordability.

---

## 🏗️ Architecture & Data Flow

The pipeline operates on Google Cloud Platform (Cloud Composer, GCS) and utilizes dbt Cloud for transformation logic.

![Architecture Diagram](\\report\\bde_at3_architect_flow.png)
*(Please insert your architecture diagram image here)*

**Key Architectural Features:**
* **Idempotency:** Loaders truncate Bronze tables on initial load; Silver layer handles deduplication using deterministic ordering rules.
* **SCD Type 2:** Dimensional history (Hosts, Listings) is tracked using dbt Snapshots with a `timestamp` strategy.
* **Sequential Loading:** The Airflow DAG loads monthly data chronologically, triggering dbt after each month to maintain SCD integrity.

---

## 📂 Repository Structure

```text
BDE_ASSIGNMENT_3/
├── dag/
│   ├── part_1_and_3_load.py   # Combined DAG for initial bootstrap and sequential monthly loads
│   └── part_1_initial_load.py # (Legacy) Part 1 specific logic
├── dbt/
│   ├── models/
│   │   ├── bronze/            # Source definitions for Raw layers
│   │   ├── silver/            # Cleaning, Casting, and Normalization logic
│   │   └── gold/              # Star Schema (Dimensions/Facts) and Reporting Marts
│   ├── snapshots/             # SCD2 definitions for Host, Property, Neighbourhood, LGA
│   └── dbt_project.yml        # Project configuration
├── sql/
│   ├── part_1.sql             # DDL for Bronze table creation
│   └── part_4.sql             # Ad-hoc SQL scripts for Business Analysis (Q1-Q5)
├── requirements.txt           # Python dependencies for Airflow
└── README.md                  # Project documentation
``` 
---

## 🚀 Setup & Execution

**Prerequisites:**
- **Google Cloud Platform:** Cloud Composer (Airflow) and Cloud SQL (Postgres).
- **dbt Cloud:** Project configured to connect to the Postgres instance.
- **Data Sources:** Airbnb CSVs (May 2020 - Apr 2021) and ABS Census Packs.

**Part 1 & 3: Ingestion & Pipeline**
The Airflow DAG `bde_at3_part1_and_part3` handles the end-to-end flow:
- **Bootstrap:** Runs `sql/part_1.sql` to create Bronze tables.
- **Initial Load:** Ingests May 2020 Airbnb data + Census + Mappings.
- **Sequential Loop:** Iterates through remaining files (`06_2020.csv` to `04_2021.csv`).
- **Transformation Trigger:** After loading each CSV, Airflow triggers the dbt Cloud job and waits for success before archiving the file.

**Part 2: Data Warehouse (dbt)**
- **Bronze:** Raw text ingestion.
- **Silver:** Cleaning (Regex price stripping, date parsing) and Normalization (Bridge tables for LGA/Suburb).
- **Snapshots:** Implements SCD2. `lga_snapshot` uses `check` strategy; others use `timestamp`.
- **Gold:** Fact table `g_fact_listing_monthly` linked to Dimensions via time-valid joins (`dbt_valid_from` / `dbt_valid_to`).

**Part 4: Analysis Outcomes**
Execute `sql/part_4.sql` to generate insights:
- **Q1:** Demographic differences (Top vs Bottom revenue LGAs).
- **Q2:** Correlation between Median Age and Revenue ($r \approx 0.70$).
- **Q3:** Best Listing Config (Entire Apt/Home for 2-4 guests).
- **Q4:** Host Concentration (94% of multi-listing hosts operate in a single LGA).
- **Q5:** Mortgage Affordability (Airbnb revenue rarely covers annual mortgages).

---

## 🛠️ Key Technologies
- **Apache Airflow:** For orchestration and state management.
- **dbt (data build tool):** For T (Transform) in ELT, testing, and documentation.
- **PostgreSQL:** The target Data Warehouse.
- **Google Cloud Storage:** Landing zone for raw data.