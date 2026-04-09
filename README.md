# 🔄 multi-source-etl-pipeline

> A **production-ready, modular ETL (Extract → Transform → Load) pipeline** built in Python — designed to unify data from **MySQL**, **SQL Server**, and **Excel** into a single, clean MySQL target database. Portfolio-grade project demonstrating professional data engineering practices.

---

## 📌 Overview

This pipeline automates the full data integration lifecycle:

| Stage | Description |
|-------|-------------|
| **Extract** | Pull raw data from 3 heterogeneous sources (MySQL, SQL Server, Excel) |
| **Transform** | Clean, standardize, merge, and enrich data using Pandas |
| **Load** | Write the unified master DataFrame to a target MySQL database with validation |

Each stage is independently testable, fully logged, and designed to slot into any orchestrator like **Apache Airflow**, **cron**, or **Windows Task Scheduler**.

---

## 🗂️ Project Structure

```
multi-source-etl-pipeline/
│
├── pipeline.py               # 🚀 Main entry point — orchestrates all 4 stages
├── extract.py                # Stage 2: Extractors for MySQL, SQL Server & Excel
├── transform.py              # Stage 3: Data cleaning, merging, and enrichment
├── load.py                   # Stage 4: Load to target MySQL + post-load validation
├── demo_pipeline.py          # Standalone demo using generated sample data
├── generate_sample_data.py   # Generates mock CSV data for local testing
├── setup_target_db.sql       # SQL script to initialize the target MySQL schema
│
├── config/
│   └── config.yaml           # All source/target DB credentials & pipeline settings
│
├── utils/
│   ├── __init__.py
│   ├── config_loader.py      # YAML + .env config resolver
│   ├── db_connector.py       # SQLAlchemy engine builders (MySQL & SQL Server)
│   └── logger.py             # Colored, structured logger (console + file)
│
├── data/                     # Input Excel files & generated CSVs (gitignored outputs)
├── logs/                     # Auto-created pipeline run logs (gitignored)
│
├── .env                      # ⚠️ Local credentials — NEVER commit (gitignored)
├── .gitignore
└── requirements.txt
```

---

## ⚙️ Tech Stack

| Category | Library / Tool |
|----------|---------------|
| Language | Python 3.10+ |
| Data Processing | `pandas >= 2.2`, `numpy >= 2.1` |
| Database ORM | `SQLAlchemy >= 2.0` |
| MySQL Driver | `PyMySQL` + `cryptography` |
| SQL Server Driver | `pyodbc` (via ODBC) |
| Excel Reader | `openpyxl` |
| Config | `PyYAML` + `python-dotenv` |
| Logging | `colorlog` (colored console) + file handler |

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/multi-source-etl-pipeline.git
cd multi-source-etl-pipeline
```

### 2. Create & Activate a Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Your Environment

Duplicate the example env file and fill in your credentials:

```bash
cp .env.example .env       # or manually create .env
```

**.env format:**
```dotenv
# MySQL Source
MYSQL_SOURCE_HOST=localhost
MYSQL_SOURCE_PORT=3306
MYSQL_SOURCE_USER=root
MYSQL_SOURCE_PASSWORD=your_password
MYSQL_SOURCE_DB=source_db

# SQL Server Source
SQLSERVER_HOST=localhost
SQLSERVER_PORT=1433
SQLSERVER_USER=sa
SQLSERVER_PASSWORD=your_password
SQLSERVER_DB=source_db

# MySQL Target
MYSQL_TARGET_HOST=localhost
MYSQL_TARGET_PORT=3306
MYSQL_TARGET_USER=root
MYSQL_TARGET_PASSWORD=your_password
MYSQL_TARGET_DB=etl_output
```

Edit `config/config.yaml` to point to your tables and Excel file path.

### 5. Initialize the Target Database

```bash
mysql -u root -p < setup_target_db.sql
```

### 6. Run the Pipeline

```bash
# Default (uses config/config.yaml)
python pipeline.py

# Custom config path
python pipeline.py --config path/to/custom_config.yaml
```

### 7. Run the Demo (No DB Required)

Generate mock data and run a full pipeline demo locally:

```bash
python generate_sample_data.py   # Creates CSV files in data/
python demo_pipeline.py          # Runs ETL on the mock data
```

---

## 🔬 Pipeline Architecture

```
                    ┌─────────────────────────────────┐
                    │         pipeline.py              │
                    │   (Orchestrator / Entry Point)   │
                    └──────┬──────────┬───────────────┘
                           │          │
          ┌────────────────▼──┐    ┌──▼─────────────────┐
          │    extract.py      │    │    config/           │
          │                    │    │    config.yaml       │
          │  ┌──────────────┐  │    │    + .env            │
          │  │ MySQL source │  │    └────────────────────-─┘
          │  ├──────────────┤  │
          │  │ SQL Server   │  │
          │  ├──────────────┤  │
          │  │ Excel file   │  │
          │  └──────┬───────┘  │
          └─────────┼──────────┘
                    │ raw DataFrames
          ┌─────────▼──────────┐
          │   transform.py     │
          │                    │
          │  1. Standardize    │
          │  2. Handle NULLs   │
          │  3. Deduplicate    │
          │  4. Cast types     │
          │  5. Merge/Join     │
          │  6. Derive cols    │
          └─────────┬──────────┘
                    │ master DataFrame
          ┌─────────▼──────────┐
          │     load.py        │
          │                    │
          │  Chunked INSERT     │
          │  + Row-count       │
          │    validation      │
          └─────────┬──────────┘
                    │
          ┌─────────▼──────────┐
          │  Target MySQL DB   │
          │  (etl_output)      │
          └────────────────────┘
```

---

## 🧹 Transformation Steps

| # | Step | Description |
|---|------|-------------|
| 1 | **Column Standardization** | Converts all column names to `snake_case` |
| 2 | **Missing Value Handling** | Numeric → median fill; String → `"Unknown"`; Dates → `NaT` |
| 3 | **Deduplication** | Removes fully duplicate rows |
| 4 | **Type Casting** | Auto-detects and casts date, ID, and numeric columns |
| 5 | **Dataset Merging** | Left-joins on `product_id` / `customer_id`; falls back to concat |
| 6 | **Derived Columns** | Computes `revenue`, `discount_amount`, `net_revenue`, `is_high_value`, `order_year`, `order_month` |

---

## 📋 Configuration Reference

All settings live in `config/config.yaml`. Sensitive values are injected from `.env` at runtime.

```yaml
pipeline:
  log_level: INFO
  log_file: logs/pipeline.log

mysql_source:
  host: ${MYSQL_SOURCE_HOST}
  table: orders
  chunk_size: 10000

sqlserver_source:
  host: ${SQLSERVER_HOST}
  table: products
  chunk_size: 10000

excel_source:
  file_path: data/customers.xlsx
  sheet_name: Sheet1

mysql_target:
  host: ${MYSQL_TARGET_HOST}
  table: master_data
  if_exists: replace        # replace | append | fail
  chunk_size: 1000
```

---

## 📝 Logging

The pipeline produces structured, colored logs to both the console and a rotating log file:

```
============================================================
  AUTOMATED DATA PIPELINE  |  RUN: 20260409_115000
============================================================
[1/4] Loading configuration from 'config/config.yaml'...
[2/4] Starting extraction...
  orders       -> 5000 rows x 8 columns
  products     -> 200 rows x 6 columns
  customers    -> 1500 rows x 5 columns
[3/4] Starting transformation...
[4/4] Starting load...
============================================================
PIPELINE COMPLETE | run_id: 20260409_115000 | duration: 4.21s | rows: 5000
============================================================
```

Log files are saved to `logs/pipeline.log` (gitignored).

---

## 🛡️ Error Handling

- Each pipeline stage has isolated `try/except` blocks with detailed error messages
- Critical failures raise exceptions and exit with code `1` — compatible with CI/CD and orchestrators
- Post-load row-count validation catches silent data loss

---

## 🧪 Running Without Live Databases

```bash
# Step 1: Generate mock CSV data
python generate_sample_data.py

# Step 2: Run the full demo pipeline
python demo_pipeline.py
```

This runs the entire ETL process using generated mock data, no database connections needed.

---

## 📁 Gitignore Summary

The following are excluded from version control:

- `.env` — local credentials
- `venv/` — virtual environment
- `logs/` — runtime log files
- `data/pipeline_output.csv` — generated outputs
- `__pycache__/` — Python cache files

---

## 🙋 Author

Built as a **portfolio project** showcasing production-ready data engineering skills:
- Modular ETL design
- Multi-source data integration
- Pandas-based transformation
- SQLAlchemy 2.x database patterns
- Structured logging and error handling

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
