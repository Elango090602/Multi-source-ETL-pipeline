"""
=============================================================================
extract.py — Data Extraction Module
=============================================================================
Responsible for pulling raw data from all three source systems:
  1. MySQL database         → orders table
  2. SQL Server database    → products table
  3. Excel file             → customers sheet

Each extractor returns a raw pandas DataFrame. No transformation happens here.
=============================================================================
"""

import pandas as pd
from sqlalchemy.engine import Engine
from typing import Dict, Any

from utils.logger import get_logger
from utils.db_connector import build_mysql_engine, build_sqlserver_engine

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 1. MySQL Extractor
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_mysql(cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Connect to the MySQL source database and fetch the configured table.

    Uses chunked reading to handle large tables efficiently. All chunks
    are concatenated into a single DataFrame before returning.

    Args:
        cfg: The 'mysql_source' section from config.yaml, with resolved
             credentials and table name.

    Returns:
        pd.DataFrame: Raw data from the MySQL source table.

    Raises:
        Exception: Propagates any connection or query error after logging.
    """
    logger.info("Extracting data from MySQL | table: '%s'", cfg["table"])

    try:
        engine: Engine = build_mysql_engine(cfg)
        query = f"SELECT * FROM `{cfg['table']}`"

        # SQLAlchemy 2.x + pandas 3.x: pass a Connection, not the Engine
        with engine.connect() as conn:
            chunks = pd.read_sql(
                sql=query,
                con=conn,
                chunksize=cfg.get("chunk_size", 10_000),
            )
            df = pd.concat(list(chunks), ignore_index=True)

        engine.dispose()   # Return all pooled connections

        logger.info(
            "MySQL extraction complete | rows: %d | columns: %d",
            len(df), len(df.columns),
        )
        return df

    except Exception as exc:
        logger.error("MySQL extraction failed: %s", exc, exc_info=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# 2. SQL Server Extractor
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_sqlserver(cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Connect to the SQL Server source database and fetch the configured table.

    Args:
        cfg: The 'sqlserver_source' section from config.yaml.

    Returns:
        pd.DataFrame: Raw data from the SQL Server source table.

    Raises:
        Exception: Propagates any connection or query error after logging.
    """
    logger.info("Extracting data from SQL Server | table: '%s'", cfg["table"])

    try:
        engine: Engine = build_sqlserver_engine(cfg)

        # Use schema-qualified name if provided, else plain table name
        table_ref = f"[{cfg['table']}]"
        query = f"SELECT * FROM {table_ref}"

        # SQLAlchemy 2.x + pandas 3.x: pass a Connection, not the Engine
        with engine.connect() as conn:
            chunks = pd.read_sql(
                sql=query,
                con=conn,
                chunksize=cfg.get("chunk_size", 10_000),
            )
            df = pd.concat(list(chunks), ignore_index=True)

        engine.dispose()   # Return all pooled connections

        logger.info(
            "SQL Server extraction complete | rows: %d | columns: %d",
            len(df), len(df.columns),
        )
        return df

    except Exception as exc:
        logger.error("SQL Server extraction failed: %s", exc, exc_info=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# 3. Excel Extractor
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_excel(cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Read data from an Excel workbook into a DataFrame.

    Args:
        cfg: The 'excel_source' section from config.yaml, containing
             'file_path' and 'sheet_name'.

    Returns:
        pd.DataFrame: Raw data from the specified Excel sheet.

    Raises:
        FileNotFoundError : If the Excel file path is invalid.
        Exception         : Any other read error.
    """
    file_path = cfg["file_path"]
    sheet_name = cfg.get("sheet_name", 0)   # Default: first sheet

    logger.info(
        "Extracting data from Excel | file: '%s' | sheet: '%s'",
        file_path, sheet_name,
    )

    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            engine="openpyxl",          # Required for .xlsx format
        )

        logger.info(
            "Excel extraction complete | rows: %d | columns: %d",
            len(df), len(df.columns),
        )
        return df

    except FileNotFoundError:
        logger.error(
            "Excel file not found at path: '%s'. "
            "Ensure the file exists at the project root or the path in config.yaml is correct.",
            file_path,
        )
        raise
    except Exception as exc:
        logger.error("Excel extraction failed: %s", exc, exc_info=True)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# 4. Master Extractor (calls all three)
# ─────────────────────────────────────────────────────────────────────────────

def run_extraction(config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    Orchestrate extraction from all three source systems.

    Args:
        config: The full resolved configuration dictionary.

    Returns:
        dict with keys:
          - "orders"    : DataFrame from MySQL
          - "products"  : DataFrame from SQL Server
          - "customers" : DataFrame from Excel

    Raises:
        Exception: If any extractor fails (individual errors are logged first).
    """
    logger.info("=" * 60)
    logger.info("EXTRACTION PHASE STARTED")
    logger.info("=" * 60)

    datasets: Dict[str, pd.DataFrame] = {}

    # ── MySQL ──────────────────────────────────────────────────────────────
    datasets["orders"] = extract_from_mysql(config["mysql_source"])

    # ── SQL Server ────────────────────────────────────────────────────────
    datasets["products"] = extract_from_sqlserver(config["sqlserver_source"])

    # ── Excel ─────────────────────────────────────────────────────────────
    datasets["customers"] = extract_from_excel(config["excel_source"])

    logger.info("EXTRACTION PHASE COMPLETE — %d datasets loaded", len(datasets))
    return datasets
