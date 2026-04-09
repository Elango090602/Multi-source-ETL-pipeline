"""
=============================================================================
load.py — Data Loading Module
=============================================================================
Responsible for persisting the final transformed DataFrame to the
target MySQL database.

Features:
  - Chunked inserts to handle large DataFrames efficiently
  - Table creation / replacement controlled via config (replace / append)
  - Post-load validation: row-count check against what was written
  - Full error handling with rollback awareness
=============================================================================
"""

import pandas as pd
from typing import Any, Dict

from utils.db_connector import build_mysql_engine
from utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helper — Sanitize DataFrame before DB write
# ─────────────────────────────────────────────────────────────────────────────

def _sanitize_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final prep before writing to the database:
      - Convert pandas NA/NaT types that some DB drivers cannot handle
      - Drop any duplicated column names (can arise from messy merges)
      - Reset index to avoid writing the internal index as a column

    Args:
        df: Transformed master DataFrame.

    Returns:
        Sanitized DataFrame ready for DB insertion.
    """
    # Drop duplicated columns (keep first occurrence)
    df = df.loc[:, ~df.columns.duplicated()]

    # Replace pandas NA/NaT with None (DB-safe null)
    df = df.where(pd.notnull(df), other=None)

    # Ensure a clean sequential integer index
    df = df.reset_index(drop=True)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Main Loader
# ─────────────────────────────────────────────────────────────────────────────

def load_to_mysql(df: pd.DataFrame, cfg: Dict[str, Any]) -> int:
    """
    Write the master DataFrame to the target MySQL table.

    Behavior is controlled by cfg['if_exists']:
      - "replace" : Drop and recreate the table (data overwrite).
      - "append"  : Insert rows without modifying existing data.
      - "fail"    : Raise ValueError if table already exists.

    Args:
        df  : Final transformed DataFrame.
        cfg : The 'mysql_target' section from config.yaml.

    Returns:
        int: Number of rows successfully written.

    Raises:
        Exception: Any connection or write error (after logging).
    """
    table_name = cfg["table"]
    if_exists  = cfg.get("if_exists", "replace")
    chunk_size = cfg.get("chunk_size", 1_000)

    logger.info(
        "Loading %d rows → MySQL table '%s' (if_exists='%s')",
        len(df), table_name, if_exists,
    )

    try:
        engine = build_mysql_engine(cfg)
        df_clean = _sanitize_for_db(df)

        # SQLAlchemy 2.x + pandas 3.x: pass a Connection, not the Engine
        with engine.begin() as conn:      # engine.begin() = auto-commit transaction
            df_clean.to_sql(
                name=table_name,
                con=conn,
                if_exists=if_exists,
                index=False,              # Don't write the pandas index as a column
                chunksize=chunk_size,     # Insert in batches for performance
                method="multi",           # Batch INSERT statements (faster than row-by-row)
            )

        engine.dispose()   # Release pooled connections

        logger.info(
            "Load complete | table: '%s' | rows written: %d",
            table_name, len(df_clean),
        )
        return len(df_clean)

    except Exception as exc:
        logger.error(
            "Load to MySQL failed for table '%s': %s", table_name, exc,
            exc_info=True,
        )
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Post-Load Validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_load(cfg: Dict[str, Any], expected_rows: int) -> bool:
    """
    Query the target table and compare its row count with the expected value.

    This provides a basic sanity check that the load completed without
    silent truncation or data loss.

    Args:
        cfg           : The 'mysql_target' section from config.yaml.
        expected_rows : Number of rows that were written.

    Returns:
        True if the actual row count matches expected, False otherwise.
    """
    from sqlalchemy import text

    table_name = cfg["table"]
    logger.info("Validating load | expected rows: %d", expected_rows)

    try:
        engine = build_mysql_engine(cfg)
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM `{table_name}`")
            )
            actual_rows = result.scalar()

        if actual_rows == expected_rows:
            logger.info(
                "Validation PASSED ✓ | table '%s' has %d rows.",
                table_name, actual_rows,
            )
            return True
        else:
            logger.warning(
                "Validation MISMATCH ✗ | expected: %d | actual: %d",
                expected_rows, actual_rows,
            )
            return False

    except Exception as exc:
        logger.error("Validation query failed: %s", exc, exc_info=True)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Master Loader (called by pipeline.py)
# ─────────────────────────────────────────────────────────────────────────────

def run_load(df: pd.DataFrame, config: Dict[str, Any]) -> bool:
    """
    Orchestrate the full load phase: write data, then validate.

    Args:
        df     : Transformed master DataFrame.
        config : Full resolved configuration dictionary.

    Returns:
        True if load + validation succeed, False otherwise.
    """
    logger.info("=" * 60)
    logger.info("LOAD PHASE STARTED")
    logger.info("=" * 60)

    target_cfg = config["mysql_target"]

    rows_written = load_to_mysql(df, target_cfg)
    is_valid     = validate_load(target_cfg, rows_written)

    logger.info("LOAD PHASE COMPLETE | success: %s", is_valid)
    return is_valid
