# -*- coding: utf-8 -*-
"""
=============================================================================
pipeline.py -- Main Pipeline Orchestrator
=============================================================================
Entry point for the Automated Data Pipeline.

Run this file directly:
    python pipeline.py

Or call run_pipeline() programmatically from any other script.

Pipeline stages (in order):
  1. Load & validate configuration
  2. Extract   -- raw DataFrames from MySQL, SQL Server, Excel
  3. Transform -- clean, merge, enrich
  4. Load      -- write master DataFrame to target MySQL database
  5. Report    -- log summary metrics

The pipeline uses structured logging throughout and raises exceptions
on failure so it can be wrapped by any orchestrator (Airflow, cron, etc.).
=============================================================================
"""

import sys
import time
import traceback
from datetime import datetime

from utils.config_loader import load_config
from utils.logger import get_logger
from extract import run_extraction
from transform import run_transformation
from load import run_load

# Bootstrap a logger using defaults; will be reconfigured after config loads
logger = get_logger(__name__)


# =============================================================================
# Pipeline Runner
# =============================================================================

def run_pipeline(config_path: str = "config/config.yaml") -> bool:
    # Declare global first — logger is reassigned after config loads
    global logger

    """
    Execute the full ETL pipeline end-to-end.

    Stages:
      1. Config load
      2. Extraction
      3. Transformation
      4. Load + Validation

    Args:
        config_path: Path to the YAML configuration file.
                     Defaults to 'config/config.yaml'.

    Returns:
        True  -- pipeline completed successfully.
        False -- pipeline failed (errors are logged; exception is re-raised).

    Raises:
        Exception: Any unhandled error that terminates the pipeline.
    """
    pipeline_start = time.perf_counter()
    run_id         = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 62)
    logger.info("  AUTOMATED DATA PIPELINE  |  RUN: %s", run_id)
    logger.info("=" * 62)

    # -------------------------------------------------------------------------
    # Stage 1: Configuration
    # -------------------------------------------------------------------------
    stage_start = time.perf_counter()
    logger.info("[1/4] Loading configuration from '%s'...", config_path)

    try:
        config = load_config(config_path)

        # Re-apply logging settings now that we have the full config
        log_cfg = config.get("pipeline", {})
        logger = get_logger(
            __name__,
            log_file=log_cfg.get("log_file", "logs/pipeline.log"),
            level=log_cfg.get("log_level", "INFO"),
        )
        logger.info(
            "Configuration loaded [OK] (%.3fs)",
            time.perf_counter() - stage_start,
        )
    except Exception as exc:
        logger.critical("Configuration load FAILED: %s", exc)
        raise

    # -------------------------------------------------------------------------
    # Stage 2: Extraction
    # -------------------------------------------------------------------------
    stage_start = time.perf_counter()
    logger.info("[2/4] Starting extraction...")

    try:
        datasets = run_extraction(config)
        logger.info(
            "Extraction complete [OK] (%.3fs) -- %d dataset(s): %s",
            time.perf_counter() - stage_start,
            len(datasets),
            list(datasets.keys()),
        )
        for name, df in datasets.items():
            logger.info("  %-12s -> %d rows x %d columns", name, *df.shape)

    except Exception as exc:
        logger.error("Extraction FAILED: %s", exc)
        logger.debug(traceback.format_exc())
        raise

    # -------------------------------------------------------------------------
    # Stage 3: Transformation
    # -------------------------------------------------------------------------
    stage_start = time.perf_counter()
    logger.info("[3/4] Starting transformation...")

    try:
        master_df = run_transformation(datasets)
        logger.info(
            "Transformation complete [OK] (%.3fs) -- final shape: %s",
            time.perf_counter() - stage_start,
            master_df.shape,
        )
        logger.info("  Columns: %s", list(master_df.columns))

    except Exception as exc:
        logger.error("Transformation FAILED: %s", exc)
        logger.debug(traceback.format_exc())
        raise

    # -------------------------------------------------------------------------
    # Stage 4: Load
    # -------------------------------------------------------------------------
    stage_start = time.perf_counter()
    logger.info("[4/4] Starting load...")

    try:
        success = run_load(master_df, config)
        if not success:
            logger.warning(
                "Load completed but validation returned False. "
                "Inspect logs for row count discrepancy."
            )
        logger.info(
            "Load complete [OK] (%.3fs)", time.perf_counter() - stage_start
        )
    except Exception as exc:
        logger.error("Load FAILED: %s", exc)
        logger.debug(traceback.format_exc())
        raise

    # -------------------------------------------------------------------------
    # Pipeline Summary
    # -------------------------------------------------------------------------
    total_elapsed = time.perf_counter() - pipeline_start
    logger.info("=" * 62)
    logger.info(
        "PIPELINE COMPLETE | run_id: %s | duration: %.2fs | rows: %d",
        run_id, total_elapsed, len(master_df),
    )
    logger.info("=" * 62)

    return success


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    """
    Command-line entry point.
    Accepts an optional --config argument to override the default config path.

    Usage:
        python pipeline.py
        python pipeline.py --config path/to/custom_config.yaml
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Automated Data Pipeline -- Extract, Transform, Load",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/config.yaml",
        help=(
            "Path to the YAML configuration file.\n"
            "Default: config/config.yaml"
        ),
    )
    args = parser.parse_args()

    try:
        success = run_pipeline(config_path=args.config)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user (KeyboardInterrupt).")
        sys.exit(130)
    except Exception:
        # Error was already logged inside run_pipeline
        sys.exit(1)


if __name__ == "__main__":
    main()
