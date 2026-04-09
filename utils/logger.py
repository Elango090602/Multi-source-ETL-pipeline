"""
=============================================================================
logger.py — Centralized Logging Setup
=============================================================================
Provides a reusable logger with:
  - Colored console output (via colorlog)
  - File-based logging with rotation
  - Single configuration point for the entire pipeline
=============================================================================
"""

import logging
import os
from logging.handlers import RotatingFileHandler

import colorlog


def get_logger(name: str, log_file: str = "logs/pipeline.log", level: str = "INFO") -> logging.Logger:
    """
    Create and return a configured logger instance.

    Args:
        name     : Logger name (typically __name__ of the calling module).
        log_file : Path to the log file.
        level    : Logging level string — DEBUG, INFO, WARNING, ERROR, CRITICAL.

    Returns:
        logging.Logger: Configured logger ready to use.
    """
    # Ensure the log directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    # Resolve the numeric log level
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)

    # Avoid adding duplicate handlers if logger already exists
    if logger.handlers:
        return logger

    # ── Console Handler (colored) ──────────────────────────────────────────
    console_formatter = colorlog.ColoredFormatter(
        fmt=(
            "%(log_color)s%(asctime)s │ %(levelname)-8s%(reset)s "
            "│ %(cyan)s%(name)s%(reset)s │ %(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG":    "white",
            "INFO":     "green",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red",
        },
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(console_formatter)

    # ── File Handler (rotating, plain text) ───────────────────────────────
    file_formatter = logging.Formatter(
        fmt="%(asctime)s │ %(levelname)-8s │ %(name)s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,   # 5 MB per file
        backupCount=3,               # Keep last 3 rotated files
        encoding="utf-8",
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
