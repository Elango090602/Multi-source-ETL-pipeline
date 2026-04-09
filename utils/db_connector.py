"""
=============================================================================
db_connector.py — Database Connection Factory
=============================================================================
Provides a unified interface to create SQLAlchemy engines for:
  - MySQL     (via PyMySQL driver)
  - SQL Server (via pyodbc ODBC driver)

Engines are created once and can be reused across extract/load phases.
=============================================================================
"""

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Dict, Any


def build_mysql_engine(cfg: Dict[str, Any]) -> Engine:
    """
    Build and return a SQLAlchemy engine for a MySQL database.

    Connection URL format:
        mysql+pymysql://<user>:<pass>@<host>:<port>/<db>

    Args:
        cfg: A dict with keys: host, port, database, username, password.

    Returns:
        sqlalchemy.engine.Engine
    """
    url = (
        f"mysql+pymysql://{cfg['username']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        f"?charset=utf8mb4"
    )
    engine = create_engine(
        url,
        pool_pre_ping=True,       # Reconnect automatically on stale connections
        pool_recycle=3600,        # Recycle connections every 1 hour
        echo=False,               # Set True to print raw SQL (debug only)
    )
    return engine


def build_sqlserver_engine(cfg: Dict[str, Any]) -> Engine:
    """
    Build and return a SQLAlchemy engine for a SQL Server database.

    Supports both:
      - Named instances  (e.g., HOSTNAME\\SQLEXPRESS) → no port, uses Browser svc
      - TCP connections  (e.g., hostname:1433)        → explicit port in DSN

    Named SQL Server Express instances use dynamic ports, so we omit the port
    from the URL and instead pass all params via the ODBC connection string.

    Args:
        cfg: A dict with keys: host, database, username, password, driver.
             'port' is optional; omit or set to None for named instances.

    Returns:
        sqlalchemy.engine.Engine
    """
    import urllib

    driver = cfg.get("driver", "ODBC Driver 17 for SQL Server")
    host   = cfg["host"]          # e.g. "ZORO\\SQLEXPRESS" or "192.168.1.10"
    port   = cfg.get("port")      # None / "" → omit (named instance)

    # Build server string — include port only when explicitly provided
    server = f"{host},{port}" if port and str(port) not in ("", "0", "None") else host

    odbc_params = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};"
        f"PWD={cfg['password']};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=no;"
    )

    connection_url = (
        f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_params)}"
    )

    engine = create_engine(
        connection_url,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
    )
    return engine


def test_connection(engine: Engine, source_name: str) -> bool:
    """
    Validate a database connection by executing a lightweight query.

    Args:
        engine      : SQLAlchemy engine to test.
        source_name : Human-readable name for logging purposes.

    Returns:
        True if connection succeeds, False otherwise.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        # Caller handles logging; re-raise so pipeline knows
        raise ConnectionError(
            f"Connection test failed for '{source_name}': {exc}"
        ) from exc
