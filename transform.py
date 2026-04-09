"""
=============================================================================
transform.py — Data Transformation Module
=============================================================================
Applies all cleaning, standardization, and enrichment logic to raw DataFrames.

Transformation steps (in order):
  1. Standardize column names      → lowercase, underscores, no special chars
  2. Handle missing values         → fill or drop based on column type
  3. Remove duplicate rows
  4. Cast data types               → dates, numerics, etc.
  5. Merge/concatenate datasets    → unified DataFrame
  6. Derive new columns            → revenue = price * quantity, etc.
=============================================================================
"""

import re
import pandas as pd
import numpy as np
from typing import Dict

from utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Standardize Column Names
# ─────────────────────────────────────────────────────────────────────────────

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize all column names to snake_case:
      - Strip leading/trailing whitespace
      - Convert to lowercase
      - Replace spaces and special characters with underscores
      - Collapse multiple consecutive underscores

    Args:
        df: Input DataFrame with arbitrary column names.

    Returns:
        DataFrame with cleaned, consistent column names.
    """
    def clean_name(col: str) -> str:
        col = str(col).strip().lower()
        col = re.sub(r"[^a-z0-9]+", "_", col)   # non-alphanumeric → underscore
        col = re.sub(r"_+", "_", col)            # collapse multiple underscores
        col = col.strip("_")                     # strip leading/trailing underscores
        return col

    original_cols = list(df.columns)
    df = df.rename(columns=clean_name)
    renamed = {o: n for o, n in zip(original_cols, df.columns) if o != n}

    if renamed:
        logger.debug("Renamed columns: %s", renamed)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Handle Missing Values
# ─────────────────────────────────────────────────────────────────────────────

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply a strategy for missing values based on column dtype:
      - Numeric columns  → fill with the column median
      - String columns   → fill with the literal "Unknown"
      - Date columns     → leave as NaT (propagate naturally)

    Then drop rows that are still entirely empty.

    Args:
        df: DataFrame potentially containing NaN/NaT values.

    Returns:
        DataFrame with missing values handled.
    """
    before = df.isnull().sum().sum()

    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            logger.debug("Filled numeric NaN in '%s' with median=%.4f", col, median_val)

        elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("Unknown")
            logger.debug("Filled string NaN in '%s' with 'Unknown'", col)

        # Date/datetime columns: leave as NaT

    # Drop rows where every column is null (completely empty rows)
    df = df.dropna(how="all")

    after = df.isnull().sum().sum()
    logger.info(
        "Missing values handled | before: %d | after: %d", before, after
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Remove Duplicates
# ─────────────────────────────────────────────────────────────────────────────

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop fully duplicate rows and reset the index.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with duplicate rows removed.
    """
    before = len(df)
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    removed = before - len(df)

    if removed:
        logger.info("Removed %d duplicate row(s).", removed)
    else:
        logger.info("No duplicate rows found.")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Data Type Casting
# ─────────────────────────────────────────────────────────────────────────────

def cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Intelligently coerce column data types:
      - Columns with 'date' or 'time' in their name → pd.to_datetime
      - Columns with 'id' suffix that are strings   → try numeric cast
      - Numeric-looking string columns              → try pd.to_numeric

    Args:
        df: Input DataFrame with potentially wrong types.

    Returns:
        DataFrame with corrected data types.
    """
    for col in df.columns:
        # ── Date/time columns ─────────────────────────────────────────────
        if any(keyword in col for keyword in ("date", "time", "created", "updated")):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                logger.debug("Cast '%s' to datetime.", col)
            except Exception:
                pass  # Leave as-is if conversion fails

        # ── ID columns (string-encoded integers) ──────────────────────────
        elif col.endswith("_id") and df[col].dtype == object:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            logger.debug("Cast ID column '%s' to Int64.", col)

        # ── Try to upcast object columns that look numeric ─────────────────
        elif df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            # Only apply if most values converted successfully (< 20% NaN introduced)
            nan_ratio = converted.isna().mean()
            if nan_ratio < 0.2:
                df[col] = converted
                logger.debug(
                    "Cast '%s' to numeric (%.0f%% success).",
                    col, (1 - nan_ratio) * 100,
                )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Merge / Concatenate Datasets
# ─────────────────────────────────────────────────────────────────────────────

def merge_datasets(
    orders: pd.DataFrame,
    products: pd.DataFrame,
    customers: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine the three cleaned DataFrames into one unified master DataFrame.

    Merge strategy:
      1. Join orders ← products  on 'product_id'  (left join — keep all orders)
      2. Join result ← customers on 'customer_id' (left join — keep all orders)

    If the expected join keys don't exist (e.g., demo/sample data), fall back
    to a simple vertical concatenation with a 'source' tag column.

    Args:
        orders    : Cleaned orders DataFrame.
        products  : Cleaned products DataFrame.
        customers : Cleaned customers DataFrame.

    Returns:
        pd.DataFrame: Unified master DataFrame.
    """
    logger.info("Merging datasets...")

    # ── Strategy A: Key-based joins ───────────────────────────────────────
    has_product_key  = "product_id"  in orders.columns and "product_id"  in products.columns
    has_customer_key = "customer_id" in orders.columns and "customer_id" in customers.columns

    if has_product_key or has_customer_key:
        merged = orders.copy()

        if has_product_key:
            # Avoid duplicate columns by suffixing conflicts
            merged = pd.merge(
                merged, products,
                on="product_id",
                how="left",
                suffixes=("", "_product"),
            )
            logger.info("Joined orders ↔ products on 'product_id'.")

        if has_customer_key:
            merged = pd.merge(
                merged, customers,
                on="customer_id",
                how="left",
                suffixes=("", "_customer"),
            )
            logger.info("Joined result ↔ customers on 'customer_id'.")

        return merged

    # ── Strategy B: Fallback — vertical concatenation ─────────────────────
    logger.warning(
        "Join keys (product_id / customer_id) not found in all datasets. "
        "Falling back to vertical concatenation."
    )
    orders["source"]    = "mysql_orders"
    products["source"]  = "sqlserver_products"
    customers["source"] = "excel_customers"

    combined = pd.concat([orders, products, customers], ignore_index=True)
    logger.info(
        "Concatenated %d datasets | total rows: %d", 3, len(combined)
    )
    return combined


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Derive New Columns
# ─────────────────────────────────────────────────────────────────────────────

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich the merged DataFrame with calculated business metrics.

    Derived columns (only added when source columns exist):
      - revenue         = unit_price × quantity
      - discount_amount = revenue × discount_pct / 100
      - net_revenue     = revenue − discount_amount
      - order_year      = year extracted from order_date
      - order_month     = month number extracted from order_date
      - is_high_value   = Boolean flag (revenue > 75th percentile)

    Args:
        df: Merged DataFrame.

    Returns:
        DataFrame enriched with derived columns.
    """
    logger.info("Adding derived columns...")

    # ── Revenue ────────────────────────────────────────────────────────────
    price_col    = next((c for c in df.columns if "price" in c or "unit_price" in c), None)
    quantity_col = next((c for c in df.columns if "quantity" in c or "qty" in c), None)

    if price_col and quantity_col:
        df["revenue"] = (
            pd.to_numeric(df[price_col], errors="coerce") *
            pd.to_numeric(df[quantity_col], errors="coerce")
        )
        logger.info("Derived 'revenue' = %s × %s", price_col, quantity_col)

        # Discount & net revenue
        discount_col = next(
            (c for c in df.columns if "discount" in c),
            None,
        )
        if discount_col:
            df["discount_amount"] = df["revenue"] * (
                pd.to_numeric(df[discount_col], errors="coerce").fillna(0) / 100
            )
            df["net_revenue"] = df["revenue"] - df["discount_amount"]
            logger.info("Derived 'discount_amount' and 'net_revenue'.")

        # High-value flag
        threshold = df["revenue"].quantile(0.75)
        df["is_high_value"] = df["revenue"] > threshold
        logger.info(
            "Derived 'is_high_value' flag (threshold=%.2f).", threshold
        )
    else:
        logger.warning(
            "Could not derive 'revenue': columns 'price/unit_price' and "
            "'quantity/qty' not both found. Available columns: %s",
            list(df.columns),
        )

    # ── Date parts ────────────────────────────────────────────────────────
    date_col = next(
        (c for c in df.columns if "order_date" in c or ("date" in c and "created" not in c)),
        None,
    )
    if date_col and pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df["order_year"]  = df[date_col].dt.year
        df["order_month"] = df[date_col].dt.month
        logger.info(
            "Derived 'order_year' and 'order_month' from '%s'.", date_col
        )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Master Transformer (calls all steps)
# ─────────────────────────────────────────────────────────────────────────────

def run_transformation(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Apply the full transformation pipeline to the raw datasets.

    Args:
        datasets: Dict produced by extract.run_extraction():
                  {"orders": df, "products": df, "customers": df}

    Returns:
        pd.DataFrame: Cleaned, merged, and enriched master DataFrame.
    """
    logger.info("=" * 60)
    logger.info("TRANSFORMATION PHASE STARTED")
    logger.info("=" * 60)

    cleaned: Dict[str, pd.DataFrame] = {}

    # Apply per-dataset cleaning steps
    for name, df in datasets.items():
        logger.info("--- Transforming dataset: '%s' ---", name)
        df = standardize_column_names(df)
        df = handle_missing_values(df)
        df = remove_duplicates(df)
        df = cast_data_types(df)
        cleaned[name] = df
        logger.info(
            "Dataset '%s' cleaned | rows: %d | columns: %d",
            name, len(df), len(df.columns),
        )

    # Merge all cleaned datasets into one
    master_df = merge_datasets(
        orders=cleaned["orders"],
        products=cleaned["products"],
        customers=cleaned["customers"],
    )

    # Add derived / calculated columns
    master_df = add_derived_columns(master_df)

    logger.info(
        "TRANSFORMATION PHASE COMPLETE | final shape: %s", master_df.shape
    )
    return master_df
