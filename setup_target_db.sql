-- =============================================================================
-- setup_target_db.sql  —  Target MySQL Database Setup
-- =============================================================================
-- Run this script once against your target MySQL server (localhost) before
-- executing the pipeline.
--
-- Connection:  mysql -u root -p < setup_target_db.sql
-- Or paste into MySQL Workbench / DBeaver.
-- =============================================================================

-- Step 1: Create the target database if it doesn't exist
-- -------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `pipeline_output`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE `pipeline_output`;

-- =============================================================================
-- Step 2: Create the destination table
-- =============================================================================
-- NOTE: The pipeline uses if_exists='replace' in config.yaml, which means
-- pandas.to_sql() will DROP and RECREATE this table on every run automatically.
--
-- This manual DDL is provided so you can:
--   a) Pre-create the table with proper indexes and constraints
--   b) Use if_exists='append' mode instead of 'replace'
--   c) Inspect the expected schema before running the pipeline
-- =============================================================================

DROP TABLE IF EXISTS `pipeline_output`;

CREATE TABLE `pipeline_output` (
  -- ── From MySQL orders table ─────────────────────────────────────────────
  `order_id`         INT             NULL,
  `customer_id`      INT             NULL,
  `product_id`       INT             NULL,
  `quantity`         DECIMAL(10,2)   NULL,
  `unit_price`       DECIMAL(10,2)   NULL,
  `discount`         DECIMAL(10,2)   NULL,      -- discount percentage
  `order_date`       DATETIME        NULL,
  `status`           VARCHAR(50)     NULL,

  -- ── From SQL Server products table ──────────────────────────────────────
  `product_name`     VARCHAR(255)    NULL,
  `category`         VARCHAR(100)    NULL,
  `stock_qty`        INT             NULL,
  `supplier`         VARCHAR(255)    NULL,

  -- ── From Excel customers sheet ──────────────────────────────────────────
  `first_name`       VARCHAR(100)    NULL,
  `last_name`        VARCHAR(100)    NULL,
  `email`            VARCHAR(255)    NULL,
  `country`          VARCHAR(100)    NULL,
  `segment`          VARCHAR(100)    NULL,
  `join_date`        DATETIME        NULL,

  -- ── Derived / calculated columns (added by transform.py) ────────────────
  `revenue`          DECIMAL(12,2)   NULL,      -- unit_price × quantity
  `discount_amount`  DECIMAL(12,2)   NULL,      -- revenue × discount / 100
  `net_revenue`      DECIMAL(12,2)   NULL,      -- revenue - discount_amount
  `is_high_value`    TINYINT(1)      NULL,      -- 1 if revenue > 75th percentile
  `order_year`       INT             NULL,      -- year(order_date)
  `order_month`      INT             NULL,      -- month(order_date)

  -- ── Row metadata ────────────────────────────────────────────────────────
  `loaded_at`        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP

) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Master output table for the automated ETL pipeline';


-- =============================================================================
-- Step 3: Useful indexes (optional — add after first load if needed)
-- =============================================================================
-- ALTER TABLE `pipeline_output` ADD INDEX idx_order_id   (`order_id`);
-- ALTER TABLE `pipeline_output` ADD INDEX idx_customer_id (`customer_id`);
-- ALTER TABLE `pipeline_output` ADD INDEX idx_product_id  (`product_id`);
-- ALTER TABLE `pipeline_output` ADD INDEX idx_order_date  (`order_date`);
-- ALTER TABLE `pipeline_output` ADD INDEX idx_segment     (`segment`);


-- =============================================================================
-- Verification
-- =============================================================================
SELECT 'Target DB setup complete!' AS status;
SHOW TABLES FROM `pipeline_output`;
