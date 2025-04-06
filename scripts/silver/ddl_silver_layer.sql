/*
 ==========================================
 This is the DDL for the Silver Layer tables.
 It first deletes existing tables and then recreates them with correct data types.
 
 Metadata Columns:
 - `dwh_create_date`: Captures when data was first inserted
 - `dwh_update_date`: Captures when data was last updated
 
 Warning: Ensure you have a backup before running this script.
 ==========================================
 */
-- 🚀 Ensure the silver schema exists
CREATE SCHEMA IF NOT EXISTS silver;
-- 🚀 Drop existing tables if they exist

-- 🚀 Create Silver Table for Customer Information
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
  cst_id INTEGER,
  cst_key TEXT,
  cst_firstname TEXT,
  cst_lastname TEXT,
  cst_marital_status TEXT,
  cst_gndr TEXT,
  cst_create_date DATE,
  dwh_create_date TIMESTAMP DEFAULT NOW(),
  -- Metadata: when data was inserted
  dwh_update_date TIMESTAMP DEFAULT NOW() -- Metadata: when data was last updated
);
-- 🚀 Create Silver Table for Product Information
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
  prd_id INTEGER,
  cat_id VARCHAR(50),
  prd_key VARCHAR(50),
  -- Ensures unique product keys
  prd_nm TEXT,
  prd_cost NUMERIC(10, 2),
  prd_line TEXT,
  prd_start_dt DATE,
  prd_end_dt DATE,
  dwh_create_date TIMESTAMP DEFAULT NOW(),
  dwh_update_date TIMESTAMP DEFAULT NOW()
);
-- 🚀 Create Silver Table for Sales Details
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
  sls_ord_num TEXT,
  sls_prd_key TEXT,
  sls_cust_id INTEGER NOT NULL,
  sls_order_dt DATE,
  sls_ship_dt DATE,
  sls_due_dt DATE,
  sls_sales NUMERIC(12, 2),
  sls_quantity INTEGER,
  sls_price NUMERIC(10, 2),
  dwh_create_date TIMESTAMP DEFAULT NOW(),
  dwh_update_date TIMESTAMP DEFAULT NOW()
);
-- 🚀 Create Silver Table for ERP Customer Data
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
  cid TEXT PRIMARY KEY,
  bdate DATE,
  gen TEXT,
  dwh_create_date TIMESTAMP DEFAULT NOW(),
  dwh_update_date TIMESTAMP DEFAULT NOW()
);
-- 🚀 Create Silver Table for ERP Location Data

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
  cid TEXT PRIMARY KEY,
  cntry TEXT,
  dwh_create_date TIMESTAMP DEFAULT NOW(),
  dwh_update_date TIMESTAMP DEFAULT NOW()
);
-- 🚀 Create Silver Table for ERP Product Category
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
  id TEXT PRIMARY KEY,
  cat TEXT,
  subcat TEXT,
  maintenance TEXT,
  dwh_create_date TIMESTAMP DEFAULT NOW(),
  dwh_update_date TIMESTAMP DEFAULT NOW()
);