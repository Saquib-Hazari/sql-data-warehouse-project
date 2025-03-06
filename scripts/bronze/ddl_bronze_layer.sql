/*
 ==========================================
 This is DDL bronze layer table creation function, it consists of Dropping the table then importing the table.
 ------------------------------------------
 how to use: 
 Run this program which will first delete the table and then recreate it.
 
 Warning: 
 User should have a backup data to restore because the below code delete the table and recreates them.
 ============================================
 */
-- 🚀 Ensure the bronze schema exists
CREATE SCHEMA IF NOT EXISTS bronze;
-- 🚀 Drop existing tables if they exist
DROP TABLE IF EXISTS bronze.crm_cust_info;
DROP TABLE IF EXISTS bronze.crm_prd_info;
DROP TABLE IF EXISTS bronze.crm_sales_details;
DROP TABLE IF EXISTS bronze.erp_cust_az12;
DROP TABLE IF EXISTS bronze.erp_loc_a101;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
-- 🚀 Create bronze table for Customer Information
CREATE TABLE bronze.crm_cust_info (
  cst_id TEXT,
  cst_key TEXT,
  cst_firstname TEXT,
  cst_lastname TEXT,
  cst_marital_status TEXT,
  cst_gndr TEXT,
  cst_create_date TEXT
);
-- 🚀 Create bronze table for Product Information
CREATE TABLE bronze.crm_prd_info (
  prd_id TEXT,
  prd_key TEXT,
  prd_nm TEXT,
  prd_cost TEXT,
  prd_line TEXT,
  prd_start_dt TEXT,
  prd_end_dt TEXT
);
-- 🚀 Create bronze table for Sales Details
CREATE TABLE bronze.crm_sales_details (
  sls_ord_num TEXT,
  sls_prd_key TEXT,
  sls_cust_id TEXT,
  sls_order_dt TEXT,
  sls_ship_dt TEXT,
  sls_due_dt TEXT,
  sls_sales TEXT,
  sls_quantity TEXT,
  sls_price TEXT
);
-- 🚀 Create bronze table for ERP Customer Data
CREATE TABLE bronze.erp_cust_az12 (cid TEXT, bdate TEXT, gen TEXT);
-- 🚀 Create bronze table for ERP Location Data
CREATE TABLE bronze.erp_loc_a101 (cid TEXT, cntry TEXT);
-- 🚀 Create bronze table for ERP Product Category
CREATE TABLE bronze.erp_px_cat_g1v2 (
  id TEXT,
  cat TEXT,
  subcat TEXT,
  maintenance TEXT
);