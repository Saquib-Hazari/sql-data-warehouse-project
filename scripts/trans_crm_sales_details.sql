/* 
 =====================================
 Inserting the silver.crm_sales_details table data
 -------------------------------------
 Work Done:
 Data Cleaning
 Data Transformation.
 Handling Missing values.
 Handling Invalid data.
 =======================================
 */
DO $$ BEGIN RAISE NOTICE 'Truncating the table silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
RAISE NOTICE 'Inserting the data into silver.crm_sales_details';
INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
  )
SELECT sls_ord_num,
  sls_prd_key,
  sls_cust_id::INTEGER,
  CASE
    WHEN LENGTH(sls_order_dt) != 8
    OR sls_order_dt::INTEGER = 0 THEN NULL
    ELSE TO_DATE(sls_order_dt, 'YYYYMMDD')
  END AS sls_order_dt,
  CASE
    WHEN LENGTH(sls_ship_dt) != 8
    OR sls_ship_dt::INTEGER = 0 THEN NULL
    ELSE TO_DATE(sls_ship_dt, 'YYYYMMDD')
  END AS sls_ship_dt,
  CASE
    WHEN LENGTH(sls_due_dt) != 8
    OR sls_due_dt::INTEGER = 0 THEN NULL
    ELSE TO_DATE(sls_due_dt, 'YYYYMMDD')
  END AS sls_due_dt,
  CASE
    WHEN sls_sales::INTEGER IS NULL
    OR sls_sales::INTEGER <= 0
    OR sls_sales::INTEGER != sls_price::INTEGER * sls_quantity::INTEGER THEN sls_quantity::INTEGER * ABS(sls_price::INTEGER)
    ELSE sls_sales::INTEGER
  END AS sls_sales,
  sls_quantity::INTEGER,
  CASE
    WHEN sls_price::INTEGER IS NULL
    OR sls_price::INTEGER <= 0 THEN sls_sales::INTEGER / NULLIF(sls_quantity::INTEGER, 0)
    ELSE sls_price::INTEGER
  END AS sls_price
FROM bronze.crm_sales_details;
RAISE NOTICE 'Successfully Inserted the data into silver.crm_sales_details';
END $$