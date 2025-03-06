/*
 ========================================================
 Stored Procedure
 ========================================================
 Script Purpose:
 This script performes ETL (Extract , transform , load ) process
 populate the data for silver layer from bronze schema.
 Action performed: 
 1) Truncate the silver table.
 2) Insert Transformed and cleansed data from bronze into silver layer.
 Parameter:
 NONE
 This stored procedure does not accept any parameter or return any values.
 
 Usage example:
 CALL silver.load_silver_layer();
 
 ---------------------------------------------------------
 */
CREATE OR REPLACE PROCEDURE silver.load_silver_layer() LANGUAGE plpgsql AS $$
DECLARE start_time TIMESTAMP;
end_time TIMESTAMP;
total_duration INTERVAL;
row_count INT;
BEGIN -- 🚀 Log batch start time
start_time := clock_timestamp();
RAISE NOTICE '🚀 Loading Silver Layer Started at: %',
start_time;
-- 🛑 Step 1: Load crm_cust_info
BEGIN RAISE NOTICE '🗑️ Truncating table silver.crm_cust_info...';
TRUNCATE TABLE silver.crm_cust_info;
RAISE NOTICE '📂 Inserting data into silver.crm_cust_info...';
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
  )
SELECT cst_id::INTEGER,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname) AS cst_lastname,
  CASE
    WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
    WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
    ELSE 'n/a'
  END AS cst_gndr,
  CASE
    WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
    WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
    ELSE 'n/a'
  END AS cst_marital_status,
  cst_create_date::DATE
FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY cst_id
        ORDER BY cst_create_date DESC
      ) AS flag_last
    FROM bronze.crm_cust_info
  ) AS subquery
WHERE flag_last = 1;
-- ✅ Count inserted rows
SELECT COUNT(*) INTO row_count
FROM silver.crm_cust_info;
RAISE NOTICE '✅ silver.crm_cust_info: % rows inserted.',
row_count;
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Error inserting into silver.crm_cust_info: %',
SQLERRM;
END;
-- 🛑 Step 2: Load crm_prd_info
BEGIN RAISE NOTICE '🗑️ Truncating table silver.crm_prd_info...';
TRUNCATE TABLE silver.crm_prd_info;
RAISE NOTICE '📂 Inserting data into silver.crm_prd_info...';
INSERT INTO silver.crm_prd_info (
    prd_id,
    cst_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
  )
SELECT prd_id::INTEGER,
  REPLACE(
    SUBSTRING(
      prd_key
      FROM 1 FOR 5
    ),
    '-',
    '_'
  ) AS cst_id,
  SUBSTRING(
    prd_key
    FROM 7 FOR LENGTH(prd_key)
  ) AS prd_key,
  prd_nm,
  COALESCE(prd_cost::NUMERIC(10, 2), 0) AS prd_cost,
  CASE
    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
  END AS prd_line,
  CAST(prd_start_dt AS DATE) AS prd_start_dt,
  CAST(
    LEAD(prd_start_dt::DATE) OVER (
      PARTITION BY prd_key
      ORDER BY prd_start_dt::DATE
    ) - INTERVAL '1 day' AS DATE
  ) AS prd_end_dt
FROM bronze.crm_prd_info;
-- ✅ Count inserted rows
SELECT COUNT(*) INTO row_count
FROM silver.crm_prd_info;
RAISE NOTICE '✅ silver.crm_prd_info: % rows inserted.',
row_count;
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Error inserting into silver.crm_prd_info: %',
SQLERRM;
END;
-- 🛑 Step 3: Load crm_sales_details
BEGIN RAISE NOTICE '🗑️ Truncating table silver.crm_sales_details...';
TRUNCATE TABLE silver.crm_sales_details;
RAISE NOTICE '📂 Inserting data into silver.crm_sales_details...';
INSERT INTO silver.crm_sales_details (
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
  sls_sales::NUMERIC(10, 2),
  sls_quantity::INTEGER,
  sls_price::NUMERIC(10, 2)
FROM bronze.crm_sales_details;
-- ✅ Count inserted rows
SELECT COUNT(*) INTO row_count
FROM silver.crm_sales_details;
RAISE NOTICE '✅ silver.crm_sales_details: % rows inserted.',
row_count;
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Error inserting into silver.crm_sales_details: %',
SQLERRM;
END;
-- 🚀 Capture End Time
end_time := clock_timestamp();
total_duration := end_time - start_time;
-- ✅ Log Completion
RAISE NOTICE '✅ Silver Layer Load Completed at %',
end_time;
RAISE NOTICE '⏳ Total Duration: % seconds.',
EXTRACT(
  EPOCH
  FROM total_duration
);
END $$;