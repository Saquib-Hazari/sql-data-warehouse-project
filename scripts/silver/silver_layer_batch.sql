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
CREATE OR REPLACE PROCEDURE silver.load_silver_layer()
LANGUAGE plpgsql
AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  total_duration INTERVAL;
  row_count INT;
BEGIN
  -- Start time logging
  start_time := clock_timestamp();
  RAISE NOTICE '==================================';
  RAISE NOTICE 'LOADING SILVER LAYER';
  RAISE NOTICE '==================================';

  RAISE NOTICE '-- Batch started at: %', start_time;

  -- CRM: Customer Info
  BEGIN
    RAISE NOTICE 'Truncating silver.crm_cust_info...';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE 'Inserting into silver.crm_cust_info...';
    INSERT INTO silver.crm_cust_info (
      cst_id, cst_key, cst_firstname, cst_lastname,
      cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT 
      cst_id::INTEGER,
      cst_key,
      TRIM(cst_firstname),
      TRIM(cst_lastname),
      CASE
        WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
        WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
        ELSE 'n/a'
      END,
      CASE
        WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
        WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
        ELSE 'n/a'
      END,
      cst_create_date::DATE
    FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
      FROM bronze.crm_cust_info
    ) t
    WHERE flag_last = 1;

    SELECT COUNT(*) INTO row_count FROM silver.crm_cust_info;
    RAISE NOTICE '-- Total rows inserted in crm_cust_info: %', row_count;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE WARNING '❌ Error inserting into crm_cust_info: %', SQLERRM;
  END;

  -- CRM: Product Info
  BEGIN
    RAISE NOTICE 'Truncating silver.crm_prd_info...';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE 'Inserting into silver.crm_prd_info...';
    INSERT INTO silver.crm_prd_info (
      prd_id, cat_id, prd_key, prd_nm,
      prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT 
      prd_id::INTEGER,
      REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
      SUBSTRING(prd_key, 7),
      prd_nm,
      COALESCE(prd_cost::INTEGER, 0),
      CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
      END,
      prd_start_dt::DATE,
      (LEAD(prd_start_dt::DATE) OVER (
        PARTITION BY prd_key ORDER BY prd_start_dt::DATE
      ) - INTERVAL '1 day')::DATE
    FROM bronze.crm_prd_info;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE WARNING '❌ Error inserting into crm_prd_info: %', SQLERRM;
  END;

  -- CRM: Sales Details
  BEGIN
    RAISE NOTICE 'Truncating silver.crm_sales_details...';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE 'Inserting into silver.crm_sales_details...';
    INSERT INTO silver.crm_sales_details (
      sls_ord_num, sls_prd_key, sls_cust_id,
      sls_order_dt, sls_ship_dt, sls_due_dt,
      sls_sales, sls_quantity, sls_price
    )
    SELECT 
      sls_ord_num,
      sls_prd_key,
      sls_cust_id::INTEGER,
      CASE 
        WHEN sls_order_dt ~ '^\d{8}$' AND sls_order_dt::INTEGER != 0 THEN TO_DATE(sls_order_dt, 'YYYYMMDD')
        ELSE NULL
      END,
      CASE 
        WHEN sls_ship_dt ~ '^\d{8}$' AND sls_ship_dt::INTEGER != 0 THEN TO_DATE(sls_ship_dt, 'YYYYMMDD')
        ELSE NULL
      END,
      CASE 
        WHEN sls_due_dt ~ '^\d{8}$' AND sls_due_dt::INTEGER != 0 THEN TO_DATE(sls_due_dt, 'YYYYMMDD')
        ELSE NULL
      END,
      CASE 
        WHEN sls_sales IS NULL OR sls_sales::INTEGER <= 0 OR 
             sls_sales::INTEGER != sls_quantity::INTEGER * ABS(sls_price::INTEGER)
        THEN sls_quantity::INTEGER * ABS(sls_price::INTEGER)
        ELSE sls_sales::INTEGER
      END,
      sls_quantity::INTEGER,
      CASE 
        WHEN sls_price IS NULL OR sls_price::INTEGER <= 0
        THEN sls_sales::INTEGER / NULLIF(sls_quantity::INTEGER, 0)
        ELSE sls_price::INTEGER
      END
    FROM bronze.crm_sales_details;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE WARNING '❌ Error inserting into crm_sales_details: %', SQLERRM;
  END;

  -- ERP: Customer AZ12
  BEGIN
    RAISE NOTICE 'Truncating silver.erp_cust_az12...';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE 'Inserting into silver.erp_cust_az12...';
    INSERT INTO silver.erp_cust_az12 (
      cid, bdate, gen
    )
    SELECT 
      CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
      END,
      CASE 
        WHEN bdate ~ '^\d{4}-\d{2}-\d{2}$' AND bdate::DATE <= CURRENT_DATE
        THEN bdate::DATE
        ELSE NULL
      END,
      CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
      END
    FROM bronze.erp_cust_az12;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE WARNING '❌ Error inserting into erp_cust_az12: %', SQLERRM;
  END;

  -- ERP: Location A101
  BEGIN
    RAISE NOTICE 'Truncating silver.erp_loc_a101...';
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE 'Inserting into silver.erp_loc_a101...';
    INSERT INTO silver.erp_loc_a101 (
      cid, cntry
    )
    SELECT 
      REPLACE(cid, '-', '') AS cid,
      CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
        ELSE TRIM(cntry)
      END
    FROM bronze.erp_loc_a101;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE WARNING '❌ Error inserting into erp_loc_a101: %', SQLERRM;
  END;

  -- ERP: PX Category G1V2
  BEGIN
    RAISE NOTICE 'Truncating silver.erp_px_cat_g1v2...';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE 'Inserting into silver.erp_px_cat_g1v2...';
    INSERT INTO silver.erp_px_cat_g1v2 (
      id, cat, subcat, maintenance
    )
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE WARNING '❌ Error inserting into erp_px_cat_g1v2: %', SQLERRM;
  END;

  -- End time logging
  end_time := clock_timestamp();
  total_duration := end_time - start_time;

  RAISE NOTICE '-- Batch completed at: %', end_time;
  RAISE NOTICE '-- Total time duration: % seconds', EXTRACT(EPOCH FROM total_duration);

END;
$$;

CALL silver.load_silver_layer();