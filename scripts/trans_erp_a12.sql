/*
 ============================================
 Inserting the table content for silver.erp_cust_az12 
 ---------------------------------------
 Work Done:
 Handling invalid values.
 Data normalizations.
 ============================================
 */
DO $$ BEGIN RAISE NOTICE 'Truncating the table silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
RAISE NOTICE 'Inserting the data into the silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
SELECT CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
  END AS cid,
  CASE
    WHEN bdate::DATE > CURRENT_DATE THEN NULL
    ELSE bdate::DATE
  END AS bdate,
  CASE
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
  END AS gen
FROM bronze.erp_cust_az12;
RAISE NOTICE 'Successfully inserted the data into the silver.erp_cust_az12';
END $$