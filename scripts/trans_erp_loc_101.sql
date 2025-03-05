/* 
 ========================================
 Inserting into silver.erp_loc_a101 
 ------------------------------------
 Work Done:
 Data Validation.
 Data Transformation.
 Data Standardization.
 ==========================================
 */
DO $$ BEGIN RAISE NOTICE 'Truncating the table silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
RAISE NOTICE 'Inserting the data in silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT REPLACE (cid, '-', '') AS cid,
  CASE
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) = ''
    OR cntry IS NULL THEN 'n/a'
    ELSE cntry
  END AS cntry
FROM bronze.erp_loc_a101;
RAISE NOTICE 'Successfully inserted the data into silver.erp_loc_a101';
END $$