/*
 =======================================
 Inserting into the silver_erp_px_cat_g1v2 table
 ---------------------------------
 Work Done:
 Data Cleansing
 Data Trimming
 Duplicate values
 cleaning the Null values
 =========================================
 */
DO $$ BEGIN RAISE NOTICE 'Truncating the table silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
RAISE NOTICE 'Inserting the data in silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT id,
  cat,
  subcat,
  maintenance
FROM bronze.erp_px_cat_g1v2;
RAISE NOTICE 'Successfully inserted the data into silver.erp_px_cat_g1v2';
END $$