/* 
 ==========================================
 Creating a Data import procedure where to check how much time and duration each file is taking to import.
 ------------------------------------------
 This process uses the timestamp and start time and end time which allow user to check how much time taken to import the bulk import csv files.
 -------------------------------------------
 How to use:
 CALL bronze.import_crm_data(); user this calling function to use the procedure.
 ============================================
 */
CREATE OR REPLACE PROCEDURE bronze.import_crm_data() LANGUAGE plpgsql AS $$
DECLARE start_time TIMESTAMP;
truncate_time TIMESTAMP;
import_time TIMESTAMP;
table_start_time TIMESTAMP;
table_end_time TIMESTAMP;
row_count INT;
BEGIN -- 🚀 Log the start time
start_time := clock_timestamp();
RAISE NOTICE 'Starting data import at %',
start_time;
-- 🛑 Step 1: Truncate the tables before importing
BEGIN RAISE NOTICE 'Truncating tables...';
TRUNCATE TABLE bronze.crm_cust_info,
bronze.crm_prd_info,
bronze.crm_sales_details,
bronze.erp_cust_az12,
bronze.erp_loc_a101,
bronze.erp_px_cat_g1v2;
truncate_time := clock_timestamp();
RAISE NOTICE 'Truncation completed in % seconds.',
EXTRACT(
  EPOCH
  FROM (truncate_time - start_time)
);
EXCEPTION
WHEN OTHERS THEN RAISE WARNING 'Failed to truncate tables: %',
SQLERRM;
END;
-- 🚀 Step 2: Import data table by table with row count & timing
RAISE NOTICE 'Starting CSV file imports...';
-- 📌 Import Customer Information
table_start_time := clock_timestamp();
BEGIN COPY bronze.crm_cust_info
FROM '/tmp/cust_info.csv' DELIMITER ',' CSV HEADER;
table_end_time := clock_timestamp();
SELECT COUNT(*) INTO row_count
FROM bronze.crm_cust_info;
RAISE NOTICE '✅ Imported cust_info.csv (% rows) in % seconds.',
row_count,
EXTRACT(
  EPOCH
  FROM (table_end_time - table_start_time)
);
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Failed to import cust_info.csv: %',
SQLERRM;
END;
-- 📌 Import Product Information
table_start_time := clock_timestamp();
BEGIN COPY bronze.crm_prd_info
FROM '/tmp/prd_info.csv' DELIMITER ',' CSV HEADER;
table_end_time := clock_timestamp();
SELECT COUNT(*) INTO row_count
FROM bronze.crm_prd_info;
RAISE NOTICE '✅ Imported prd_info.csv (% rows) in % seconds.',
row_count,
EXTRACT(
  EPOCH
  FROM (table_end_time - table_start_time)
);
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Failed to import prd_info.csv: %',
SQLERRM;
END;
-- 📌 Import Sales Details
table_start_time := clock_timestamp();
BEGIN COPY bronze.crm_sales_details
FROM '/tmp/sales_details.csv' DELIMITER ',' CSV HEADER;
table_end_time := clock_timestamp();
SELECT COUNT(*) INTO row_count
FROM bronze.crm_sales_details;
RAISE NOTICE '✅ Imported sales_details.csv (% rows) in % seconds.',
row_count,
EXTRACT(
  EPOCH
  FROM (table_end_time - table_start_time)
);
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Failed to import sales_details.csv: %',
SQLERRM;
END;
-- 📌 Import ERP Customer Data
table_start_time := clock_timestamp();
BEGIN COPY bronze.erp_cust_az12
FROM '/tmp/CUST_AZ12.csv' DELIMITER ',' CSV HEADER;
table_end_time := clock_timestamp();
SELECT COUNT(*) INTO row_count
FROM bronze.erp_cust_az12;
RAISE NOTICE '✅ Imported CUST_AZ12.csv (% rows) in % seconds.',
row_count,
EXTRACT(
  EPOCH
  FROM (table_end_time - table_start_time)
);
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Failed to import CUST_AZ12.csv: %',
SQLERRM;
END;
-- 📌 Import ERP Location Data
table_start_time := clock_timestamp();
BEGIN COPY bronze.erp_loc_a101
FROM '/tmp/LOC_A101.csv' DELIMITER ',' CSV HEADER;
table_end_time := clock_timestamp();
SELECT COUNT(*) INTO row_count
FROM bronze.erp_loc_a101;
RAISE NOTICE '✅ Imported LOC_A101.csv (% rows) in % seconds.',
row_count,
EXTRACT(
  EPOCH
  FROM (table_end_time - table_start_time)
);
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Failed to import LOC_A101.csv: %',
SQLERRM;
END;
-- 📌 Import ERP Product Category Data
table_start_time := clock_timestamp();
BEGIN COPY bronze.erp_px_cat_g1v2
FROM '/tmp/PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
table_end_time := clock_timestamp();
SELECT COUNT(*) INTO row_count
FROM bronze.erp_px_cat_g1v2;
RAISE NOTICE '✅ Imported PX_CAT_G1V2.csv (% rows) in % seconds.',
row_count,
EXTRACT(
  EPOCH
  FROM (table_end_time - table_start_time)
);
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Failed to import PX_CAT_G1V2.csv: %',
SQLERRM;
END;
-- 🚀 Log import completion time
import_time := clock_timestamp();
RAISE NOTICE 'Data import completed at %',
import_time;
RAISE NOTICE 'Total import time: % seconds.',
EXTRACT(
  EPOCH
  FROM (import_time - truncate_time)
);
RAISE NOTICE 'Total process time (truncate + import): % seconds.',
EXTRACT(
  EPOCH
  FROM (import_time - start_time)
);
END;
$$;