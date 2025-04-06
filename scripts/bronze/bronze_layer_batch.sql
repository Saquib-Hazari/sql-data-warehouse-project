/* 
 ============================================= 
 Creating the bronze layer for the procedure to import csv and the batch process to check the time taken for over all batch process
 --------------------------------------------
 Running this file will Truncate the file and then recreate the table and insert it.
 --------------------------------------------
 How to use:
 CALL bronze.import_bronze_layer(); to call the function 
 ============================================
 */
CREATE OR REPLACE PROCEDURE bronze.import_bronze_layer() LANGUAGE plpgsql AS $$
DECLARE batch_start_time TIMESTAMP;
batch_end_time TIMESTAMP;
total_duration INTERVAL;
BEGIN -- 🚀 Log batch start time
batch_start_time := clock_timestamp();
RAISE NOTICE '==================================';
RAISE NOTICE 'LOADING BRONZE LAYER';
RAISE NOTICE '==================================';
RAISE NOTICE '🚀 Starting Bronze Layer Load at %',
batch_start_time;
-- 🛑 Step 1: Truncate the tables before loading
BEGIN RAISE NOTICE 'Truncating Bronze tables...';
TRUNCATE TABLE bronze.crm_cust_info,
bronze.crm_prd_info,
bronze.crm_sales_details,
bronze.erp_cust_az12,
bronze.erp_loc_a101,
bronze.erp_px_cat_g1v2;
RAISE NOTICE '✅ Truncation completed.';
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Failed to truncate tables: %',
SQLERRM;
END;
-- 🚀 Step 2: Load data from CSV files into Bronze tables
BEGIN RAISE NOTICE '📂 Loading data into Bronze Layer...';
COPY bronze.crm_cust_info
FROM '/Users/saquibhazari/DEVELOPERS/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' DELIMITER ',' CSV HEADER;
RAISE NOTICE '✅ Loaded cust_info.csv.';
COPY bronze.crm_prd_info
FROM '/Users/saquibhazari/DEVELOPERS/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' DELIMITER ',' CSV HEADER;
RAISE NOTICE '✅ Loaded prd_info.csv.';
COPY bronze.crm_sales_details
FROM '/Users/saquibhazari/DEVELOPERS/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' DELIMITER ',' CSV HEADER;
RAISE NOTICE '✅ Loaded sales_details.csv.';
COPY bronze.erp_cust_az12
FROM '/Users/saquibhazari/DEVELOPERS/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' DELIMITER ',' CSV HEADER;
RAISE NOTICE '✅ Loaded CUST_AZ12.csv.';
COPY bronze.erp_loc_a101
FROM '/Users/saquibhazari/DEVELOPERS/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' DELIMITER ',' CSV HEADER;
RAISE NOTICE '✅ Loaded LOC_A101.csv.';
COPY bronze.erp_px_cat_g1v2
FROM '/Users/saquibhazari/DEVELOPERS/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
RAISE NOTICE '✅ Loaded PX_CAT_G1V2.csv.';
EXCEPTION
WHEN OTHERS THEN RAISE WARNING '❌ Error occurred during data load: %',
SQLERRM;
END;
-- 🚀 Log batch end time
batch_end_time := clock_timestamp();
total_duration := batch_end_time - batch_start_time;
RAISE NOTICE '🎯 Bronze Layer Load Completed at %',
batch_end_time;
RAISE NOTICE '⏳ Total Batch Duration: % seconds.',
EXTRACT(
  EPOCH
  FROM total_duration
);
END;
$$;

CALL bronze.import_bronze_layer();