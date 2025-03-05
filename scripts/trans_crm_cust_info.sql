/* Inserting the silver crm_cust_info table
 
 -------------------------------------------
 Work done: 
 Removed Unwanted Spaces.
 Data Normalization.
 Data Filtering.
 Handling Missing data.
 Removing Duplicates.
 ------------------------------------------
 */
/* Inserting data into silver.crm_cust_info table */
DO $$ BEGIN -- 🚀 Truncate the table
RAISE NOTICE '🗑️ Truncating table silver.crm_cust_info...';
TRUNCATE TABLE silver.crm_cust_info;
-- 🚀 Inserting data
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
    SELECT *
    FROM (
        SELECT *,
          ROW_NUMBER() OVER(
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
          ) AS flag_last
        FROM bronze.crm_cust_info
      ) AS subquery
    WHERE flag_last = 1
  ) AS final_query;
-- 🚀 Final confirmation
RAISE NOTICE '✅ Data successfully loaded into the table!';
END $$;