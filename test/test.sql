-- Checking the second table silver.crm_prd_info table for issues
SELECT *
FROM bronze.crm_prd_info -- Checking for NULL and duplicates in primary key.
SELECT prd_id,
  COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 -- Creating prd_key for the customer table 
SELECT prd_id,
  prd_key,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cst_id,
  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LENGTH(prd_key)) IN (
    SELECT sls_prd_key
    FROM bronze.crm_sales_details
  ) -- Checking for the pr_nm column for
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) -- Check weather we have negative number or negative cost
  -- expected result: No result
SElECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost::INTEGER < 0
  OR prd_cost::INTEGER IS NULL