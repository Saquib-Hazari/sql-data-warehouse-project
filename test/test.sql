-- Checking the erp.sale column
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE id NOT IN(
    SELECT cst_id
    FROM silver.crm_prd_info
  ) -- Checking the cat column
SELECT DISTINCT(cat)
FROM silver.erp_px_cat_g1v2 -- Checking for the subcat
SELECT DISTINCT(maintenance)
FROM silver.erp_px_cat_g1v2
SELECT table_name,
  column_name,
  data_type
FROM information_schema.columns
WHERE table_schema = 'gold'
ORDER BY table_name,
  ordinal_position;
SELECT column_name,
  data_type
FROM information_schema.columns
WHERE table_name = 'gold.dim_products'
ORDER BY ordinal_position;
-- Testing for the joins
SELECT DISTINCT ci.cst_gender,
  ca.gen,
  CASE
    WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- created from the master table.
    ELSE COALESCE(ca.gen, 'n/a')
  END AS new_gen
FROM silver.crm_cust_info ci
  LEFT JOIN silver.erp_cust_az12 ca ON ca.cid = ci.cst_key
ORDER BY 1,
  2 --- Testing the gold.dim_customer table
SELECT *
FROM gold.dim_customer
SELECT *
FROM gold.dim_products
SELECT *
FROM gold.fact_sales -- Checking for the NULLs 
SELECT *
FROM gold.fact_sales fs
  LEFT JOIN gold.dim_customer dc ON dc.customer_key = fs.customer_key
  LEFT JOIN gold.dim_products dp ON dp.product_key = fs.product_key
WHERE dp.product_key IS NULL