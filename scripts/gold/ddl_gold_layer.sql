/* 
================================================
Creating the New table for gold layer:
This table consists of the new cleaned and joined with surrogate keys table.
Running this script will Create new tables into the gold layer.
===================================================
*/

CREATE OR REPLACE VIEW gold.dim_customer AS
SELECT ROW_NUMBER() OVER(
    ORDER BY ci.cst_id
  ) AS customer_key,
  ci.cst_id AS customer_id,
  ci.cst_key AS customer_number,
  ci.cst_firstname AS customer_firstname,
  ci.cst_lastname AS customer_lastname,
  CASE
    WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- created from the master table.
    ELSE COALESCE(ca.gen, 'n/a')
  END AS gender,
  ci.cst_marital_status AS marital_status,
  cl.cntry AS country,
  ca.bdate AS birth_date,
  ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
  LEFT JOIN silver.erp_cust_az12 ca ON ca.cid = ci.cst_key
  LEFT JOIN silver.erp_loc_a101 cl ON cl.cid = ci.cst_key -- Creating join for the prd_info and erp_prd_a12 table
CREATE OR REPLACE VIEW gold.dim_products AS
SELECT ROW_NUMBER() OVER(
    ORDER BY cp.prd_start_dt,
      cp.prd_key
  ) AS product_key,
  cp.prd_id AS product_id,
  cp.prd_key AS product_number,
  cp.prd_nm AS product_name,
  cp.cst_id AS customer_id,
  pc.cat AS category,
  pc.subcat AS subcategory,
  pc.maintenance AS maintenance,
  cp.prd_cost AS cost,
  cp.prd_line AS product_line,
  cp.prd_start_dt AS start_date
FROM silver.crm_prd_info cp
  LEFT JOIN silver.erp_px_cat_g1v2 pc ON pc.id = cp.cst_id
WHERE prd_end_dt IS NULL -- Creating join for sales table with customer and product -- keys
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT sls_ord_num AS order_number,
  dp.product_key,
  dc.customer_key,
  sls_order_dt AS order_date,
  sls_ship_dt AS shipping_date,
  sls_due_dt AS due_date,
  sls_sales AS sales,
  sls_quantity AS quantity,
  sls_price AS price
FROM silver.crm_sales_details sd
  LEFT JOIN gold.dim_products dp ON dp.product_number = sd.sls_prd_key
  LEFT JOIN gold.dim_customer dc ON dc.customer_id = sd.sls_cust_id
