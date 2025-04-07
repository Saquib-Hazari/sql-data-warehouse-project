-- Analyze the yearly performance by comparing their sales to the both average sale performance of the product and previous year sale.

WITH yearly_product_sales AS(
  SELECT
    EXTRACT(YEAR FROM f.order_date) AS years,
    SUM(f.sales) AS sales,
    p.product_name
  FROM gold.fact_sales_table f
  LEFT JOIN gold.dim_products p ON p.product_id = f.product_key
  WHERE f.order_date IS NOT NULL
  GROUP BY EXTRACT(YEAR FROM f.order_date), p.product_name
)

SELECT 
  years,
  product_name,
  sales,
  ROUND(AVG(sales) OVER(PARTITION BY product_name),2) AS avg_sales,
  ROUND(sales - AVG(sales) OVER(PARTITION BY product_name),2) AS diff_sales,
  CASE 
    WHEN ROUND(sales - AVG(sales) OVER(PARTITION BY product_name),2) > 0 THEN 'Above avg'
    WHEN ROUND(sales - AVG(sales) OVER(PARTITION BY product_name),2) < 0 THEN 'Below avg'
    ELSE 'avg'
  END AS avg_change,
  -- Year to year analysis
  LAG(sales) OVER(PARTITION BY product_name ORDER BY years) AS py_sales,
  sales - LAG(sales) OVER(PARTITION BY product_name ORDER BY years) AS diff_py_sales,
  CASE 
    WHEN sales - LAG(sales) OVER(PARTITION BY product_name ORDER BY years) > 0 THEN 'Increase'
    WHEN sales - LAG(sales) OVER(PARTITION BY product_name ORDER BY years) < 0 THEN 'Decrease'
    ELSE 'No change'
  END AS avg_change
FROM yearly_product_sales

