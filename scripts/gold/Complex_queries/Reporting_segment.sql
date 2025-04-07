/*
======================================================
Purpose: 
  This is a Report consolidate key customers metrics and behavior.

Highlight:
  Gather essential field such as name, age, and transaction details.
  - Segment customers into category (VIP, Regular, New) and age group.
  - Aggregate customer level metrics.
  - total orders
  - total sales
  - total quantity
  - total produces
  - total life span (month)

  Calculate valuable KPIs:
  - recency month since last order.

========================================================
*/

-- (followed by your full query here)
CREATE OR REPLACE VIEW gold.report_customers AS 
WITH base_query AS (
  SELECT 
    f.order_number,
    f.product_key,
    f.customer_key,
    f.order_date,
    f.quantity,
    f.sales,
    c.customer_number,
    c.country,
    CONCAT(c.customer_firstname, ' ', c.customer_lastname) AS customer_name,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.birth_date))::INTEGER AS age
  FROM gold.fact_sales_table f 
  LEFT JOIN gold.dim_customer_table c 
    ON c.customer_id = f.customer_key
  WHERE c.customer_number IS NOT NULL
),

aggregated_metrics AS (
  SELECT 
    customer_key,
    customer_number,
    customer_name,
    country,
    age,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(sales) AS total_sales,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT product_key) AS total_product,
    MAX(order_date) AS last_order_date,
    CAST(DATE_PART('month', AGE(MAX(order_date), MIN(order_date))) 
      + (DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12) AS INTEGER) AS lifespan,
    CAST(DATE_PART('month', AGE(CURRENT_DATE, MAX(order_date))) 
      + (DATE_PART('year', AGE(CURRENT_DATE, MAX(order_date))) * 12) AS INTEGER) AS recency_months
  FROM base_query
  GROUP BY customer_key, customer_number, customer_name, age, country
)

SELECT 
  customer_key,
  customer_number,
  customer_name,
  country,
  age,
  total_orders,
  total_sales,
  total_quantity,
  total_product,
  last_order_date,
  lifespan,
  recency_months,
  CASE 
    WHEN age < 20 THEN 'Below 20'
    WHEN age BETWEEN 20 AND 29  THEN '20 - 30'
    WHEN age BETWEEN 30 AND 39 THEN '30 - 40'
    WHEN age BETWEEN 40 AND 49 THEN '40 - 50'
    ELSE  '50 Above'
  END AS age_group,
  CASE 
    WHEN lifespan > 12 AND total_sales > 5000 THEN 'VIP'
    WHEN lifespan > 12 AND total_sales <= 5000 THEN 'REGULAR'
    ELSE 'NEW'
  END AS customer_segment,
  CASE 
    WHEN total_sales = 0 THEN 0
    ELSE ROUND(total_sales / total_orders, 2)
  END AS avg_order_value,
  CASE 
    WHEN lifespan = 0 THEN total_sales  
    ELSE ROUND(total_sales::NUMERIC / lifespan::NUMERIC, 2)
  END AS avg_monthly_spent
FROM aggregated_metrics;