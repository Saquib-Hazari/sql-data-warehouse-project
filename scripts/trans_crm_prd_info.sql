/*
 =========================================
 Inserting the silver.crm_prd_info data. This data is fully cleaned and derived from the bronze table.
 ----------------------------------------
 Work Done:
 Derived Columns
 Data Enrichment
 Handling missing values and NULL values.
 Data Transformations.
 -------------------------------------------
 ============================================
 */
DO $$ BEGIN RAISE NOTICE 'Truncating the table silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
RAISE NOTICE 'Inserting data to silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
    prd_id,
    cst_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
  )
SELECT prd_id::INTEGER,
  -- ✅ Convert TEXT to INTEGER
  REPLACE(
    SUBSTRING(
      prd_key
      FROM 1 FOR 5
    ),
    '-',
    '_'
  ) AS cst_id,
  SUBSTRING(
    prd_key
    FROM 7 FOR LENGTH(prd_key)
  ) AS prd_key,
  prd_nm,
  COALESCE(prd_cost::NUMERIC(10, 2), 0) AS prd_cost,
  -- ✅ Ensure numeric conversion
  CASE
    UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    ELSE 'n/a'
  END AS prd_line,
  CAST(prd_start_dt AS DATE) AS prd_start_dt,
  CAST(
    LEAD(prd_start_dt::DATE) OVER(
      PARTITION BY prd_key
      ORDER BY prd_start_dt::DATE
    ) - INTERVAL '1 day' AS DATE
  ) AS prd_end_dt
FROM bronze.crm_prd_info;
RAISE NOTICE 'Successfully inserted the data into silver.crm_prd_info';
END $$;