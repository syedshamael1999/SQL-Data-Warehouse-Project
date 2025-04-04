/*
===============================================================================
Quality Check (Silver Layer)
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
/* ========================================================
1. silver.crm_cust_info
   ======================================================== */
    
-- 1.  Check for NULLs or duplicates in Primary Key
-- Expectiation: No Result
SELECT 
      cst_id,
      COUNT(*) AS pk_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
-- Findings: No results

-- 2. Check for unwanted spaces in string values
-- Expectiation: No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
-- Findings: No Results

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE  cst_lastname != TRIM(cst_lastname) 
-- Findings: No Results

-- 3. Check data standardization and consistency of values in low cardinality columns
SELECT 
      DISTINCT(cst_gndr)
FROM silver.crm_cust_info
-- Findings: Normalized Data
SELECT 
      DISTINCT(cst_marital_status)
FROM silver.crm_cust_info
-- Findings: Normalized Data

SELECT * FROM silver.crm_cust_info

/* ========================================================
2. silver.crm_prd_info
   ======================================================== */

-- 1.  Check for NULLs or duplicates in Primary Key
SELECT 
      prd_id,
      COUNT(*) AS pk_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
-- Findings: No results

-- 2. Check for unwanted spaces in string values
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) 
-- Findings: No Results

-- 4. Check prd_cost - check quality of the numbers - NULLS or Negative Numbers
SELECT 
	 prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 
   OR prd_cost IS NULL

-- 5. Normalize / Standardize prd_line.
SELECT DISTINCT(prd_line)
FROM silver.crm_prd_info

-- 6. Start and end date - Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt 

SELECT * FROM silver.crm_prd_info

/* ========================================================
3. silver.crm_sales_details
   ======================================================== */

--Check Formula
--Business Rule: 1. Sales = Quantity * Price. 2. Negatives, Zeros & NULLs Not Allowed
SELECT DISTINCT
		sls_sales ,
		sls_quantity,
		sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales,
		sls_quantity,
		sls_price
-- Findings: No Results!

SELECT * FROM silver.crm_sales_details

/* ========================================================
4. silver.erp_cust_az12
   ======================================================== */
 
-- 1. Check table links
SELECT * FROM silver.erp_cust_az12
SELECT * FROM silver.crm_cust_info

-- 2. Check bdate (if higher than current date)
SELECT 
      bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()
-- Findings: No Results

-- 3. Check low cardinality data
SELECT 
      DISTINCT(gen)
FROM silver.erp_cust_az12
-- Findings: Normalized / Standardized

/* ========================================================
5. silver.erp_loc_a101
   ======================================================== */
 -- 1. Table Links
 -- cid of erp_loc_a101 can be linked with cid of crm_cust_info
 SELECT * FROM silver.erp_loc_a101
 SELECT * FROM silver.crm_cust_info

-- 2. Check low cardinality data
SELECT 
      DISTINCT(cntry)
FROM silver.erp_loc_a101
-- Findings: Normalized / Standardized

/* ========================================================
6. silver.erp_px_cat_g1v2
   ======================================================== */
SELECT * 
FROM silver.erp_px_cat_g1v2

/*======================================================== */
-- Unlean Data
SELECT * FROM bronze.crm_cust_info
SELECT * FROM bronze.crm_prd_info
SELECT * FROM bronze.crm_sales_details
SELECT * FROM bronze.erp_cust_az12
SELECT * FROM bronze.erp_loc_a101
SELECT * FROM bronze.erp_px_cat_g1v2

-- Cleaned Data
SELECT * FROM silver.crm_cust_info
SELECT * FROM silver.crm_prd_info
SELECT * FROM silver.crm_sales_details
SELECT * FROM silver.erp_cust_az12
SELECT * FROM silver.erp_loc_a101
SELECT * FROM silver.erp_px_cat_g1v2
