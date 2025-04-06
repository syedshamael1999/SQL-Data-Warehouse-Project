/*
===============================================================================
Exploratory Checks On Silver Layer
===============================================================================
Script Purpose:
    This script performs critical exploratory checks on Silver Layer tables to 
    ensure data quality before building Gold Layer Views (dimensions & fact - Star Schema). It helps 
    validate that incoming data is clean, consistent, and reliable.

Usage Notes:
   - Run this script after Silver Layer load but before building Gold Layer Views.
   - Review all anomalies, clean data as needed, and document exceptions or fixes.
   - Serves as a foundation for building trusted dimension and fact tables.
===============================================================================
*/

-- ===========================================================================
-- Customers Dimension
-- ===========================================================================
-- Table Links: 1. crm_cust_info (cst_key) == erp_cust_az12 (cid)
--              2. crm_cust_info (cst_key) == erp_loc_a101 (cid)
-- crm_cust_info = Left Table
SELECT 
     ci.cst_id,
	 ci.cst_key,
	 ci.cst_firstname,
	 ci.cst_lastname,
	 ci.cst_marital_status,
	 ci.cst_gndr,
	 ci.cst_create_date,
	 ca.bdate,
	 ca.gen,
	 la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca -- Avoid INNER JOIN as the other table might not have all info about customers as the Left table. So you might not get full info. Stick to LEFT as all info from the Master table is present 
ON        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON        ci.cst_key = la.cid


SELECT * FROM silver.crm_cust_info
SELECT * FROM silver.erp_cust_az12
SELECT * FROM silver.erp_loc_a101

-- Check Duplicates
SELECT cst_id, COUNT(*)
FROM(SELECT 
     ci.*,
	 ca.bdate,
	 ca.gen,
	 la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca -- Avoid INNER JOIN as the other table might not have all info about customers as the Left table. So you might not get full info. Stick to LEFT as all info from the Master table is present 
ON        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON        ci.cst_key = la.cid
)t GROUP BY cst_id HAVING COUNT(*) >1
-- No Duplicates

-- Data Integration (as there are 2 gender columns)
SELECT 
     DISTINCT 
	 ci.cst_gndr,
	 ca.gen,
	 CASE 
	     WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- When info in CRM is not n/a meaning that infor is either Mail or Female then take infor from CRM (Master for gender info)
		 ELSE COALESCE(ca.gen, 'n/a') -- Otherwise take info from ERP. Also convert NULL to n/a
		 END AS new_gen -- Data Integration: Integrated 2 columns with same description
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca -- Avoid INNER JOIN as the other table might not have all info about customers as the Master table. So you might not get full info. Stick to LEFT as all info from the Master table is present 
ON        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON        ci.cst_key = la.cid
-- Findings: 1. Unmatching info: left row has male, right row has female. ** Determine wthr CRM or ERP is the Master. Here its CRM** so the CRM info is more accurate (So the left table (CRM) (cst_gndr) is accurate
--           2. Left row gender, right row n/a
--           3. Left row n/a, right row gender
--           4. Left row n/a, right row NULL - NULL due to no matches when tables are joined.


-- Final Query (Use this to create views)
SELECT 
          ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- Surrogate Key
		  ci.cst_id                          AS customer_id,
		  ci.cst_key                         AS customer_number,
		  ci.cst_firstname	                 AS first_name,
		  ci.cst_lastname                    AS last_name,
		  la.cntry                           AS country,
		  ci.cst_marital_status              AS marital_status,
	      CASE 
	          WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- When info in CRM is not n/a meaning that infor is either Mail or Female then take infor from CRM (Master for gender info)
		      ELSE COALESCE(ca.gen, 'n/a') -- Otherwise take info from ERP. Also convert NULL to n/a
	      END                                AS gender, -- Data Integration: Integrated 2 columns with same description
	      ca.bdate                           AS birthdate,
	      ci.cst_create_date                 AS create_date
FROM      silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca -- Avoid INNER JOIN as the other table might not have all info about customers as the Left table. So you might not get full info. Stick to LEFT as all info from the Master table is present 
ON        ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON        ci.cst_key = la.cid;
-- This table is a Dimension as it contains descriptions about customers.

-- ===========================================================================
-- Products Dimension
-- ===========================================================================
-- Table Links: crm_prd_info (prd_key) == erp_px_cat_g1v2 (id) - According to Data Integration Model. However we had added cat_id column in silver.crm_prd_info so we will use that to link
-- crm_prd_info (cat_id) == erp_px_cat_g1v2 (id)


SELECT * FROM silver.crm_prd_info pn
SELECT * FROM silver.erp_px_cat_g1v2 pc

SELECT 
		  pn.prd_id,
		  pn.cat_id,
		  pn.prd_key,
		  pn.prd_nm,
		  pn.prd_cost, 
		  pn.prd_line,
		  pn.prd_start_dt,
		  pc.cat,
		  pc.subcat,
		  pc.maintenance
FROM      silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON        pn.cat_id = pc.id
WHERE     prd_end_dt IS NULL -- To filter out all historical data - old data consists of end date. With this u can remove prd_end_dt from SELECT as it is NULL.


-- Check Duplicates
SELECT prd_key, COUNT(*) FROM
(SELECT 
		  pn.*,
		  pc.cat,
		  pc.subcat,
		  pc.maintenance
FROM      silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON        pn.cat_id = pc.id
WHERE     prd_end_dt IS NULL -- To filter out all historical data - old data consists of end date. With this u can remove prd_end_dt from SELECT as it is NULL.
)t GROUP BY prd_key
HAVING COUNT(*) >1
-- No Duplicates

-- Final Query (Use this to create views)
SELECT 
          ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
		  pn.prd_id       AS product_id,
		  pn.prd_key      AS product_number,
		  pn.prd_nm       AS product_name,
		  pn.cat_id       AS category_id,
		  pc.cat          AS category,
		  pc.subcat       AS subcategory,
		  pc.maintenance  AS maintenance,
		  pn.prd_cost     AS cost, 
		  pn.prd_line     AS product_line,
		  pn.prd_start_dt AS start_date
FROM      silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON        pn.cat_id = pc.id
WHERE     prd_end_dt IS NULL 
-- Dimention Table

-- ===========================================================================
-- Sales Fact
-- ===========================================================================
-- Table Links: 1. crm_sales_details (sls_prd_key) == dim_products (product_number) - [old -> crm_prd_info (prd_key)]
--              2. crm_sales_details (sls_cst_id) == dim_customers (customer_id) [old -> crm_cust_info (cst_id)]
-- crm_cust_info = Left Table

SELECT * FROM silver.crm_sales_details
SELECT * FROM gold.dim_products

SELECT * FROM silver.crm_sales_details
SELECT * FROM gold.dim_customers

-- Final Query (Use this to create views)
SELECT 
          sd.sls_ord_num  AS order_number,
		  pr.product_key  AS product_key, -- Replacing sd.sls_prd_key with Products Surrogate Key 
		  cu.customer_key AS customer_key, -- Replacing  sd.sls_cust_id with Customer Surrogate Key 
		  sd.sls_order_dt AS order_date,
		  sd.sls_ship_dt  AS shipping_date,
		  sd.sls_due_dt   AS due_date,
		  sd.sls_sales    AS sales_amount,
		  sd.sls_quantity AS quantity,
		  sd.sls_price    AS price
FROM      silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON        sd.sls_prd_key = pr.product_number  
LEFT JOIN gold.dim_customers cu
ON        sd.sls_cust_id = cu.customer_id
-- Fact table - Keys: Numbersm keys, ids. Dates. measure: sales, qty, price
-- Fact connects multiple dimensions. Here we need to present the surogate keys that come from the dimensions.
-- **The sls_prd_key and sls_cust_id come from source system. Since we need to connect Fact with Dim using Surrogate Keys we need to replace these two with the created Dim keys namely customer_key and product_key.
-- We are doing Data Lookup - Joining tables to get one info
-- Result: Dimension Keys - order_number, product_kay, customer_key. Dates- order_date, shipping_date, due_date. Measures: sales_amount, quantity, price.
