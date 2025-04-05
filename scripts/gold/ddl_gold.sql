/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
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
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
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
WHERE     pn.prd_end_dt IS NULL; -- To filter out all historical data - old data consists of end date. With this u can remove prd_end_dt from SELECT as it is NULL.
-- Dimention Table
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
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
ON        sd.sls_cust_id = cu.customer_id;
-- Fact table - Keys: Numbersm keys, ids. Dates. measure: sales, qty, price
-- Fact connects multiple dimensions. Here we need to present the surogate keys that come from the dimensions.
-- **The sls_prd_key and sls_cust_id come from source system. Since we need to connect Fact with Dim using Surrogate Keys we need to replace these two with the created Dim keys namely customer_key and product_key.
-- We are doing Data Lookup - Joining tables to get one info
-- Result: Dimension Keys - order_number, product_kay, customer_key. Dates- order_date, shipping_date, due_date. Measures: sales_amount, quantity, price.
GO
