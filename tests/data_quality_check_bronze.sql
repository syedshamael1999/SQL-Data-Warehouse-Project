/* Data Quality Check in Bronze Layer before inserting data into Silver Layer */

/* ========================================================
1. bronze.crm_cust_info
   ======================================================== */

SELECT * FROM bronze.crm_cust_info
    
-- 1.  Check for NULLs or duplicates in Primary Key
-- Expectiation: Primanry Key must be unique and not null. (No Result)
SELECT 
      cst_id,
      COUNT(*) AS pk_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
-- Findings: Found Duplicates and NULL

-- Pick one of the values that are duplicate and do a query on that
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466
-- Findings: Returned 3 rows of the same cst_id.
-- Next step: check the cst_create_date - there are 3 different creation dates. It means that there is an oldest and a newest record. We need the latest record
-- Next step: Rank cst_id based on the create date
SELECT *
FROM(SELECT *,
	   ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1
-- Findings: This is the duplicate-free data we need


-- 2. Check for unwanted spaces in string values
-- Expectiation: No Results
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) -- Where column is not equal to the column after trimming. Removes leading and trailing spaces
-- Findings: Unwanted spaces present

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE  cst_lastname != TRIM(cst_lastname) 
-- Findings: Unwanted spaces present

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE   cst_gndr != TRIM(cst_gndr);
-- Findings: Unwanted spaces not present
-- Solution: TRIM(cst_firstname) AS cst_firstname, TRIM(cst_lastname) AS cst_lastname,

-- 3. Check data standardization and consistency of values in low cardinality columns (M/F, Yes/No columns)
SELECT 
      DISTINCT(cst_gndr)
FROM bronze.crm_cust_info
-- Findings: Only 3 values - M, F, NULL
-- We want meaningfull values. eg: M = Male
-- Solution: CASE WHEN statement
SELECT 
      DISTINCT(cst_marital_status)
FROM bronze.crm_cust_info

-- 4. Check dates for data type
-- As defined in the data type it is indeed a date

-- Transformation Query for Silver Layer 
/*
INSERT INTO silver.crm_cust_info (
                                 cst_id, 
			                     cst_key, 
			                     cst_firstname, 
								 cst_lastname, 
								 cst_marital_status, 
								 cst_gndr,
								 cst_create_date ) -- Insert the below cleaned data into the silver layer's crm_cust_info table.
SELECT 
      cst_id,
	  cst_key,
	  TRIM(cst_firstname) AS cst_firstname, -- 2. Remove unwanted spaces in the name columns 
	  TRIM(cst_lastname) AS cst_lastname,
	   CASE 
	      WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
		  WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
		  ELSE 'n/a' -- Handle Missing data
	  END AS cst_marital_status, -- 3. (a) Normalize / Standardize marital status to a readable format
	  CASE 
	      WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' -- UPPER incase there are lower case f or m
		  WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' -- TRIM incase any new data has unwanted spaces
		  ELSE 'n/a' -- Handle Missing data
	  END AS cst_gndr, -- 3. (b) Normalize / Standardize gender to a readable format
	  cst_create_date
FROM(
      SELECT *,
	         ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
      FROM bronze.crm_cust_info
      WHERE cst_id IS NOT NULL
      )t WHERE flag_last = 1; -- 1. Remove duplicated by ranking and choosing only flags = 1
	*/


/* ========================================================
2. bronze.crm_prd_info
   ======================================================== */
SELECT * FROM bronze.crm_prd_info

-- 1.  Check for NULLs or duplicates in Primary Key
-- Expectiation: No Result
SELECT 
      prd_id,
      COUNT(*) AS pk_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
-- Findings: No results

-- 2. Split Column (prd_key)
-- Here prd_key has category id + product key. (eg: CO-RF-FR_R92B-58. first 5 char is category id as seen in erp_px_cat_g1v2)
-- However the category id in erp_px_cat_g1v2 has "_" instead of "-")
--SELECT * FROM bronze.erp_px_cat_g1v2
SELECT
     prd_id,
	 prd_key,
	 REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract from 1st position till 5th
	 SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract from 7th position till the end (dynamic)
	 prd_nm,
	 prd_cost,
	 prd_line,
	 prd_start_dt,
	 prd_end_dt
FROM bronze.crm_prd_info

-- 3. Check for unwanted spaces in string values
-- Expectiation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) -- Where column is not equal to the column after trimming. Removes leading and trailing spaces
-- Findings: No Results

-- 4. Check prd_cost - check quality of the numbers - NULLS or Negative Numbers
SELECT 
	 prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 
   OR prd_cost IS NULL
-- Findings: No negative values. But NULLs present. 
-- Chose to replace NULL with 0

-- 5. Normalize / Standardize prd_line.
SELECT DISTINCT(prd_line)
FROM bronze.crm_prd_info
-- Do CASE WHEN

-- 6. Start and end date - Check for Invalid Date Orders
-- The end date must not be earlier than the start date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt -- Check if end date is earlier than the start date
-- Findings: Here Start date is later than the end date. Meaning prd_end_dt < prd_start_dt - Incorrect!
-- Select few products and take it to EXCEL
SELECT *
FROM bronze.crm_prd_info
WHERE prd_id IN ('212', '213', '214', '215', '216', '217')
-- Build logic
SELECT
	 prd_start_dt,
	 prd_end_dt,
	 CAST(prd_start_dt AS DATE) AS prd_start_dt,
	 CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS DATE) AS prd_end_dt --  The End Date of previous record = Start Date of NEXT record MINUS 1 day.
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- Transformation Query for Silver Layer 
/* 
INSERT INTO silver.crm_prd_info (
								  prd_id,
								  cat_id,
								  prd_key,
								  prd_nm,
								  prd_cost,
								  prd_line,
								  prd_start_dt,
								  prd_end_dt)
SELECT
     prd_id,
	 REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- 2. (a) Extract from 1st position till 5th. - **Did modification to DDL of silver - Added cat_id NVARCHAR(50).
	 SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- 2. (b) Extract from 7th position till the end (dynamic)
	 prd_nm,
	 ISNULL(prd_cost, 0) AS prd_cost, -- 4. Replace NULL with 0
	 CASE UPPER(TRIM(prd_line))
	      WHEN 'M' THEN 'Mountain' 
		  WHEN 'R' THEN 'Road'
		  WHEN 'S' THEN 'Other Sales' 
		  WHEN 'T' THEN 'Touring' 
		  ELSE 'n/a' -- Handle Missing data
	  END AS prd_line, -- 5. Normalize / Standardize data
	 CAST(prd_start_dt AS DATE) AS prd_start_dt, -- **Did modification to DDL of silver - DATETIME -> DATE. 
	 CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC) -1 AS DATE) AS prd_end_dt --  The End Date of previous record = Start Date of NEXT record MINUS 1 day. - **Did modification to DDL of silver - DATETIME -> DATE.
FROM bronze.crm_prd_info;
*/ 


/* ========================================================
3. crm_sales_details
   ======================================================== */
SELECT * FROM bronze.crm_sales_details

-- 1. Check sls_ord_num for unwanted spaces (String)
SELECT
		sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)
-- Finding: No Result!

-- 2. Check Keys (meant to be connected with other tables)
-- prd_key of crm_sales_details connects to prd_key of crm_prd_info
-- cst_id of crm_sales_details connects to cst_id of crm_cust_info
SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT sls_prd_key FROM silver.crm_prd_info)
-- Findings: No results - Meaning all sls_prd_key from crm_sales_details can be connected with crm_prd_info

SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
-- Findings: No results - Meaning all sls_cust_id crm_sales_details can be connected with crm_cust_info

-- 3. Check Invalid Dates - sla_order_dt, sls_ship_dt, sls_due_dt
-- According to source system this is INT. We added these as INT in the DDL
-- Now we need to transform data type to DATE
-- before that do the following steps:
-- (a) Negative numbers or zeros cant be cast to a DATE
SELECT
		sls_order_dt
FROM bronze.crm_sales_details
--WHERE sls_order_dt < 0 -- Findings: No Negatives
WHERE sls_order_dt <= 0 -- Findings: Zeros Present
-- Replace 0 with NULL usinf NULLIF (For checking further)
SELECT
		NULLIF(sls_order_dt, 0) as sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 

-- (b) The length of the date must be 8: yyyymmdd
-- id <8 or >8 we have an issue
SELECT
		NULLIF(sls_order_dt, 0) as sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0  OR LEN(sls_order_dt) != 8 
-- Findings: Found dates less than 8

-- 4. Check if orderdate is smaller than shipping and due date
SELECT
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
-- Findings: No Results

--5. Check Formula
--Business Rule: 1. Sales = Quantity * Price. 2. Negatives, Zeros & NULLs Not Allowed


SELECT DISTINCT
		sls_sales AS old_sls_sales,
		sls_quantity,
		sls_price AS old_sls_price,

		CASE
		    WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) --(c). If price is negative, convert it to a positive value (ABS)
            THEN sls_quantity * ABS(sls_price) --(a). If Sales is negative, zero or null, derive it using Quantity and Price
			ELSE sls_sales
		END AS sls_sales,

		CASE 
		    WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0) --(b). If price is zero or null, calculate it using Sales and quantity
			ELSE sls_price
        END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price -- Findings: Found Values that dont follow the formula
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL -- Findings: NULLS present
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 -- Findings: Negatives and Zeros present
ORDER BY sls_sales,
		sls_quantity,
		sls_price
-- Solution Rules: (a). If Sales is negative, zero or null, derive it using Quantity and Price
--                 (b). If price is zero or null, calculate it using Sales and quantity
--                 (c). If price is negative, convert it to a positive value

-- Transformation Query for Silver Layer 
/* 
INSERT INTO silver.crm_sales_details (
										sls_ord_num,
										sls_prd_key,
										sls_cust_id,
										sls_order_dt,
										sls_ship_dt,
										sls_due_dt,
										sls_sales,
										sls_quantity,
										sls_price
		                              )
SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
		    WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL -- 3. To replace 0 and values >8 and <8 with NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- In MSSQL you need to first convert INT to VARCHAR to convert to DATE
		END AS sls_order_dt,

		CASE
		    WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,

		CASE
		    WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

		CASE
		    WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) --(c). If price is negative, convert it to a positive value (ABS)
            THEN sls_quantity * ABS(sls_price) --(a). If Sales is negative, zero or null, derive it using Quantity and Price
			ELSE sls_sales
		END AS sls_sales,

		sls_quantity,
		CASE 
		    WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0) --(b). If price is zero or null, calculate it using Sales and quantity
			ELSE sls_price
        END AS sls_price
FROM bronze.crm_sales_details
*/ 


/* ========================================================
4. erp_cust_az12
   ======================================================== */
 

-- 1. Check table links
-- From the data integration model we can see that cid of erp_cust_az12 can be liked with cst_key of crm_cust_info
SELECT * FROM bronze.erp_cust_az12
SELECT * FROM bronze.crm_cust_info
-- Findings: cid has extra 3 letters. We dont have any specification about these letters so we discard them

-- 2. Check bdate (if higher than current date)
SELECT 
      bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE()
-- Findings: There are future dates
-- Report to source sysrem

-- 3. Check low cardinality data
SELECT 
      DISTINCT(gen)
FROM bronze.erp_cust_az12
-- Findings: NULL, Blank Space, F, M, Female, Male 

-- Transformation Query for Silver Layer 
/*
INSERT INTO silver.erp_cust_az12 (
									cid,
									bdate,
									gen
		)
SELECT
       CASE  
	       WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --1. Remove 'NAS' prefix
		   ELSE cid
		   END AS cid,
	   CASE 
	       WHEN bdate > GETDATE() THEN NULL
		   ELSE bdate
	   END AS bdate, --2. Replace future bdates with NULL as they are incorrect data.  
	   CASE
	       WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
		   WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
	       ELSE 'n/a'
	   END AS gen
FROM bronze.erp_cust_az12
*/


/* ========================================================
5. erp_loc_a101
   ======================================================== */
 

 -- 1. Table Links
 -- cid of erp_loc_a101 can be linked with cid of crm_cust_info
 SELECT * FROM bronze.erp_loc_a101
 SELECT * FROM bronze.crm_cust_info
 -- Findings: unnecessary '-' after 2nd letter

-- 2. Check low cardinality data
SELECT 
      DISTINCT(cntry)
FROM bronze.erp_loc_a101
-- Findings: multiple versions of one country 


SELECT 
      cid,
	  cntry
FROM bronze.erp_loc_a101

-- Transformation Query for Silver Layer 
/*
INSERT INTO silver.erp_loc_a101 (
								 cid,
								 cntry
		)
SELECT 
      REPLACE(cid, '-', '') AS cid, --1. Handlled invalid values - replaced '-' with '')
	  
	  CASE
	      WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		  WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		  WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		  ELSE TRIM(cntry)
	  END AS cntry --2. Data Normalization 
FROM bronze.erp_loc_a101

*/


/* ========================================================
6. erp_px_cat_g1v2
   ======================================================== */

 -- 1. Table links
 -- id in erp_px_cat_g1v2 can be linked to cat_id crm_prd_info (in silver.crm_prd_info we added cat_id)
SELECT * FROM bronze.erp_px_cat_g1v2
SELECT * FROM silver.crm_prd_info


-- 2. Check unwanted spaces in string
SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)
-- Findings: No Results

-- 2. Check low cardinality data
SELECT DISTINCT
      --id,
	  --cat,
	  --subcat,
	  --maintenance
FROM bronze.erp_px_cat_g1v2
-- No null

-- This table doesnt require cleaning

-- Transformation Query for Silver Layer 
/*
INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
SELECT 
      id,
	  cat,
	  subcat,
	  maintenance
FROM bronze.erp_px_cat_g1v2
*/



SELECT * FROM bronze.crm_cust_info
SELECT * FROM bronze.crm_prd_info
SELECT * FROM bronze.crm_sales_details
SELECT * FROM bronze.erp_cust_az12
SELECT * FROM bronze.erp_loc_a101
SELECT * FROM bronze.erp_px_cat_g1v2
