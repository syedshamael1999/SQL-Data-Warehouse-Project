/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
		BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

--1. Loading silver.crm_cust_info
	    SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
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
			  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';
		

--2. Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
			 REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- 2. (a) Extract from 1st position till 5th. - **Did modification to DDL of silver - Added cat_id NVARCHAR(50). [Derived Columns]
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
			 CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC) -1 AS DATE) AS prd_end_dt --  The End Date of previous record = Start Date of NEXT record MINUS 1 day. - **Did modification to DDL of silver - DATETIME -> DATE. [Data Enrichment - adding new data to enhance]
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
 PRINT '>> -------------';

		

--3. Loading crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
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
					WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL -- 3. To replace 0 and values >8 and <8 with NULL [Handeling Invalid Data]]
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- In MSSQL you need to first convert INT to VARCHAR to convert to DATE [Data Type Casting]
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
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';
		
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';


--4. Loading erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
											cid,
											bdate,
											gen
				)
		SELECT
			   CASE  
				   WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --1. Handled invalid values - Remove 'NAS' prefix
				   ELSE cid
				   END AS cid,
			   CASE 
				   WHEN bdate > GETDATE() THEN NULL
				   ELSE bdate
			   END AS bdate, --2. Handled invalid values - Replace future bdates with NULL as they are incorrect data.  
			   CASE
				   WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
				   WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
				   ELSE 'n/a'
			   END AS gen --3. Data Normalization 
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
 PRINT '>> -------------';

		

--5. Loading erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
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
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';
		

--6. Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
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
		FROM bronze.erp_px_cat_g1v2;
  
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
