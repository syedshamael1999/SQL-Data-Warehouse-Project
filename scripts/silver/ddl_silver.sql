/*
===============================================================================
DDL Script: Create silver Tables
===============================================================================
Script Purpose:
	This script creates tables in the 'silver' schema, dropping existing tables
	if they already exist.
	Run this script to re-define the DDL structure of 'silver' tables
===============================================================================
*/

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
   DROP TABLE silver.crm_cust_info; --TSQL logic: in MSSQL if u need to rename or change data type you cant just change and execute as the table already exists. In other databases u can use "CREATE OR REPLACE TABLE" So check if table exists -> Drop Table -> Create tabel. Object Type = U (User) (User defined tables)
GO
CREATE TABLE silver.crm_cust_info (
	cst_id              INT,
	cst_key             NVARCHAR(50),
	cst_firstname       NVARCHAR(50),
	cst_lastname        NVARCHAR(50),
	cst_marital_status  NVARCHAR(50),
	cst_gndr            NVARCHAR(50),
	cst_create_date     DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE() --Metadata/Technical column: (Naming convention "dwn_<column_name>. This column tracks when a record was added to the data warehouse. DATETIME2 - Like DATETIME but with smaller storage footprint. DEFAULT GETDATE() - Automatically populates the column with current timestamp when a new row is inserted. We do not need to specify this in any ETL script
);
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info (
	prd_id        INT,
	cat_id        NVARCHAR(50), -- Added during cleaning data
	prd_key       NVARCHAR(50),
	prd_nm	      NVARCHAR(50),
	prd_cost	    INT,
	prd_line	    NVARCHAR(50),
	prd_start_dt  DATE, -- Modified from DATETIME to DATE during cleaning data
	prd_end_dt    DATE, -- Modified from DATETIME to DATE during cleaning data
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
   DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details (
	sls_ord_num  NVARCHAR(50),
	sls_prd_key  NVARCHAR(50),
	sls_cust_id  INT,
	sls_order_dt DATE, -- Modified from INT to DATE during cleaning data
	sls_ship_dt  DATE, -- Modified from INT to DATE during cleaning data
	sls_due_dt   DATE, -- Modified from INT to DATE during cleaning data
	sls_sales    INT,
	sls_quantity INT,
	sls_price    INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
   DROP TABLE silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101 (
	cid   NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
   DROP TABLE silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12 (
	cid   NVARCHAR(50),
	bdate DATE,
	gen   NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE silver.erp_px_cat_g1v2;
GO
CREATE TABLE silver.erp_px_cat_g1v2 (
	id          NVARCHAR(50),
	cat         NVARCHAR(50),
	subcat      NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
