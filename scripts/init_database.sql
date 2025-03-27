/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master; -- Switch to master DB. Its a DB where you can create other DBs.
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE; -- SET SINGLE_USER: only one user can access the DB at a time - Necessary before dropping the database, as other users might be using it. WITH ROLLBACK IMMEDIATE: Rolls back any active transactions immediately and disconnects all users - Ensures that no ongoing transactions block the database from being dropped.
    DROP DATABASE DataWarehouse;
END;
GO


-- Create Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO
-- Create Schemas (A container within a DB that groups related DB objects like tables, views, stored procedures and functions)

CREATE SCHEMA bronze;
GO -- GO separates batches when working with multiple SQL statements. It tells MS SQL Server to process the previous batch before moving on. GO is specific to MS SQL Server.

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
