/*
===========================================
Create Database and Schemas
===========================================
Script Purpose:
	This script creates a new database nammed 'DataWarehouse' after checking if it already exists.
	If the databaze exits, it it dropped and recreated. Additonally, the script sets up three schemas
	within database : 'bronze_layer, silver_layer, gold_layer'.

	!!!WARNING!!!!
	Running this script will drop te entire 'DataWarehouse' database if it exists.
	All data in the database will be parmanently deleted. Prrceed with caution
	and ensure you have proper backups before running this script.

*/

USE master;

CREATE DATABASE DataWarehouse;

GO
-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
	END;
	GO

USE DataWarehouse;

GO

CREATE SCHEMA bronze_layer;
GO
CREATE SCHEMA silver_layer;
GO
CREATE SCHEMA gold_layer;
