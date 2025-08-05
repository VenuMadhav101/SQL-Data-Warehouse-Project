USE master;
GO

-- drop and recreate the 'DataWarehouseProject' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseProject')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- create the 'DataWarehouseProject' database
CREATE DATABASE DataWarehouseProject;
GO

USE DataWarehouseProject;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
