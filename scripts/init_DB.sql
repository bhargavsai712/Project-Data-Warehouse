/*
CREATE DATABASE & SCHEMAS

PURPOSE:
Creates a database named 'DataWH' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas Within the database: 'bronze,'silver','gold'

CAUTION:
Running this script will drop entire database if already exists.

*/


USE master;
GO

--Drop and recreate the DataDW database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWH')
BEGIN
    ALTER DATABASE DataWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWH
END;
GO

-- Create the 'DataWH' database
CREATE DATABASE DataWH;
GO

--Create Schemas
USE DataWH;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
