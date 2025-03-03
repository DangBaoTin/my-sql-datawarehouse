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

USE my-datawarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

GRANT USAGE ON SCHEMA bronze, silver, gold TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA bronze, silver, gold TO anon, authenticated, service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA bronze, silver, gold TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA bronze, silver, gold TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bronze, silver, gold GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bronze, silver, gold GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA bronze, silver, gold GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;