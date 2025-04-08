/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'fantasyPL' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'fantasyPL' database if it exists. 
    All data, schemas, and objects in the database will be permanently deleted. 
    Proceed with extreme caution and ensure you have proper backups before running this script.
    Additionally, ensure no active connections exist to the database, as PostgreSQL will not drop 
    a database with active connections unless forced.
*/

-- Terminate any existing connections to 'fantasyPL' (if it exists) to allow dropping
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'fantasypl') THEN
        PERFORM pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = 'fantasypl' AND pid <> pg_backend_pid();
    END IF;
END $$;

-- Drop the 'fantasyPL' database if it exists
DROP DATABASE IF EXISTS fantasypl;

-- Create the 'fantasyPL' database
CREATE DATABASE fantasypl;

-- Connect to the newly created 'fantasyPL' database
-- Note: In a script, you may need to run this separately or use `\c fantasypl` in psql
\connect fantasypl

-- Create the 'bronze' schema for raw, unprocessed data
CREATE SCHEMA bronze;

-- Create the 'silver' schema for cleaned or lightly transformed data
CREATE SCHEMA silver;

-- Create the 'gold' schema for fully transformed, business-ready data
CREATE SCHEMA gold;