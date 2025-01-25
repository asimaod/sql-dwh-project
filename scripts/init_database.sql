/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'. 
    Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
*/

-- Database created via terminal
create database DataWarehouse; 

-- Coming back to the DBMS and going to the 'DataWarehouse' database
-- Create Schemas
create schema bronze;
create schema silver;
create schema gold;
