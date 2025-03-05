/*
 ======================================
 Create Database and Schemas
 ======================================
 Scripts: This create new database and check if the database is already exist then it deletes the database and creates a new database with the name. Additionally the scripts setup 3 schemas with in the database: "silver", "gold", "bronze".
 
 Warning: Running this script will erase all the Data permanently and recreate the database. Please ensure you have a backup plan before running the script.
 */
-- Step 1: Drop Database if it Exists
DROP DATABASE IF EXISTS datawarehouse;
--Checking which database using
SELECT current_database();
-- Instead, run this SQL after manually switching to the new database:
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;