/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       11_teardown.sql
Author:       Jeremiah Hansen
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/


USE ROLE ACCOUNTADMIN;

DROP DATABASE RETAIL;
-- alter database retail rename to retail_backup;

DROP WAREHOUSE HOL_WH;

DROP ROLE RETAIL;

-- Drop the weather share
DROP DATABASE FROSTBYTE_WEATHERSOURCE;
