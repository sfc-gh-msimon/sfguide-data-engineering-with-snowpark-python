/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       08_orchestrate_jobs.sql
Author:       Jeremiah Hansen
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Tasks (with Stream triggers)
-- SNOWFLAKE ADVANTAGE: Task Observability

USE ROLE RETAIL;
USE WAREHOUSE HOL_WH;
USE SCHEMA RETAIL.HARMONIZED;


-- ----------------------------------------------------------------------------
-- Step #1: Create the tasks to call our Python stored procedures
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TASK ORDERS_UPDATE_TASK
WAREHOUSE = HOL_WH
WHEN
  SYSTEM$STREAM_HAS_DATA('POS_FLATTENED_V_STREAM')
AS
CALL HARMONIZED.ORDERS_UPDATE_SP();

CREATE OR REPLACE TASK DAILY_CITY_METRICS_UPDATE_TASK
WAREHOUSE = HOL_WH
AFTER ORDERS_UPDATE_TASK
WHEN
  SYSTEM$STREAM_HAS_DATA('ORDERS_STREAM')
AS
CALL ANALYTICS.DAILY_CITY_METRICS_UPDATE_SP();


-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

ALTER TASK DAILY_CITY_METRICS_UPDATE_TASK RESUME;
EXECUTE TASK ORDERS_UPDATE_TASK;