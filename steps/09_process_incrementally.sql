/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       09_process_incrementally.sql
Author:       Jeremiah Hansen
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/



-- ----------------------------------------------------------------------------
-- Step #1: Add new/remaining order data
-- ----------------------------------------------------------------------------

USE ROLE RETAIL;
USE WAREHOUSE HOL_WH;
USE RETAIL.RAW_POS;

ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE wait_for_completion=true;
COPY INTO ORDER_HEADER
FROM @external.frostbyte_raw_stage/pos/order_header/year=2022
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

COPY INTO ORDER_DETAIL
FROM @external.frostbyte_raw_stage/pos/order_detail/year=2022
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.PARQUET_FORMAT)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL;

-- See how many new records are in the stream
--SELECT COUNT(*) FROM HARMONIZED.POS_FLATTENED_V_STREAM;

-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

USE SCHEMA HARMONIZED;
EXECUTE TASK ORDERS_UPDATE_TASK;


-- ----------------------------------------------------------------------------
-- Step #3: Monitor tasks in Snowsight
-- ----------------------------------------------------------------------------
-- https://docs.snowflake.com/en/user-guide/ui-snowsight-tasks.html

-- manual queries to get task details
SHOW TASKS;

-- Task execution history in the past day
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('DAY',-1,CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100))
ORDER BY SCHEDULED_TIME DESC;

-- Query history in the past hour
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('HOURS',-1,CURRENT_TIMESTAMP()),CURRENT_TIMESTAMP()))
ORDER BY START_TIME DESC
LIMIT 100;

-- Scheduled task runs
SELECT
    TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) NEXT_RUN,
    SCHEDULED_TIME,
    NAME,
    STATE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'SCHEDULED'
ORDER BY COMPLETED_TIME DESC;

-- Other task-related metadata queries
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS())
  ORDER BY SCHEDULED_TIME;

SELECT *
  FROM TABLE(INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS())
  ORDER BY SCHEDULED_TIME;