
CREATE DATABASE BIGDATA_OPERATIONAL;

USE BIGDATA_OPERATIONAL;

CREATE OR REPLACE TABLE BIGDATA_OPERATIONAL.PUBLIC.TABLAS_HUERFANAS  AS


WITH ACCESS_HISTORY_DIRECT AS (
    SELECT 
        obj.value:objectName::string AS objName, 
        COUNT(DISTINCT query_id)     AS uses, 
        MAX(query_start_time)        AS last_direct_access
    FROM snowflake.account_usage.access_history AS ahd,
         LATERAL FLATTEN(input => ahd.DIRECT_OBJECTS_ACCESSED) obj
    GROUP BY objName
),

ACCESS_HISTORY_INDIRECT AS (
    SELECT 
        obj.value:objectName::string AS objName, 
        COUNT(DISTINCT query_id)     AS uses, 
        MAX(query_start_time)        AS last_indirect_access
    FROM snowflake.account_usage.access_history AS ahi,
         LATERAL FLATTEN(input => ahi.BASE_OBJECTS_ACCESSED) obj
    GROUP BY objName
),

ASD AS (
    SELECT 
        T.TABLE_CATALOG, 
        T.TABLE_SCHEMA, 
        T.TABLE_NAME,
        CONCAT_WS('.', COALESCE(T.TABLE_CATALOG, 'TABLE_CATALOG'), COALESCE(T.TABLE_SCHEMA, 'TABLE_SCHEMA'), COALESCE(T.TABLE_NAME, 'TABLE_NAME')) AS FULL_OBJECT_NAME,
        T.TABLE_TYPE,
        SM.ACTIVE_BYTES / 1024 / 1024 / 1024 AS ACTIVE_GB,
        T.CREATED,
        T.LAST_ALTERED,
        T.LAST_DDL,
        
        AHD.USES                  AS DIRECT_USES,
        AHD.last_direct_access    AS LAST_DIRECT_ACCESS,
        CASE
            WHEN LAST_DIRECT_ACCESS IS NULL THEN NULL  
            ELSE DATEDIFF('day', LAST_DIRECT_ACCESS, CURRENT_TIMESTAMP())
        END AS DAYS_SINCE_DIRECT_ACCESS,
        
        AHI.USES                  AS INDIRECT_USES,
        AHI.last_indirect_access  AS LAST_INDIRECT_ACCESS,
        CASE
            WHEN LAST_INDIRECT_ACCESS IS NULL THEN NULL  
            ELSE DATEDIFF('day', LAST_INDIRECT_ACCESS, CURRENT_TIMESTAMP())
        END AS DAYS_SINCE_INDIRECT_ACCESS,
        
        CASE
            WHEN LAST_DIRECT_ACCESS IS NULL AND LAST_INDIRECT_ACCESS IS NULL THEN 'old'
            WHEN (LAST_DIRECT_ACCESS  >= DATEADD(day, -90, CURRENT_TIMESTAMP()))
                OR (LAST_INDIRECT_ACCESS >= DATEADD(day, -90, CURRENT_TIMESTAMP())) THEN 'new'
            ELSE 'old'
        END AS HK_STATUS

    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES T
        LEFT JOIN ACCESS_HISTORY_DIRECT AHD
            ON CONCAT(T.TABLE_CATALOG, '.', T.TABLE_SCHEMA, '.', T.TABLE_NAME) = AHD.objName
        LEFT JOIN ACCESS_HISTORY_INDIRECT AHI
            ON CONCAT(T.TABLE_CATALOG, '.', T.TABLE_SCHEMA, '.', T.TABLE_NAME) = AHI.objName
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS SM
            ON SM.TABLE_CATALOG = T.TABLE_CATALOG
            AND SM.TABLE_SCHEMA  = T.TABLE_SCHEMA
            AND SM.TABLE_NAME    = T.TABLE_NAME
            AND SM.DELETED = FALSE
    WHERE T.DELETED IS NULL
      -- Filtros para solo ver bases productivas:
  /*    AND T.TABLE_CATALOG IN ('AMPLITUDE_LANDING','DB_COE_DATA_ANALYTICS','DB_COE_DATAQUALITY',
            'DB_CONTROL_GESTION','DB_DATAOPERATIONAL','DB_GRUPONARANJA_ANALYTICS','DB_NARANJA_DATA_MODELS',
            'DB_NARANJA_DIGITAL_ANALYTICS','DB_NARANJA_FACTS_ANALYTICS','DB_NARANJA_WORKSPACE')
   -- AND T.TABLE_CATALOG NOT LIKE '%_DEV%'
   --   AND T.TABLE_CATALOG NOT LIKE '%_NONPROD%'
   --   AND T.TABLE_CATALOG NOT LIKE '%_STAGING%'
   --   AND T.TABLE_CATALOG <> 'SNOWFLAKE'
   --   AND T.TABLE_CATALOG <> 'SNOWFLAKE_CONNECTOR_FOR_GOOGLE_ANALYTICS_RAW_DATA'
*/
)
-- Selección final sin filtrar por 'new', ordenando para ver las usadas primero y luego las pesadas
SELECT * FROM ASD WHERE HK_STATUS='old'
ORDER BY HK_STATUS DESC, ACTIVE_GB DESC NULLS LAST;






/*
Ranking Functions
Ranking window functions in SQL assign a numerical rank to rows within a dataset, often
partitioned into groups, based on a specified order.

Find duplicates using row_number
Provides a sequential ranking of the given partition by the number of rows within the partition. If
two rows have the same info based on the partition by lists (for ex, columns that identify a
uniqueness) the function determines a rank based on the ORDER BY statement.
It is used to find duplicates on VALUES of a list of columns that, as a group, should be unique.
*/


SELECT
*,
ROW_NUMBER() OVER (
    PARTITION BY table_catalog, table_schema, table_name
    -- Columns that identify a duplicate
    ORDER BY LAST_DDL DESC
    -- Column that decide row_number order
) as OCCURENCE_ID
FROM BIGDATA_OPERATIONAL.PUBLIC.TABLAS_HUERFANAS
qualify OCCURENCE_ID > 1





t_catalog | t_schema | t_name | occurence_id
------------+----------+--------+-------------
db1 |schema1 | table1 | 2
db1 |schema1 | table1 | 3
db2 |schema5 | table1 | 2
db4 |schema3 | table6 | 2




SELECT column1, column2, column3, ..., COUNT(*) as count
FROM my_table
GROUP BY column1, column2, column3, ... HAVING COUNT(*) > 1

