-- TPC-DS_query96
/*
Compute a count of sales from a named store to customers with a given number of dependents made in a
specified half hour period of the day.
Qualification Substitution Parameters:
*) HOUR.01 = 8
*) DEPCNT.01 = 5
*/
SELECT  COUNT(*) AS SALES_COUNT
FROM {{ ref('store_sales') }}
    ,{{ ref('household_demographics') }} 
    ,{{ ref('time_dim') }}, {{ ref('store') }}
WHERE SS_SOLD_TIME_SK = TIME_DIM.T_TIME_SK   
    AND SS_HDEMO_SK = HOUSEHOLD_DEMOGRAPHICS.HD_DEMO_SK 
    AND SS_STORE_SK = S_STORE_SK
    AND TIME_DIM.T_HOUR = 8
    AND TIME_DIM.T_MINUTE >= 30
    AND HOUSEHOLD_DEMOGRAPHICS.HD_DEP_COUNT = 5
    AND STORE.S_STORE_NAME = 'ese'
ORDER BY COUNT(*)