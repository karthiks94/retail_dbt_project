/*
For each item and a specific year and month calculate the sum of the extended sales price of store transactions.
Qualification Substitution Parameters:
*) MONTH.01 = 11
*) YEAR.01 = 2000
*/

SELECT  DT.D_YEAR
  ,ITEM.I_CATEGORY_ID
  ,ITEM.I_CATEGORY
  ,SUM(SS_EXT_SALES_PRICE) AS EXTENDED_SALES_PRICE
 FROM   {{ ref('date_dim') }} DT
  ,{{ ref('store_sales') }}
  ,{{ ref('item') }}
 WHERE DT.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK
  AND STORE_SALES.SS_ITEM_SK = ITEM.I_ITEM_SK
  AND ITEM.I_MANAGER_ID = 1    
  AND DT.D_MOY=11
  AND DT.D_YEAR=2000
 GROUP BY   DT.D_YEAR
    ,ITEM.I_CATEGORY_ID
    ,ITEM.I_CATEGORY
 ORDER BY SUM(SS_EXT_SALES_PRICE) DESC, DT.D_YEAR
    ,ITEM.I_CATEGORY_ID
    ,ITEM.I_CATEGORY