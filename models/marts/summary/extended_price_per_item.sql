/*
Report the total of extended sales price for all items of a specific brand in a specific year and month.
Qualification Substitution Parameters
*) MONTH.01=12
*) YEAR.01=2002
*/

SELECT  DT.D_YEAR
  ,ITEM.I_BRAND_ID BRAND_ID
  ,ITEM.I_BRAND BRAND
  ,SUM(SS_EXT_SALES_PRICE) EXTENDED_SALES_PRICE
 FROM {{ ref('date_dim') }} DT
     ,{{ ref('store_sales') }}
     ,{{ ref('item') }}
 WHERE DT.D_DATE_SK = STORE_SALES.SS_SOLD_DATE_SK
    AND STORE_SALES.SS_ITEM_SK = ITEM.I_ITEM_SK
    AND ITEM.I_MANAGER_ID = 1
    AND DT.D_MOY=12
    AND DT.D_YEAR=2002
 GROUP BY DT.D_YEAR
  ,ITEM.I_BRAND
  ,ITEM.I_BRAND_ID
 ORDER BY DT.D_YEAR
  ,EXTENDED_SALES_PRICE DESC
  ,BRAND_ID