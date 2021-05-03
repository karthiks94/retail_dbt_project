-- TPC-DS_query98
/*
Report on items sold in a given 30 day period, belonging to the specified category.
Qualification Substitution Parameters
*) YEAR.01 = 2002
*) SDATE.01 = 2002-05-27
*/

SELECT I_ITEM_ID
      ,I_ITEM_DESC 
      ,I_CATEGORY 
      ,I_CLASS 
      ,I_CURRENT_PRICE
      ,SUM(SS_EXT_SALES_PRICE) AS ITEMREVENUE 
      ,SUM(SS_EXT_SALES_PRICE)*100/SUM(SUM(SS_EXT_SALES_PRICE)) OVER
          (PARTITION BY I_CLASS) AS REVENUERATIO
FROM  
  {{ ref('store_sales') }}
      ,{{ ref('item') }} 
      ,{{ ref('date_dim') }}
WHERE 
  SS_ITEM_SK = I_ITEM_SK 
    AND I_CATEGORY IN ('Women', 'Electronics', 'Shoes')
    AND SS_SOLD_DATE_SK = D_DATE_SK
  AND D_DATE BETWEEN CAST('2002-05-27' AS DATE) 
        AND DATEADD(DAY,30,TO_DATE('2002-05-27'))
GROUP BY 
  I_ITEM_ID
        ,I_ITEM_DESC 
        ,I_CATEGORY
        ,I_CLASS
        ,I_CURRENT_PRICE
ORDER BY 
  I_CATEGORY
        ,I_CLASS
        ,I_ITEM_ID
        ,I_ITEM_DESC
        ,REVENUERATIO