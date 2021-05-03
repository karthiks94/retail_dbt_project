-- TPC-DS_query22
/*
For each product name, brand, class, category, calculate the average quantity on hand. Group data by product
name, brand, class and category.
Qualification Substitution Parameters:
*) DMS.01 = 1200
*/

SELECT  I_PRODUCT_NAME
             ,I_BRAND
             ,I_CLASS
             ,I_CATEGORY
             ,ROUND(AVG(INV_QUANTITY_ON_HAND)) QUANTITY_ON_HAND
       FROM {{ ref('inventory') }}
           ,{{ ref('date_dim') }}
           ,{{ ref('item') }}
       WHERE INV_DATE_SK=D_DATE_SK
              AND INV_ITEM_SK=I_ITEM_SK
              AND D_MONTH_SEQ BETWEEN 1200 AND 1200 + 11
       GROUP BY I_PRODUCT_NAME
                       ,I_BRAND
                       ,I_CLASS
                       ,I_CATEGORY
ORDER BY QUANTITY_ON_HAND, I_PRODUCT_NAME, I_BRAND, I_CLASS, I_CATEGORY