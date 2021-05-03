-- TPC-DS_query89
/*
Within a year list all month and combination of item categories, classes and brands that have had monthly sales
larger than 0.1 percent of the total yearly sales.
Qualification Substitution Parameters:
*) CLASS_F.01 = dresses
*) CAT_F.01 = Women
*) CLASS_E.01 = birdal
*) CAT_E.01 = Jewelry
*) CLASS_D.01 = shirts
*) CAT_D.01 = Men
*) CLASS_C.01 = football
*) CAT_C.01 = Sports
*) CLASS_B.01 = stereo
*) CAT_B.01 = Electronics
*) CLASS_A.01 = computers
*) CAT_A.01 = Books
*) YEAR.01 = 1999
*/

SELECT  *
FROM(
SELECT I_CATEGORY, I_CLASS, I_BRAND,
       S_STORE_NAME, S_COMPANY_NAME,
       D_MOY,
       SUM(SS_SALES_PRICE) SUM_SALES,
       AVG(SUM(SS_SALES_PRICE)) OVER
         (PARTITION BY I_CATEGORY, I_BRAND, S_STORE_NAME, S_COMPANY_NAME)
         AVG_MONTHLY_SALES
FROM {{ ref('item') }}, {{ ref('store_sales') }}, {{ ref('date_dim') }}, {{ ref('store') }}
WHERE SS_ITEM_SK = I_ITEM_SK AND
      SS_SOLD_DATE_SK = D_DATE_SK AND
      SS_STORE_SK = S_STORE_SK AND
      D_YEAR IN (1998) AND
        ((I_CATEGORY IN ('Women','Sports','Children') AND
          I_CLASS IN ('maternity','basketball','school-uniforms')
         )
      OR (I_CATEGORY IN ('Home','Music','Shoes') AND
          I_CLASS IN ('accent','classical','mens') 
        ))
GROUP BY I_CATEGORY, I_CLASS, I_BRAND,
         S_STORE_NAME, S_COMPANY_NAME, D_MOY) TMP1
WHERE CASE WHEN (AVG_MONTHLY_SALES <> 0) THEN (ABS(SUM_SALES - AVG_MONTHLY_SALES) / AVG_MONTHLY_SALES) ELSE NULL END > 0.1
ORDER BY SUM_SALES - AVG_MONTHLY_SALES, S_STORE_NAME