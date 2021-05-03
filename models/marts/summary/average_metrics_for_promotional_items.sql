-- TPC-DS_query7

/*
Compute the average quantity, list price, discount, and sales price for promotional items sold in stores where the
promotion is not offered by mail or a special event. Restrict the results to a specific gender, marital and
educational status.
Qualification Substitution Parameters:
*) YEAR.01=2001
*) ES.01=College
*) MS.01=S
*) GEN.01=M
*/

SELECT  I_ITEM_ID, 
      AVG(SS_QUANTITY) AVG_QUANTITY,
      AVG(SS_LIST_PRICE) AVG_LIST_PRICE,
      AVG(SS_COUPON_AMT) AVG_COUPON_AMOUNT,
      AVG(SS_SALES_PRICE) AVG_SALES_PRICE 
FROM {{ ref('store_sales') }}, {{ ref('customer_demographics' )}}, {{ ref('date_dim') }}, {{ ref('item') }}, {{ ref('promotion') }}
WHERE SS_SOLD_DATE_SK = D_DATE_SK AND
     SS_ITEM_SK = I_ITEM_SK AND
     SS_CDEMO_SK = CD_DEMO_SK AND
     SS_PROMO_SK = P_PROMO_SK AND
     CD_GENDER = 'M' AND 
     CD_MARITAL_STATUS = 'M' AND
     CD_EDUCATION_STATUS = '4 yr Degree' AND
     (P_CHANNEL_EMAIL = 'N' OR P_CHANNEL_EVENT = 'N') AND
     D_YEAR = 2001 
     AND SS_SALES_PRICE IS NOT NULL
GROUP BY I_ITEM_ID
ORDER BY AVG(SS_SALES_PRICE) DESC