-- List Price and Selling Price can't be zero, price should always be >= 0.
-- Therefore return records where this isn't true to make the test fail.

SELECT SS_LIST_PRICE, SS_SALES_PRICE 
FROM {{ ref('store_sales') }}
WHERE
SS_LIST_PRICE < 0 OR SS_SALES_PRICE < 0