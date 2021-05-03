-- List Price and Selling Price can't be zero, price should always be >= 0.
-- Therefore return records where this isn't true to make the test fail.

SELECT INV_QUANTITY_ON_HAND
FROM {{ source('RAW_DATA', 'INVENTORY') }}
WHERE
INV_QUANTITY_ON_HAND < 0