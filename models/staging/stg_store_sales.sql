{%- set store_sales_reference_object = 'store_sales_snapshot' -%}
{%- set stg_item_reference_object = 'stg_item' -%}
{%- set stg_store_reference_object = 'stg_store' -%}
{%- set stg_customer_reference_object = 'stg_customer' -%}
{%- set stg_customer_address_reference_object = 'stg_customer_address' -%}
{%- set stg_customer_demographics_reference_object = 'stg_customer_demographics' -%}
{%- set stg_household_demographics_reference_object = 'stg_household_demographics' -%}
{%- set stg_promotion_reference_object = 'stg_promotion' -%}

{%- set schema_name = 'RAW_DATA_SNAPSHOT' -%}

{%- set primary_key_cols = 'SS_SOLD_DATE_TIME' -%}


with int_store_sales as (
        SELECT REPLACE(date({{ primary_key_cols }}), '-', '') as SS_SOLD_DATE_SK,
        (hour(time({{ primary_key_cols }})) * 3600) + (minute(time({{ primary_key_cols }})) * 60) + (second(time({{ primary_key_cols }}))) as SS_SOLD_TIME_SK,
        I.I_ITEM_SK AS SS_ITEM_SK,
        C.C_CUSTOMER_SK AS SS_CUSTOMER_SK,
        CD.CD_DEMO_SK AS SS_CDEMO_SK,
        HD.HD_DEMO_SK AS SS_HDEMO_SK,
        CA.CA_ADDRESS_SK AS SS_ADDR_SK,
        S.S_STORE_SK AS SS_STORE_SK,
        P.P_PROMO_SK AS SS_PROMO_SK,
        SS.SS_TICKET_NUMBER, SS.SS_QUANTITY, SS.SS_WHOLESALE_COST, SS.SS_LIST_PRICE, SS.SS_SALES_PRICE, 
        SS.SS_EXT_DISCOUNT_AMT, SS.SS_EXT_SALES_PRICE, SS.SS_EXT_WHOLESALE_COST, SS.SS_EXT_LIST_PRICE, 
        SS.SS_EXT_TAX, SS.SS_COUPON_AMT, SS.SS_NET_PAID, SS.SS_NET_PAID_INC_TAX, SS.SS_NET_PROFIT, SS.LOAD_TS, 
        SS.DBT_SCD_ID, SS.DBT_UPDATED_AT, SS.DBT_VALID_FROM, SS.DBT_VALID_TO
        FROM {{ ref(store_sales_reference_object) }} SS
        LEFT JOIN {{ ref(stg_item_reference_object) }} I ON SS.SS_ITEM_ID = I.I_ITEM_ID
        LEFT JOIN {{ ref(stg_store_reference_object) }} S ON SS.SS_STORE_ID = S.S_STORE_ID
        LEFT JOIN {{ ref(stg_customer_address_reference_object) }} CA ON SS.SS_ADDR_ID = CA.CA_ADDRESS_ID
        LEFT JOIN {{ ref(stg_customer_reference_object) }} C ON SS.SS_CUSTOMER_ID = C.C_CUSTOMER_ID
        LEFT JOIN {{ ref(stg_customer_demographics_reference_object) }} CD ON SS.SS_CDEMO_ID = CD.CD_DEMO_ID
        LEFT JOIN {{ ref(stg_household_demographics_reference_object) }} HD ON SS.SS_HDEMO_ID = HD.HD_DEMO_ID
        LEFT JOIN {{ ref(stg_promotion_reference_object) }} P ON SS.SS_PROMO_ID = P.P_PROMO_ID
),
store_sales as (
                SELECT {{ dbt_utils.surrogate_key('SS_SOLD_DATE_SK', 'SS_SOLD_TIME_SK', 'SS_ITEM_SK', 'SS_CUSTOMER_SK', 'SS_CDEMO_SK', 'SS_HDEMO_SK', 'SS_ADDR_SK', 'SS_STORE_SK', 'SS_PROMO_SK') }} as STORE_SALES_SK,
                *,
                {{ dbt_utils.surrogate_key('SS_TICKET_NUMBER', 'SS_QUANTITY', 'SS_WHOLESALE_COST', 'SS_LIST_PRICE', 'SS_SALES_PRICE', 'SS_EXT_DISCOUNT_AMT', 'SS_EXT_SALES_PRICE', 'SS_EXT_WHOLESALE_COST', 'SS_EXT_LIST_PRICE', 'SS_EXT_TAX', 'SS_COUPON_AMT', 'SS_NET_PAID', 'SS_NET_PAID_INC_TAX', 'SS_NET_PROFIT', 'LOAD_TS', 'DBT_SCD_ID', 'DBT_UPDATED_AT', 'DBT_VALID_FROM', 'DBT_VALID_TO') }} as MD5_HASH
                FROM int_store_sales
)

SELECT * FROM store_sales
