{%- set unique_key_col = 'CA_ADDRESS_SK' -%}

{{
  config(
    materialized='incremental',
    unique_key=unique_key_col
  )
}}

with customer_address as (
    select SOURCE.* from {{ ref('stg_customer_address')}} AS SOURCE

    {%- if is_incremental()  %}
    LEFT JOIN {{ this }} AS TARGET ON SOURCE.{{ unique_key_col }} = TARGET.{{ unique_key_col }}
    {%- endif %}
    WHERE SOURCE.DBT_VALID_TO IS NULL 

    {%- if is_incremental()  -%}

    -- this filter will only be applied on an incremental run
    AND SOURCE.MD5_HASH <> TARGET.MD5_HASH 
    AND SOURCE.LOAD_TS > (select max(LOAD_TS) from {{ this }})

    {%- endif -%}
)
select * from customer_address