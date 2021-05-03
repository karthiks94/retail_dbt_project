{%- set store_reference_object = 'store_snapshot' -%}
{%- set store_market_reference_object = 'store_market_snapshot' -%}

{%- set schema_name = 'RAW_DATA_SNAPSHOT' -%}

{%- set store_table_name = store_reference_object.upper() -%}
{%- set store_market_table_name = store_market_reference_object.upper() -%}

{# primary_key_cols should not have spaces after comma. Eg - 'col_1, col_2' #}
{%- set primary_key_cols = 'S_STORE_ID' -%}
{%- set primary_key_cols_list = primary_key_cols.split(",") -%}
{%- set surrogate_key_col_name = 'S_STORE_SK' -%}

{%- set store_md5_construct = get_md5_hash_cols_construct(schema_name, store_table_name, primary_key_cols_list) -%}
{%- set store_market_md5_construct = get_md5_hash_cols_construct(schema_name, store_market_table_name, primary_key_cols_list) -%}

with int_store as (

        SELECT S.*, 
                SM.S_MARKET_ID, SM.S_MARKET_DESC, SM.S_MARKET_MANAGER FROM {{ ref(store_reference_object) }} S
                JOIN {{ ref(store_market_reference_object) }} SM
                ON S.{{ primary_key_cols }} = SM.{{ primary_key_cols }}
),

store as (
        SELECT {{ dbt_utils.surrogate_key(primary_key_cols_list) }} as {{ surrogate_key_col_name }},
                *,
                {{ dbt_utils.surrogate_key('S_MARKET_ID', 'S_REC_START_DATE', 'S_MARKET_DESC', 'S_REC_END_DATE', 'S_CLOSED_DATE', 'S_MARKET_MANAGER', 'S_STORE_NAME', 'S_NUMBER_EMPLOYEES', 'S_FLOOR_SPACE', 'S_HOURS', 'S_MANAGER', 'S_GEOGRAPHY_CLASS', 'S_DIVISION_ID', 'S_DIVISION_NAME', 'S_COMPANY_ID', 'S_COMPANY_NAME', 'S_STREET_NUMBER', 'S_STREET_NAME', 'S_STREET_TYPE', 'S_SUITE_NUMBER', 'S_CITY', 'S_COUNTY', 'S_STATE', 'S_ZIP', 'S_COUNTRY', 'S_GMT_OFFSET', 'S_TAX_PRECENTAGE', 'LOAD_TS', 'DBT_SCD_ID', 'DBT_UPDATED_AT', 'DBT_VALID_FROM', 'DBT_VALID_TO') }} as MD5_HASH
        FROM int_store)

select * from store

