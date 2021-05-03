{%- set schema_name = 'RAW_DATA' -%}
{%- set table_name = 'DATE_DIM' -%}
{# primary_key_cols should not have spaces after comma. Eg - 'col_1, col_2' #}
{%- set primary_key_cols = 'D_DATE_ID' -%}
{%- set primary_key_cols_list = primary_key_cols.split(",") -%}
{%- set surrogate_key_col_name = 'D_DATE_SK' -%}
{%- set md5_construct = get_md5_hash_cols_construct(schema_name, table_name, primary_key_cols_list) -%}

with date_dim as (

        SELECT 
        {{ get_date_surrogate_key('D_DATE') }} as {{ surrogate_key_col_name }},
        -- REPLACE(D_DATE, '-', '') as {{ surrogate_key_col_name }},
                *,
                MD5 ({{ md5_construct }}) as MD5_HASH
        FROM {{ source(schema_name, table_name) }}

)

SELECT * FROM date_dim
