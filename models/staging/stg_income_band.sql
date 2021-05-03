{%- set reference_object = 'income_band_snapshot' -%}
{%- set schema_name = 'RAW_DATA_SNAPSHOT' -%}
{%- set table_name = reference_object.upper() -%}
{# primary_key_cols should not have spaces after comma. Eg - 'col_1, col_2' #}
{%- set primary_key_cols = 'IB_INCOME_BAND_ID' -%}
{%- set primary_key_cols_list = primary_key_cols.split(",") -%}
{%- set surrogate_key_col_name = 'IB_INCOME_BAND_SK' -%}
{%- set md5_construct = get_md5_hash_cols_construct(schema_name, table_name, primary_key_cols_list) -%}

with income_band as (
        SELECT {{ dbt_utils.surrogate_key(primary_key_cols_list) }} as {{ surrogate_key_col_name }},
                *,
                MD5 ({{ md5_construct }}) as MD5_HASH
        FROM {{ ref(reference_object) }}
)

SELECT * FROM income_band
