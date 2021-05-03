{%- set reference_object = 'warehouse_snapshot' -%}
{%- set schema_name = 'RAW_DATA_SNAPSHOT' -%}
{%- set table_name = reference_object.upper() -%}
{# primary_key_cols should not have spaces after comma. Eg - 'col_1, col_2' #}
{%- set primary_key_cols = 'W_WAREHOUSE_ID' -%}
{%- set primary_key_cols_list = primary_key_cols.split(",") -%}
{%- set surrogate_key_col_name = 'W_WAREHOUSE_SK' -%}
{%- set md5_construct = get_md5_hash_cols_construct(schema_name, table_name, primary_key_cols_list) -%}

with warehouse as (
        SELECT {{ dbt_utils.surrogate_key(primary_key_cols_list) }} as {{ surrogate_key_col_name }},
                *,
                MD5 ({{ md5_construct }}) as MD5_HASH
        FROM {{ ref(reference_object) }}
)

SELECT * FROM warehouse
