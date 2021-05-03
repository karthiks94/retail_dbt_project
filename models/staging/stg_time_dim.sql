{%- set schema_name = 'RAW_DATA' -%}
{%- set table_name = 'TIME_DIM' -%}
{# primary_key_cols should not have spaces after comma. Eg - 'col_1, col_2' #}
{%- set primary_key_cols = 'T_TIME_ID' -%}
{%- set primary_key_cols_list = primary_key_cols.split(",") -%}
{%- set surrogate_key_col_name = 'T_TIME_SK' -%}
{%- set md5_construct = get_md5_hash_cols_construct(schema_name, table_name, primary_key_cols_list) -%}

with time_dim as (
        SELECT {{ get_time_surrogate_key('T_HOUR', 'T_MINUTE', 'T_SECOND') }} as {{ surrogate_key_col_name }},
        -- (T_HOUR * 3600) + (T_MINUTE * 60) + (T_SECOND) as {{ surrogate_key_col_name }},
                *,
                MD5 ({{ md5_construct }}) as MD5_HASH
        FROM {{ source(schema_name, table_name) }}
)

SELECT * FROM time_dim
