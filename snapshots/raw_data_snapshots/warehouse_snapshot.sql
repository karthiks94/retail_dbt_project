{% snapshot warehouse_snapshot %}

{%- set database_name = 'ANALYTICS' -%}
{%- set schema_name = 'RAW_DATA' -%}
{%- set table_name = 'WAREHOUSE' -%}
{%- set unique_key_col = 'W_WAREHOUSE_ID' -%}
{%- set last_load_ts_col = 'LOAD_TS' -%}

{% set snapshot_schema = schema_name + '_snapshot' %}

{{
    config(
      target_database=database_name,
      target_schema=snapshot_schema,
      unique_key=unique_key_col,
      strategy='timestamp',
      updated_at=last_load_ts_col,
    )
}}

select * from {{ source(schema_name, table_name) }}

{% endsnapshot %}