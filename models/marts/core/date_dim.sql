{{
  config(
    materialized='incremental',
    unique_key='D_DATE_SK'
  )
}}

with date_dim as (
    select * from {{ ref('stg_date_dim')}}
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where LOAD_TS > (select max(LOAD_TS) from {{ this }})

    {% endif %}
)
select * from date_dim