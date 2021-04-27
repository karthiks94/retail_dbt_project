{{
  config(
    materialized='incremental',
    unique_key='T_TIME_SK'
  )
}}

with time_dim as (
    select * from {{ ref('stg_time_dim')}}
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where LAST_LOAD_TS > (select max(LAST_LOAD_TS) from {{ this }})

    {% endif %}
)
select * from time_dim