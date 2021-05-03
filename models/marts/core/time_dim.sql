{{
  config(
    materialized='table'
  )
}}


select * from {{ ref('stg_time_dim')}}
