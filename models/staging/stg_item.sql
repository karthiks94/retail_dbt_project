{%- set item_reference_object = 'item_snapshot' -%}
{%- set item_brand_reference_object = 'item_brand_snapshot' -%}
{%- set item_category_reference_object = 'item_category_snapshot' -%}
{%- set item_class_reference_object = 'item_class_snapshot' -%}
{%- set item_manufact_reference_object = 'item_manufact_snapshot' -%}

{%- set schema_name = 'RAW_DATA_SNAPSHOT' -%}

{%- set item_table_name = item_reference_object.upper() -%}
{%- set item_brand_table_name = item_brand_reference_object.upper() -%}
{%- set item_category_table_name = item_category_reference_object.upper() -%}
{%- set item_class_table_name = item_class_reference_object.upper() -%}
{%- set item_manufact_table_name = item_manufact_reference_object.upper() -%}


{# primary_key_cols should not have spaces after comma. Eg - 'col_1, col_2' #}
{%- set primary_key_cols = 'I_ITEM_ID' -%}
{%- set primary_key_cols_list = primary_key_cols.split(",") -%}
{%- set surrogate_key_col_name = 'I_ITEM_SK' -%}

with int_item as (

        SELECT I.*, 
               IB.I_BRAND_ID, IB. I_BRAND,
               I_CATEGORY_ID, I_CATEGORY, 
               I_CLASS_ID, I_CLASS, 
               I_MANUFACT_ID, I_MANUFACT
        FROM {{ ref(item_reference_object) }} I
            JOIN {{ ref(item_brand_reference_object) }} IB
            ON I.{{ primary_key_cols }} = IB.{{ primary_key_cols }}
            JOIN {{ ref(item_category_reference_object) }} ICAT
            ON I.{{ primary_key_cols }} = ICAT.{{ primary_key_cols }}
            JOIN {{ ref(item_class_reference_object) }} ICLS
            ON I.{{ primary_key_cols }} = ICLS.{{ primary_key_cols }}
            JOIN {{ ref(item_manufact_reference_object) }} IM
            ON I.{{ primary_key_cols }} = IM.{{ primary_key_cols }}
),

item as (
        SELECT {{ dbt_utils.surrogate_key(primary_key_cols_list) }} as {{ surrogate_key_col_name }},
                *,
                {{ dbt_utils.surrogate_key('I_REC_START_DATE', 'I_REC_END_DATE', 'I_ITEM_DESC', 'I_CURRENT_PRICE', 'I_WHOLESALE_COST', 'I_SIZE', 'I_FORMULATION', 'I_COLOR', 'I_UNITS', 'I_CONTAINER', 'I_MANAGER_ID', 'I_PRODUCT_NAME', 'I_BRAND_ID', 'I_BRAND', 'I_CATEGORY_ID', 'I_CATEGORY', 'I_CLASS_ID', 'I_CLASS', 'I_MANUFACT_ID', 'I_MANUFACT', 'DBT_SCD_ID', 'DBT_UPDATED_AT', 'DBT_VALID_FROM', 'DBT_VALID_TO') }} as MD5_HASH
        FROM int_item)

select * from item

