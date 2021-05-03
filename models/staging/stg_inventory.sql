{%- set inventory_reference_object = 'inventory_snapshot' -%}
{%- set stg_item_reference_object = 'stg_item' -%}
{%- set stg_warehouse_reference_object = 'stg_warehouse' -%}

{%- set schema_name = 'RAW_DATA_SNAPSHOT' -%}


with int_inventory as (
        SELECT {{ get_date_surrogate_key('INV_DATE') }} as INV_DATE_SK,
                I.I_ITEM_SK AS INV_ITEM_SK,
                W.W_WAREHOUSE_SK AS INV_WAREHOUSE_SK,
                INV_QUANTITY_ON_HAND,
                INV.DBT_SCD_ID, INV.DBT_UPDATED_AT, INV.DBT_VALID_FROM, INV.DBT_VALID_TO
                FROM {{ ref(inventory_reference_object) }} INV
                LEFT JOIN {{ ref(stg_item_reference_object) }} I ON INV_ITEM_ID = I_ITEM_ID
                LEFT JOIN {{ ref(stg_warehouse_reference_object) }} W ON INV_WAREHOUSE_ID = W_WAREHOUSE_ID
),
inventory as (
                SELECT {{ dbt_utils.surrogate_key('INV_DATE_SK', 'INV_ITEM_SK', 'INV_WAREHOUSE_SK') }} as INV_SK,
                        *,
                        {{ dbt_utils.surrogate_key('INV_DATE_SK', 'INV_ITEM_SK', 'INV_WAREHOUSE_SK', 'INV_QUANTITY_ON_HAND', 'DBT_SCD_ID', 'DBT_UPDATED_AT', 'DBT_VALID_FROM', 'DBT_VALID_TO') }} as MD5_HASH
                FROM int_inventory
)

SELECT * FROM inventory
