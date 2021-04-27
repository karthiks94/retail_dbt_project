{% macro get_md5_hash_cols_construct(schema_name, table_name, exclude_cols_list) %}

{%- set query -%}

        SELECT COLUMN_NAME FROM ANALYTICS.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{{ schema_name }}' AND TABLE_NAME = '{{ table_name }}'
        ORDER BY ORDINAL_POSITION

{%- endset -%}

{%- if execute -%}
{# Take the first column #}
{%- set results = run_query(query) -%}
{%- set results_column = results.columns[0] -%}

{%- set return_result_list = [] -%}

{% for column_name in results_column %}
    {%- if column_name not in exclude_cols_list -%}
        {{ return_result_list.append("IFNULL (CAST (" + column_name + " AS STRING ), '')") }}
    {%- endif -%}
{% endfor %}

{{return(" || ".join(return_result_list))}}

{%- else -%}
{{ return(false) }}
{%- endif -%}

{% endmacro %}
