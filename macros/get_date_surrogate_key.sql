{% macro get_date_surrogate_key(date) -%}
    REPLACE({{ date }}, '-', '')
{%- endmacro %}