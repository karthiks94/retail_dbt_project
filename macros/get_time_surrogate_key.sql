{% macro get_time_surrogate_key(hour, minutes, seconds) -%}
    ({{ hour }} * 3600) + ({{ minutes }} * 60) + ({{ seconds }})
{%- endmacro %}