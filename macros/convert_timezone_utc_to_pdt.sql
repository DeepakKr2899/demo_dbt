{% macro convert_timezone_utc_to_pdt(column_name) -%}
    convert_timezone('UTC', 'America/Los_Angeles', {{ column_name }}::timestamp_ntz)
{% endmacro %}
