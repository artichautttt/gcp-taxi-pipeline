{% macro pipeline_month() %}
    {#-
        Month being built, as an integer YYYYMM, from --vars '{pipeline_month: 202501}'.
        Fails at execution if the var is missing or malformed. At parse time
        (execute is false) dbt renders models without vars, so return a placeholder.
    -#}
    {%- set raw = var('pipeline_month', none) -%}
    {%- if raw is none -%}
        {%- if execute -%}
            {{ exceptions.raise_compiler_error(
                "pipeline_month is required, e.g. dbt run --vars '{pipeline_month: 202501}'") }}
        {%- endif -%}
        {{ return(0) }}
    {%- endif -%}
    {%- set text = raw | string -%}
    {%- if not (text | length == 6 and text.isdigit() and 1 <= (text[4:] | int) <= 12) -%}
        {{ exceptions.raise_compiler_error(
            "pipeline_month must be YYYYMM with a month in 01..12, got '" ~ text ~ "'") }}
    {%- endif -%}
    {{ return(text | int) }}
{% endmacro %}


{% macro monthly_partition(field='pickup_month') %}
    {#-
        Integer-range partitioning on a YYYYMM column. Not a date partition:
        the Sandbox dataset expires date partitions older than 60 days, which
        would delete 2025 data as soon as it is written.
        copy_partitions must sit inside partition_by: insert_overwrite then
        replaces the month with a table copy job instead of a MERGE (DML is
        refused by the Sandbox; measured in the spike, 2026-10-26).
    -#}
    {{ return({
        'field': field,
        'data_type': 'int64',
        'range': {'start': 202401, 'end': 203001, 'interval': 1},
        'copy_partitions': true
    }) }}
{% endmacro %}
