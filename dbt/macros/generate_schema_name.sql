{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        A model or seed with a schema config (silver, gold) is written to
        that dataset as is, not to "<profile dataset>_<schema>" (dbt default).
        Without one, it goes to the profile dataset.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}


{% macro current_run_id() %}
    {#- Pipeline run id: --vars run_id (Airflow run_id), else dbt's invocation_id. -#}
    {{ return(var('run_id', invocation_id)) }}
{% endmacro %}
