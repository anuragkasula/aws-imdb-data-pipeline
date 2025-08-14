{% macro latest_run_date(source_relation) -%}
  (select max(run_date) from {{ source_relation }})
{%- endmacro %}