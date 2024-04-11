{% macro get_create_function_as_sql(relation, sql, config, model_description) -%} 

  {%- set return_type = config.require('return_type') -%}

  {%- set params = config.require('params') -%}
  {%- set params_string -%}
    {%- for param in params -%}
      {{ param }}
      {%- if not loop.last -%}, {%- endif -%}
    {%- endfor -%}
  {%- endset -%}

  CREATE OR REPLACE FUNCTION {{ relation }} ({{ params_string }}) RETURNS {{ return_type }} 
  OPTIONS(description="""{{ model_description }}""")
  AS (
    {{ sql }}
  );
{%- endmacro %}
