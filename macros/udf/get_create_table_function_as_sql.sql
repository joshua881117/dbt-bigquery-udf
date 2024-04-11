{% macro get_create_table_function_as_sql(relation, sql, config, model_description) -%} 

  {%- set params = config.require('params') -%}
  {%- set params_string -%}
    {%- for param in params -%}
      {{ param }}
      {%- if not loop.last -%}, {%- endif -%}
    {%- endfor -%}
  {%- endset -%}

  CREATE OR REPLACE TABLE FUNCTION {{ relation }} ({{ params_string }}) 
  OPTIONS(description="""{{ model_description }}""")
  AS (
    {{ sql }}
  );
{%- endmacro %}
