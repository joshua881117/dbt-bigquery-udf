{% materialization function, adapter='bigquery' %}
  -- Prepare the database
  {% set target_relation = this %}
  {% set existing_relation = load_cached_relation(this) %}
  {% if existing_relation is not none %}
    {{ exceptions.raise_compiler_error('Relation "' ~ target_relation ~ '" exists as ' ~ existing_relation.type) }}
  {% endif %}
  
  -- Run pre-hooks
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- Execute SQL
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  
  -- Get model description
  {% set model_description =  model.description %}

  {% call statement('main') -%}
    {{ get_create_function_as_sql(target_relation, sql, config, model_description) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  -- Run post-hooks
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  
  -- Update the Relation cache
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
