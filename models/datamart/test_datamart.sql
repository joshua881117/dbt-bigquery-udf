SELECT
  column1,
  datetime
FROM 
  {{ ref('test_table_function') }}('123')