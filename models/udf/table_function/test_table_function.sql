{{ 
  config(
    params = [
      'id STRING'
    ]
  )
}}
SELECT
  CAST(column1 AS INT64) AS column1,
  {{ ref('parse_datetime') }}(column2) AS datetime
FROM
  {{ source('joshua_dataset', 'test_table') }}
WHERE
  id = '{{ id }}'