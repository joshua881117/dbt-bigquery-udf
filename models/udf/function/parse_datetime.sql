{{ 
  config(
    params = [
      'timestamp_expression STRING'
    ], 
    return_type='DATETIME' 
  )
}}
COALESCE(
  SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', timestamp_expression),
  SAFE.PARSE_DATETIME('%Y/%m/%d', timestamp_expression),
  SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', timestamp_expression),
  SAFE.PARSE_DATETIME('%Y-%m-%d', timestamp_expression),
  SAFE.PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ', timestamp_expression),
  PARSE_DATETIME('%Y/%m/%d %H:%M:%S', timestamp_expression)
)
