SELECT MAX(date) as max_date
FROM {schema_name}.{table_name}
WHERE brand IN %(brands)s

