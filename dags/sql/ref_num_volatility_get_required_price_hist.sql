SELECT 
    wpsm.date,
    wpsm.brand,
    wpsm.reference_number,       
    wpsm.rolling_avg_of_median_price    
FROM {schema_name}.{table_name} wpsm
WHERE wpsm.brand IN %(brands)s
AND wpsm.date BETWEEN %(start_date)s AND %(end_date)s
ORDER BY wpsm.brand ASC, wpsm.reference_number ASC, wpsm.date ASC