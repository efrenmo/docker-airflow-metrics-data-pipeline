SELECT
    swp.date,
    swp.brand,
    swp.currency,
    swp.median_price,
    swp.rolling_avg_of_median_price,
    swp.mean_price,
    swp.high,
    swp.low,    
    swp.parent_model,
    swp.specific_model,    
    swp.count,
    swp.condition,
    swp.brand_slug,
    swp.pct_change_1w,
    swp.dol_change_1w,
    swp.pct_change_1m,
    swp.dol_change_1m,
    swp.pct_change_3m,
    swp.dol_change_3m,
    swp.pct_change_6m,
    swp.dol_change_6m,
    swp.pct_change_1y,
    swp.dol_change_1y   
FROM {schema_name}.{table_name} swp
WHERE swp.date BETWEEN %(start_date)s AND %(end_date)s
AND swp.brand IN %(brands)s
ORDER BY swp.brand ASC, swp.specific_model ASC, swp.date ASC