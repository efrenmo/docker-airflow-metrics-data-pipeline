-- -- Used in the Airflow DAG 
SELECT
    pwp.date,
    pwp.brand,
    pwp.currency,
    pwp.median_price,
    pwp.rolling_avg_of_median_price,
    pwp.mean_price,
    pwp.high,
    pwp.low,    
    pwp.parent_model,        
    pwp.count,
    pwp.condition,
    pwp.brand_slug,
    pwp.pct_change_1w,
    pwp.dol_change_1w,
    pwp.pct_change_1m,
    pwp.dol_change_1m,
    pwp.pct_change_3m,
    pwp.dol_change_3m,
    pwp.pct_change_6m,
    pwp.dol_change_6m,
    pwp.pct_change_1y,
    pwp.dol_change_1y   
FROM {schema_name}.{table_name} pwp
WHERE pwp.date BETWEEN %(start_date)s AND %(end_date)s
AND pwp.brand IN %(brands)s
ORDER BY pwp.brand ASC, pwp.parent_model ASC, pwp.date ASC