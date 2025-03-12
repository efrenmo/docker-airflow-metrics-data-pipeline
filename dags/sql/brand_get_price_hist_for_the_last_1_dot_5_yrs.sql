-- -- Used in the Airflow DAG 
SELECT
    bwp.date,
    bwp.brand,
    bwp.currency,
    bwp.median_price,
    bwp.rolling_avg_of_median_price,
    bwp.mean_price,
    bwp.high,
    bwp.low,      
    bwp.count,
    bwp.condition,
    bwp.brand_slug,
    bwp.pct_change_1w,
    bwp.dol_change_1w,
    bwp.pct_change_1m,
    bwp.dol_change_1m,
    bwp.pct_change_3m,
    bwp.dol_change_3m,
    bwp.pct_change_6m,
    bwp.dol_change_6m,
    bwp.pct_change_1y,
    bwp.dol_change_1y   
FROM {schema_name}.{table_name} bwp
WHERE bwp.date BETWEEN %(start_date)s AND %(end_date)s
AND bwp.brand IN %(brands)s
ORDER BY bwp.brand ASC, bwp.date ASC 