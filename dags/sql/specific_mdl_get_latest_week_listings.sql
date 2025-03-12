-- Used in the Airflow DAG dag_specific_mdl_price_hist_and_stats_incremental_mixed_cond.py
-- Lates Week's Listings Query
-- This query is used to get the latest week's listings data for the specific models of the brands in the list
SELECT
    deno.date,
    deno.brand,
    deno.reference_number,   
    deno.currency,
    deno.price,
    deno.parent_model,
    deno.specific_model,   
    deno.condition,
    deno.source       
FROM {schema_name}.{table_name} deno
WHERE deno.brand IN %(brands)s
    AND deno.date = %(latest_week_date)s
