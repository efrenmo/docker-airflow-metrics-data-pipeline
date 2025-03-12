-- Lates Week's Listings Query
-- This query is used to get the latest week's listings data for the specified brands in the list
-- deno = data_enriched_rm_outl
SELECT 
    deno.date,
    deno.brand,
    deno.reference_number,   
    deno.currency,
    deno.price,
    deno.condition,
    deno.source
FROM {schema_name}.{table_name} deno
WHERE deno.brand IN %(brands)s
    AND deno.date = %(latest_week_date)s
