SELECT 
    deno.date,
    deno.brand,
    deno.reference_number,
    deno.group_reference,     
    deno.currency,
    deno.price,
    deno.parent_model,
    deno.specific_model,
    deno.year_introduced, 
    deno.listing_title,
    deno.watch_url,
    deno.condition,
    deno.source,
    deno.listing_type,
    deno.hash_id
FROM {schema_name}.{table_name} deno
WHERE deno.brand IN %(brands)s
AND deno.date BETWEEN %(start_date)s AND %(end_date)s