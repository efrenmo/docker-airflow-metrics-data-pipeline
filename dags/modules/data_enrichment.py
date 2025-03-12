# data_enrichment.py
import pandas as pd
from psycopg2 import sql
from sqlalchemy import text
from modules.database_operations import get_db_connection, get_db_conn_w_sqlalchemy



# def get_library_table(engine, brand, brand_to_library):
#     library_table = brand_to_library.get(brand)
#     if not library_table:
#         raise ValueError(f"No library table found for brand: {brand}")
    
#     query = f"""
#     SELECT brand, reference_number, group_reference, parent_model, specific_model, year_introduced
#     FROM {library_table}
#     """
#     with engine.connect() as connection:
#         return pd.read_sql(query, connection)

def get_library_table(engine, brand, brand_to_library):
    library_table = brand_to_library.get(brand)
    if not library_table:
        raise ValueError(f"No library table found for brand: {brand}")
    
    query = f"""
    SELECT brand, reference_number, group_reference, parent_model, 
           specific_model, year_introduced
    FROM {library_table}
    """
    
    with engine.connect() as connection:
        result = connection.execute(text(query))
        columns = result.keys()
        data = result.fetchall()
        return pd.DataFrame(data, columns=columns)


def load_active_library_tables(weekly_listings, brand_to_library):
    """
    Identifies unique brands from weekly_listings (current active listings) and stores the series 
    in the active_brands variable. Then executes get_library_table() function retrieving the
    library table for each of brands in the active_brands series. 
    """
    active_brands = weekly_listings['brand'].unique()
    library_dfs = {}

    engine = get_db_conn_w_sqlalchemy()
    try:
        for brand in active_brands:
            try:
                library_df = get_library_table(engine, brand, brand_to_library)
                library_dfs[brand] = library_df.drop_duplicates(subset=['reference_number'])
                print(f"Loaded library table for {brand}")
            except ValueError as e:
                print(f"Warning: {str(e)}")
    finally:
        engine.dispose()

    return library_dfs


    


def fill_missing_info(weekly_listings, library_dfs):
    """
    Fill missing information in the weekly_listings DataFrame using reference data from library_dfs.

    This function performs two passes to fill missing data:
    1. First pass: Uses reference_number to fill missing values.
    2. Second pass: Uses group_reference to fill any remaining missing values.

    The function only fills null or empty values, preserving existing data.

    Parameters:
    -----------
    weekly_listings : pandas.DataFrame
        A DataFrame containing watch listings with columns including but not limited to:
        'brand', 'reference_number', 'group_reference', 'parent_model', 'specific_model', 'year_introduced'.

    library_dfs : dict
        A dictionary where keys are brand names and values are pandas DataFrames.
        Each DataFrame contains reference data for a specific brand with columns:
        'reference_number', 'group_reference', 'parent_model', 'specific_model', 'year_introduced'.

    Returns:
    --------
    pandas.DataFrame
        A copy of the input DataFrame with missing values filled where possible.

    Notes:
    ------
    - The function processes each brand separately using the corresponding library DataFrame.
    - It only updates cells that are null or contain empty strings.
    - If a reference_number or group_reference is not found in the library, the corresponding cells remain unchanged.
    - In case of duplicate group_reference values in the library, the first occurrence is used.

    """
    # Create a unique identifier for each row
    # weekly_listings['unique_id'] =  range(len(weekly_listings))
    result = weekly_listings.copy()
    
    for brand, library_df in library_dfs.items():
        # Convert library_df to a dictionary using reference number as key for faster lookups
        reference_library_dict = library_df.set_index('reference_number').to_dict('index')

        # Create a mask for rows that need updating
        mask = ((result['brand'] == brand) &
                (
                result['group_reference'].isnull() | 
                (result['group_reference'] == '') |
                result['parent_model'].isnull() | 
                (result['parent_model'] == '') |
                result['specific_model'].isnull() | 
                (result['specific_model'] == '') |
                result['year_introduced'].isnull() | 
                (result['year_introduced'] == '') |
                (result['year_introduced'] == 0)
                )        
        )

        for col in ['group_reference', 'parent_model', 'specific_model', 'year_introduced']:            
            
            # For 'year_introduced', convert to float and handle NaN
            if col == 'year_introduced':
                # Only fill values that are null, empty, or zero
                col_mask = mask & (result[col].isnull() | (result[col] == '') | (result[col] == 0))                
                result.loc[col_mask, col] = result.loc[col_mask, 'reference_number'].map(
                    lambda x: pd.to_numeric(reference_library_dict.get(x, {}).get(col, pd.NA), errors='coerce')
                )
            else:
                # Only fill values that are null or empty
                col_mask = mask & (result[col].isnull() | (result[col]== ''))
                result.loc[col_mask, col] = result.loc[col_mask, 'reference_number'].map(
                    lambda x: reference_library_dict.get(x, {}).get(col, pd.NA)
                )
     

        # Second pass: Match on brand and group_reference
        mask_second_pass = ((result['brand'] == brand) &
                            (result['group_reference'].notnull()) &
                            (result['group_reference'] != '') &
                            (
                            result['parent_model'].isnull() | 
                            (result['parent_model'] == '') |
                            result['specific_model'].isnull() | 
                            (result['specific_model'] == '') |
                            result['year_introduced'].isnull() | 
                            (result['year_introduced'] == '') |
                            (result['year_introduced'] == 0)
                            )        
        )    
        
        # Convert library_df to a dictionary using group_reference number as the key for faster lookups
        # This approach ensures that even if there are duplicate 'group_reference' values in our library DataFrame, 
        # we'll still be able to create a dictionary for lookups. It uses the first occurrence of each 'group_reference' value.
        group_library_dict = library_df.groupby('group_reference').first().to_dict('index')
        
        for col in ['parent_model', 'specific_model', 'year_introduced']:
            
            # For 'year_introduced', convert to float and handle NaN
            if col == 'year_introduced':
                # Only fill values that are null, empty, or zero
                col_mask = mask_second_pass & (result[col].isnull() | (result[col] == '') | (result[col] == 0))
                result.loc[col_mask, col] = result.loc[col_mask, 'group_reference'].map(
                    lambda x: pd.to_numeric(group_library_dict.get(x, {}).get(col, pd.NA), errors='coerce')
                )
            else:
                # Only fill values that are null or empty
                col_mask = mask_second_pass & (result[col].isnull() | (result[col] == ''))
                result.loc[col_mask, col] = result.loc[col_mask, 'group_reference'].map(
                    lambda x: group_library_dict.get(x, {}).get(col, pd.NA)
                )
      
    result = result.fillna(pd.NA)

    return result


def enrich_weekly_listings(weekly_listings, brand_to_library):   
    library_dfs = load_active_library_tables(weekly_listings, brand_to_library)
    
    # Fill missing values
    enriched_df = fill_missing_info(weekly_listings, library_dfs)
    
    return enriched_df


