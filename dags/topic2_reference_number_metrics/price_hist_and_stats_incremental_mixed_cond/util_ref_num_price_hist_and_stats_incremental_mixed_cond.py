
# airflow-metrics-workflow/dags/dag2_reference_number_mixed_cond_metrics/ref_num_price_hist_and_stats_incremental_mixed.py

import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import extras
from psycopg2.extras import execute_values
from psycopg2 import sql
import sqlalchemy
from sqlalchemy import create_engine, text
import numpy as np
import boto3
import io
from io import BytesIO
import warnings
import s3fs
import sys 
import re

sys.path.append('/home/ubuntu/airflow-metrics-workflow/dags/')
sys.path.append('C:\\Users\\emora\\WCC_Project\\airflow-metrics-workflow\\dags')

# Modules
import modules.data_loading
from modules.data_loading import (        
    get_nearest_sunday,        
    get_date_1_5_years_ago
    )
import modules.datalake_operations
from modules.datalake_operations import (
    load_parquet_data_from_s3_v2     
)
import modules.data_cleaning
from modules.data_cleaning import (   
    convert_na_to_none, 
    clean_and_convert_to_int32
)
import modules.utilities
from modules.utilities import (
    add_hash_id, load_env_vars, 
    setup_logging,  
    get_rows_by_null_condition,   
    create_identifier, 
    enforce_datatypes,
    get_prior_sunday  
)
import modules.database_operations
from modules.database_operations import (
    get_db_connection, 
    upload_dataframe_to_rds,     
    get_sqlalchemy_connection
)
import modules.data_enrichment
from modules.data_enrichment import (        
    enrich_weekly_listings 
)
import modules.data_aggregation
from modules.data_aggregation import (
    agg_func,
    first_non_null,  
    calc_dollar_and_prct_change,  
    extend_and_ffill,
    incremental_calc_rolling_avg, 
    calculate_pct_dol_changes_for_date,
    process_multiple_offsets,
    within_series_missing_values_test,
    out_of_series_missing_values_test    
)
import modules.ref_num_clean_up
from modules.ref_num_clean_up import reference_number_fx, group_reference_fx, reference_number_function
import modules.config
from modules.config import brand_to_library_dict, datatype_dictionary, brands_ready_for_metrics


# To conform with future versions of pandas > v3
pd.set_option('future.no_silent_downcasting', True)
# Suppress the warnings about scape characters in regex. Syntax as is works well.
warnings.filterwarnings("ignore", category=SyntaxWarning, module="ref_num_clean_up")

# Initialize logging and configuration
logger = setup_logging(__name__)

# Load environtment variable from .env
load_dotenv()

# Access the environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
CURRENCY_API_KEY = os.getenv('CURRENCY_API_KEY')
AWS_REGION = "us-east-2"


# See config module for the list of brands that will be used for the current metric script
brands_list = brands_ready_for_metrics()

def load_new_listings(brands_list: list[str], data_interval_end_date: datetime ) -> pd.DataFrame:
    """
    Loads and processes new watch listings from the RDS Postgresql database for specified brands
    in the brands_list.

    This function queries the metrics_watch_listings_phase_0 table for the latest week's
    watch listings, performs data cleaning, and returns a formatted DataFrame.

    Parameters:
    -----------
    brands_list : list
        A list of brand names to filter the watch listings.
    
    data_interval_end_date : datetime
        The end date of the data interval for which the listings are being processed.
        It will typically match the date of the dag run takes place.

    Returns:
    --------
    pd.DataFrame
        A DataFrame containing the processed watch listings with the following columns:
        - date: Date of the listing
        - brand: Watch brand name
        - reference_number: Watch reference number
        - group_reference: Group reference identifier
        - parent_model: Parent model name
        - specific_model: Specific model name
        - currency: Price currency
        - price: Watch price (excluding zero prices)
        - condition: Watch condition
        - year_introduced: Year the watch model was introduced
        - source: Listing source
        - listing_title: Title of the listing
        - watch_url: URL of the watch listing
        - hash_id: Unique identifier hash

    Notes:
    ------
    - Connects to the database using get_db_connection()
    - Filters listings for January 5, 2025
    - Enforces specific data types using datatype_dictionary()
    - Removes listings with price = 0
    - Sorts results by brand, reference_number, and date
    - Logs processing statistics using logger

    Raises:
    -------
    Any database connection or query execution errors will be propagated.
    """
    conn = get_db_connection()
    
    NEW_WKLY_LISTINGS_QUERY = """
    SELECT
        wl.date,
        wl.brand,
        wl.reference_number,  
        wl.group_reference,
        wl.parent_model,
        wl.specific_model,      
        wl.currency,
        wl.price,
        wl.condition,
        wl.year_introduced,    
        wl.source,
        wl.listing_title,
        wl.watch_url,
        wl.hash_id
    FROM public.metrics_watch_listings_phase_0 wl
    WHERE wl.brand IN %s
        AND wl.date = %s
    """

    try:
        # # New week's date
        # new_week_date = datetime(2024, 11, 3).date()

        # Calculate prior Sunday from data_interval_end_date
        new_week_date = get_prior_sunday(data_interval_end_date)
        logger.info(f"Active Listings week's date: {new_week_date}")
        # Execute the query. Open a cursor to perform database operation.
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(NEW_WKLY_LISTINGS_QUERY, (tuple(brands_list), new_week_date))
            results = cursor.fetchall()

        # Create DataFrame
        new_week_listings = pd.DataFrame(results)
        dtype_dict = datatype_dictionary()
        new_week_listings = enforce_datatypes(new_week_listings, dtype_dict)
        # Filter out rows with price 0
        new_week_listings = new_week_listings[new_week_listings['price'] != 0]
        # Sort and reset index
        new_week_listings = new_week_listings.sort_values(['brand', 'reference_number', 'date'], ascending=True).reset_index(drop=True)        
        
        # Task status logs/prints       
        # Get unique values
        unique_brands = new_week_listings['brand'].unique().tolist()
        # Get number if unique values
        num_unique = new_week_listings['brand'].nunique()
        logger.info(f'Data for the week ending on {new_week_date} has {len(new_week_listings)} rows')
        logger.info(f"{num_unique} unique brands in the dataframe")
        logger.info(f"Brands included: {unique_brands} ")
        
        return new_week_listings

    finally:
        conn.close()

### ***                *** ###
### ** Data Enrichement ** ###
### ***                *** ###

def enrich_data(df):
    """ 
    Apply reference_number and group_reference cleaning functions.
    Enrich dataset with library tables. 
    """
    enriched_df = df.copy()

    # Suppress the warnings about scape characters in regex. Syntax as is works well.
    warnings.filterwarnings("ignore", category=SyntaxWarning, module="ref_num_clean_up")
    # Clean reference number column
    enriched_df['reference_number'] = enriched_df.apply(
        lambda row: reference_number_function(
            row['reference_number'], 
            row['listing_title'], 
            row['brand']
        ), axis=1
    )

    # Add group_reference column to df if does not exist
    if 'group_reference' not in enriched_df.columns:
            enriched_df['group_reference'] = pd.NA

    # For the brands that apply, it will extract group reference number values from the reference_number column 
    enriched_df['group_reference'] = group_reference_fx(enriched_df)

    # Remove rows with empty reference number field
    enriched_df = get_rows_by_null_condition(enriched_df, 'reference_number', get_null=False)
    # Remove all rows where the 'reference_number' is made up of only alphabetical characters or a combination
    # of alphabetical characters and punctuation/signs
    enriched_df = enriched_df[enriched_df['reference_number'].apply(lambda x: bool(re.search(r'\d', str(x), re.IGNORECASE)))]
    # Exclude rows where the number of non-space characters exceeds 25
    enriched_df = enriched_df[enriched_df['reference_number'].str.replace(' ', '').str.len() <= 25]
    enriched_df = enriched_df.sort_values(['brand', 'reference_number', 'date'], ascending=True).reset_index(drop=True)
    logger.info(f"Number of rows after minor cleaning: {len(enriched_df)}")

    # Enriching the dataframe by filling missing parent and specific model values using brand's library data 
    brand_to_library_dict_var = brand_to_library_dict()
    enriched_df = enrich_weekly_listings(enriched_df, brand_to_library_dict_var)

    return enriched_df

### ***           *** ###
### ** LOAD TO RDS ** ###
### ***           *** ###

def load_data_to_rds_tbl(df, schema_name, table_name, drop_source=False):
    """ Load data to the corresponding RDS table with proper formatting."""
    try:
        # Check and clean specific columns if they exist
        if 'year_introduced' in df.columns:
            df = clean_and_convert_to_int32(df, 'year_introduced').copy()
        
        if 'count' in df.columns:
            df = clean_and_convert_to_int32(df, 'count').copy()
        
        if 'median_price' in df.columns:
            df['median_price'] = pd.to_numeric(df['median_price'], errors='coerce')

        # Apply the convert_na_to_none() function to the entire DataFrame
        df = df.map(convert_na_to_none)

        # Conditionally drop the 'source' column if the parameter is set to True
        if drop_source and 'source' in df.columns:
            df = df.drop(columns=['source'])

        # Upload the dataframe to RDS
        success = upload_dataframe_to_rds(df, schema_name, table_name, chunksize=25000)
        
        if not success:
            raise Exception("Failed to load table to RDS")
            
        logger.info(f"Table loaded to RDS successfully")
            
    except Exception as e:
        logger.error(f"Error loading data to RDS: {str(e)}")
        raise


### ***                  *** ###
### ** Removing Outliers ** ###
### ***                 *** ###

def get_outlier_boundaries(engine, brands_list: list, table_name: str) -> pd.DataFrame:
    """ 
    Fetch outlier boundaries from the specified table in RDS for given brands.
    
    Parameters:
        engine: SQLAlchemy engine instance
        brands_list: List of brand names
        table_name: Full table name including schema (e.g., 'metrics.outliers_thresholds_preowned')
    
    Returns:
        DataFrame containing outlier boundaries
    """
    # Converting the Python list to a comma-separated string of quoted values
    # Using the brands_list that was used by the RDS query at the begining
    brand_string = ", ".join(f"'{brand}'" for brand in brands_list)

    # Fetch the most recent outlier boundaries for watches from RDS
    query = f"""    
    WITH latest_dates AS (
        SELECT brand, reference_number, MAX(date) as max_date
        FROM {table_name}
        GROUP BY brand, reference_number
    )
    SELECT        
        ot.date, 
        ot.brand, 
        ot.reference_number, 
        ot.lower_boundary, 
        ot.upper_boundary, 
        ot.condition
    FROM {table_name} ot
    INNER JOIN latest_dates ld 
        ON ot.brand = ld.brand 
        AND ot.reference_number = ld.reference_number 
        AND ot.date = ld.max_date
    WHERE ot.brand IN ({brand_string})        
    """
    
    with engine.connect() as connection:
        result = connection.execute(text(query))
        boundaries_df = pd.DataFrame(
            result.fetchall(), 
            columns=result.keys()
        )
    
    return boundaries_df


def apply_outlier_filtering(df: pd.DataFrame, preowned_boundaries: pd.DataFrame, new_boundaries: pd.DataFrame ) -> pd.DataFrame:
    """
    Apply outlier filtering based on condition-specific boundaries.
    
    Parameters:
        df: Input DataFrame with watch listings
        preowned_boundaries: DataFrame with preowned watch boundaries
        new_boundaries: DataFrame with new watch boundaries
    
    Returns:
        DataFrame with outliers removed
    """
    
    def log_matching_stats(condition_df: pd.DataFrame, condition_name: str) -> None:
        """
        Log statistics about reference number matching with their counterpart in the outliers
        thresholds boundaries table in RDS.

        Args:
        condition_df: DataFrame containing the condition data
        condition_name: Name of the condition being analyzed
        """
        unique_all = condition_df.drop_duplicates(
            subset=['brand', 'reference_number'], 
            keep='first'
        )
        # If the row has a value under the lower_boundary column, it means that the reference number found a 
        # match in the outliers_thresold_boundaried RDS table(s). 
        matched_rows  = get_rows_by_null_condition(condition_df, 'lower_boundary', get_null=False)
        unique_matched = matched_rows .drop_duplicates(
            subset=['brand', 'reference_number'], 
            keep='first'
        )
        
        total_unique_count = len(unique_all)
        unique_matched_count = len(unique_matched)
        match_rate = (unique_matched_count/total_unique_count*100) if total_unique_count > 0 else 0        

        logger.info(
            f"For {condition_name} condition: "
            f"{total_unique_count} unique reference numbers found, "
            f"{unique_matched_count} had outlier threshold boundaries "
            f"({match_rate:.1f}% match rate)"
        )    
    
    # For outlier detection and calculation we are separating rows between new and preowned from the wcc_enriched df
    
    # Define preowned conditions
    preowned_conditions = ['Pre-Owned', 'Pre-Owned, Seller Refurbished', 'Pre-Owned, Vintage']
    
    # Split data by condition

    # Merge pre-owned boundaries 
    # Merging wcc_enriched df (preowned rows only) with preowned_boundaries df (which has the upper and lower boundaries values)
    preowned_df = df[df['condition'].isin(preowned_conditions)].merge(
        preowned_boundaries[['brand', 'reference_number', 'lower_boundary', 'upper_boundary']],
        on=['brand', 'reference_number'],
        how='left',
        suffixes=('',' _thresholds')
    )
    
    # Merge New boundaries 
    # Merging wcc_enriched df (new rows only) with new_boundaries df (which has the upper and lower boundaries values)
    new_df = df[df['condition'] == 'New'].merge(
        new_boundaries[['brand', 'reference_number', 'lower_boundary', 'upper_boundary']],
        on=['brand', 'reference_number'],
        how='left',
        suffixes=('',' _thresholds')
    )
    
    ### Informational Logs/Prints ####
    logger.info(f'''
                wcc_enriched has {len(df)} rows. {len(preowned_df)} rows
                with Pre-Owned watch condition, and {len(new_df)} with New watch condition
                ''')

    """
    Isolate the rows from the original wcc_enriched df that have a null value under the column condition.
    For these rows we can't remove outliers. Because we can't know for sure if we can use the new, 
    or preowned set of outliers thesholds boundaries.

    We will concatenate this portion to the final wcc_df_filtered dataframe, which is the portion of the wcc_data_enriched 
    dataframe that we can remove outliers from
    """
    rows_with_null_value_condition = get_rows_by_null_condition(df, 'condition', get_null=True)
    logger.info(f'There are {len(rows_with_null_value_condition)} rows in the wcc_enriched df with null value condition')

    # Filter outliers
    preowned_filtered_for_outliers = preowned_df[
        ((preowned_df['price'] >= preowned_df['lower_boundary']) &
         (preowned_df['price'] <= preowned_df['upper_boundary'])) |
        (preowned_df['lower_boundary'].isna() & 
         preowned_df['upper_boundary'].isna())
    ]
    
    new_filtered_for_outliers = new_df[
        ((new_df['price'] >= new_df['lower_boundary']) &
         (new_df['price'] <= new_df['upper_boundary'])) |
        (new_df['lower_boundary'].isna() & 
         new_df['upper_boundary'].isna())
    ]
    # Concatenate all 3 dataframes 
    filtered_for_outliers_df = pd.concat([preowned_filtered_for_outliers, new_filtered_for_outliers, rows_with_null_value_condition])        
    filtered_for_outliers_df = filtered_for_outliers_df.sort_values(['brand', 'reference_number', 'date'], ascending=True).reset_index(drop=True)
    logger.info(f'filtered_for_outliers_df has {len(filtered_for_outliers_df)} rows')
    
    return filtered_for_outliers_df


def remove_outliers(df, brands_list):
    """
    Remove outliers based on pre-owned and new conditions outlier threshold boundaries
    """
    with get_sqlalchemy_connection() as engine: 
        preowned_boundaries = get_outlier_boundaries(engine, brands_list, "metrics.outliers_thresholds_preowned")
        new_boundaries = get_outlier_boundaries(engine, brands_list, "metrics.outliers_thresholds_new")   

    return apply_outlier_filtering(df, preowned_boundaries, new_boundaries)


### ***                              *** ###
### ** RESAMPLING BY WEEKLY FREQUENCY ** ###
### ***                              *** ###

def resampling_by_weekly_frequency(df):
    """Process weekly aggregations of the filtered data"""
    result_df = df.groupby(['brand', 'reference_number', 'date']).agg(    
        group_reference=pd.NamedAgg(column='group_reference', aggfunc=first_non_null),
        currency=pd.NamedAgg(column='currency', aggfunc='first'),
        median_price=pd.NamedAgg(column='price', aggfunc='median'),  
        mean_price=pd.NamedAgg(column='price', aggfunc=lambda x: round(x.mean(), 2)),        
        high=pd.NamedAgg(column='price', aggfunc='max'),
        low=pd.NamedAgg(column='price', aggfunc='min'),
        parent_model=pd.NamedAgg(column='parent_model', aggfunc=first_non_null),
        specific_model=pd.NamedAgg(column='specific_model', aggfunc=first_non_null),
        year_introduced=pd.NamedAgg(column='year_introduced', aggfunc=first_non_null),
        count = pd.NamedAgg(column='reference_number', aggfunc='count'),              
        source=pd.NamedAgg(column='source', aggfunc=agg_func)  
    ).reset_index()  

    result_df ['condition'] = 'Mixed'

    result_df  = result_df .sort_values(['brand', 'reference_number', 'date'], ascending=True).reset_index(drop=True)
    # Adding identifier column
    result_df  = create_identifier(result_df , 'reference_number')   

    return result_df

### ***               *** ###
### ** HISTORICAL DATA ** ###
### ***               *** ###

def load_historical_data(brands_list):
    """
    Load historical data from the metrics.wcc_wkl_prices_and_stats_mixed table.
    
    Parameters:
        brands_list (list): List of brand names to query
        
    Returns:
        pd.DataFrame: DataFrame containing historical data with proper datatypes
    """

    # We need to query historical data (from atleast 1.5 yrs ago) and combine it with this weeks data 
    # to process and calculate the necessary metrics.
    conn = get_db_connection()
    try:
        # Query to get the maximum date
        max_date_query = """
        SELECT MAX(date) as max_date
        FROM metrics.wcc_wkl_prices_and_stats_mixed
        WHERE brand = %s
        """

        # Execute the max date query
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(max_date_query, (brands_list[0],))  # Taking the first brand from brands_list parameter to get the max date
            max_date_result = cursor.fetchone()
            max_date = max_date_result['max_date']

            logger.info(f"Max date in the historical data: {max_date}")

            # max_date = datetime(2024, 4, 7).date() # Temporary line

            # Calculate the date 1.5 years ago and adjust to the nearest Sunday
            date_1_5_years_ago = get_date_1_5_years_ago(max_date)
            adjusted_start_date = get_nearest_sunday(date_1_5_years_ago)

            # Main query to fetch data
            HISTORICAL_DATA_QUERY = """
            SELECT
                wwp.date,
                wwp.brand,
                wwp.reference_number,
                wwp.identifier,  
                wwp.group_reference,
                wwp.currency,
                wwp.median_price,
                wwp.rolling_avg_of_median_price,
                wwp.mean_price,
                wwp.high,
                wwp.low,    
                wwp.parent_model,
                wwp.specific_model,      
                wwp.year_introduced, 
                wwp.count,
                wwp.condition,
                wwp.brand_slug,
                wwp.pct_change_1w,
                wwp.dol_change_1w,
                wwp.pct_change_1m,
                wwp.dol_change_1m,
                wwp.pct_change_3m,
                wwp.dol_change_3m,
                wwp.pct_change_6m,
                wwp.dol_change_6m,
                wwp.pct_change_1y,
                wwp.dol_change_1y   
            FROM metrics.wcc_wkl_prices_and_stats_mixed wwp
            WHERE wwp.date BETWEEN %s AND %s
            AND wwp.brand IN %s
            ORDER BY wwp.brand ASC, wwp.reference_number ASC, wwp.date ASC
            """

            # with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute( HISTORICAL_DATA_QUERY, ( adjusted_start_date, max_date, tuple(brands_list) ) )
            result = cursor.fetchall()

        # Create DataFrame
        df = pd.DataFrame(result)
        dtype_dict = datatype_dictionary()
        df = enforce_datatypes(df, dtype_dict)     

        return df

    finally:
        conn.close()

### ***                                             *** ###
### ** MERGING THIS WEEK'S DATA WITH HISTORICAL DATA ** ###
### ***                                             *** ###

def combine_new_and_historical_data(new_weekly_data, brands_list):
    """Combine new weekly resampled data with historical weekly data (stored in RDS) and calculate metrics"""
    historical_data = load_historical_data(brands_list)

    # Get unique brand names
    unique_brands = historical_data['brand'].unique().tolist()
    # Get number of unique brand names
    num_unique = historical_data['brand'].nunique()
    # Checking the date range of our dataframe
    max_date = historical_data["date"].max().date()
    min_date =  historical_data["date"].min().date()
    
    logger.info(
        f"{len(historical_data)} rows of historical data was retrieved from wcc_wkl_prices_and_stats_mixed table in RDS \n"
        f"the earliest date in prev_wcc_mixed_wkl df is {min_date}, and latest day is {max_date} \n"
        f"there are {num_unique} unique brands in the prev_wcc_mixed_wkl dataframe \n"
        f"Brands included: {unique_brands}"
    )
    # Concatenating historical data with this weeks data
    combined_data = pd.concat([historical_data, new_weekly_data], ignore_index=True, sort=False)

    combined_data = combined_data.sort_values(['brand', 'reference_number', 'date'], ascending=[True, True, True]).reset_index(drop=True)
    logger.info(f"There are {len(combined_data)} rows after combining historical weekly data and the new week's weekly data")   
    
    return combined_data


### ***                                           *** ###
### ** FORWARD FILL WITHIN & OUTER EXTEND AND FILL ** ###
### ***                                           *** ###

def extend_and_forward_fill(combined_data):
    """
    - It would be uncommon for the dataframe to need forward-fill within the date boundaries of the dataframe.
    Because, we fetch proccessed historical data from rds. 

    - But, it should be expected for the dataframe to need extend and fill outside the date boundaries of the dataframe.
    Because this weeks data is unlikely to contain all reference numbmers in historical data.
    For absent reference_number in current week we fill forward to cover the gaps.
    """
    
    max_date = combined_data["date"].max().date()
    nearest_sunday = get_nearest_sunday(max_date)
    logger.info(f"{nearest_sunday} is the latest date in the combined_data dataframe")
    
    columns_to_ffill = [
        'group_reference', 
        'currency',  
        'median_price', 
        'mean_price', 
        'high', 
        'low', 
        'parent_model', 
        'specific_model', 
        'year_introduced',
        'identifier',
        'brand_slug'
    ]

    logger.info("Starting Extend and Fill")

    extended_and_ffilled = (combined_data
                    .groupby(['brand', 'reference_number'])
                    .apply(extend_and_ffill, end_date=nearest_sunday, columns_to_ffill=columns_to_ffill, include_groups=False)
                    .reset_index(level=['brand', 'reference_number'])
    )
    
    out_of_bounds_test = out_of_series_missing_values_test(extended_and_ffilled, groupby_columns=['brand', 'reference_number'], date_column='date')
    logger.info(f" Out of bounds test: {out_of_bounds_test}")
    
    logger.info(f"""
        Initially there were {len(combined_data)} rows in the combined df.
        After extend and filled we have {len(extended_and_ffilled)} rows.
        """
    )
    return extended_and_ffilled


### ***                           *** ###
### ** ROLLING AVERAGE CALCULATION ** ###
### ***                           *** ###


def calculate_rolling_average(extended_and_ffilled):
    logger.info("Starting rolling average calculation")
    
    # Sorting the dataframe by date to ensure correct rolling calculation
    extended_and_ffilled.sort_values(['brand', 'reference_number', 'date'], ascending=[True, True, True], inplace=True)
    extended_and_ffilled.reset_index(drop=True, inplace=True)
    # Searches for rows with missing values in the rolling_avg_of_median_price column and marks the rows with a True or False value.
    extended_and_ffilled['needs_rolling_avg'] = extended_and_ffilled['rolling_avg_of_median_price'].isna()

    with_rolling_avg_calc = (extended_and_ffilled.groupby(['brand', 'reference_number'])
                             .apply(incremental_calc_rolling_avg, include_groups=False)
                             .reset_index(level=['brand', 'reference_number'])
    )

    # Drop the temporary 'needs_rolling_avg' column
    with_rolling_avg_calc.drop('needs_rolling_avg', axis=1, inplace=True)
    logger.info("Rolling average calculation completed")
    
    return with_rolling_avg_calc


### ***                    *** ###
### ** APPLY $ AND % CHANGE ** ###
### ***                    *** ###

def calculate_dollar_and_pct_change(with_rolling_avg_calc):
    logger.info("Calculating dollar and percentage changes")
    min_date = with_rolling_avg_calc["date"].min().date()
    max_date = with_rolling_avg_calc["date"].max().date()
    current_date = get_nearest_sunday(max_date)
    
    logger.info(f"The input dataframe ranges from {min_date} to {max_date}")
    
    offsets = [7, 30, 90, 180, 365] # In days ex: 7 days, 30 days, 90 days, 180 days, 365 days
    final_df = process_multiple_offsets(with_rolling_avg_calc, current_date, offsets)
    
    # The filtered df contain the rows that will be loaded to the RDS table. 
    final_filtered = final_df[final_df['date'] == pd.to_datetime(current_date)
                    ].sort_values(['brand', 'reference_number'], ascending=True).reset_index(drop=True)
    
    logger.info(f"Final filtered dataframe has {len(final_filtered)} rows")
    logger.info(f"Number of rows to be uploaded: {len(final_filtered)}")
    
    return final_filtered

### ***             *** ###
### ** Main Function ** ###
### ***             *** ###

def main():
        # # Initialize logging and configuration
        # logger = setup_logging(__name__)
        brands_list = brands_ready_for_metrics()
        stage_schema_name='public'
        stage_table_1_name= 'metrics_wcc_data_enriched' 
        stage_table_2_name= 'metrics_wcc_data_enriched_rm_outl' 
        stage_table_3_name= 'metrics_wcc_wkl_resampled_enriched'
        prod_schema_name = 'metrics'
        prod_table_1_name= 'wcc_wkl_prices_and_stats_mixed'
        
        # Execute pipeline
        try:
            # 1. Load and preprocess new weekly listings data
            new_week_listings = load_new_listings(brands_list)
            
            # 2. Enrich data
            wcc_enriched = enrich_data(new_week_listings)

            # 3. Load data to the wcc_data_enriched RDS table before removing outliers
            load_data_to_rds_tbl(wcc_enriched, schema_name=stage_schema_name, table_name=stage_table_1_name, drop_source=False)
                        
            # 4. Remove outliers
            wcc_filtered_for_outliers = remove_outliers(wcc_enriched, brands_list)
            
            # 5. Loading data to 'wcc_data_enriched_rm_outl' RDS table after removing outliers
            load_data_to_rds_tbl(wcc_filtered_for_outliers, schema_name=stage_schema_name, table_name=stage_table_2_name, drop_source=False)
            
            # 6. Process weekly aggregations
            wcc_mixed_wkl = resampling_by_weekly_frequency(wcc_filtered_for_outliers)
            logger.info(f'wcc_mixed_wkl has {len(wcc_mixed_wkl )} rows.')
            
            # 7. Load 'wcc_mixed_wkl' to rds table public.metrics_wcc_wkl_resampled_enriched.
            load_data_to_rds_tbl(wcc_mixed_wkl, schema_name=stage_schema_name, table_name=stage_table_3_name, drop_source=False)
            
            # 8. Combine new with historical data.
            wcc_mixed_wkl_updated = combine_new_and_historical_data(wcc_mixed_wkl, brands_list)

             # 9. Extend and Forward Fill each brand and reference number combination.
            wcc_mixed_wkl_updated = extend_and_forward_fill(wcc_mixed_wkl_updated)

            # 10. Calculating rolling average incrementally
            wcc_mixed_wkl_updated = calculate_rolling_average(wcc_mixed_wkl_updated)
            
            # 11. Calculate dollar and percentage changes
            final_filtered = calculate_dollar_and_pct_change(wcc_mixed_wkl_updated) 
            
            # 12. Upload 'final_filtered' df to 'wcc_wkl_prices_and_stats_mixed' table in RDS            
            # Loads only rows with date of the most recent sunday (date of current week)
            load_data_to_rds_tbl(final_filtered, schema_name=prod_schema_name, table_name=prod_table_1_name, drop_source=True)
            
            logger.info("Pipeline completed successfully")
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

if __name__ == "__main__":
    main()
        