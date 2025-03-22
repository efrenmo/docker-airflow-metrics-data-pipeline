import pandas as pd
import pyarrow.parquet as pq
import csv
from datetime import datetime
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import extras
from IPython.display import display, HTML
import boto3
import io
import warnings

import s3fs
import logging
import sys 

from slugify import slugify

from sqlalchemy import create_engine

sys.path.append('/home/ubuntu/airflow-metrics-workflow/dags/')


# To conform with future versions of pandas > v3
pd.set_option('future.no_silent_downcasting', True)

# Set PyArrow as the default engine for pandas
pd.options.io.parquet.engine = 'pyarrow'

# Suppress the warnings about scape characters in regex. Syntax as is works well.
warnings.filterwarnings("ignore", category=SyntaxWarning, module="ref_num_clean_up")

# Modules

import modules.utilities
from modules.utilities import (
    add_hash_id, 
    load_env_vars, 
    setup_logging,    
    get_rows_by_null_condition,    
    create_identifier,
    convert_date_column,
    enforce_datatypes,
    get_prior_sunday    
)
import modules.currency_conversion
from modules.currency_conversion import get_exchange_rates, process_dataframe
import modules.database_operations
from modules.database_operations import (
    get_db_connection
)
import modules.config
from modules.config import brand_to_library_dict, datatype_dictionary, brands_ready_for_metrics



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

### ***                *** ###
### ** Active Listings ** ###
### ***                *** ###

def load_active_listings(brands_list: list[str], data_interval_start_date: datetime) -> pd.DataFrame:
    """
    Loads active listings from the RDS Postgresql database for specified brands
    in the brands_list.

    This function queries the public.metrics_data_enriched table for the latest week's
    watch listings, and returns a formatted DataFrame.

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
        - parent_model: Parent model name
        - specific_model: Specific model name
        - currency: Price currency
        - price: Watch price (excluding zero prices)
        - condition: Watch condition
        - year_introduced: Year the watch model was introduced
        - source: Listing source      

    Notes:
    ------
    - Connects to the database using get_db_connection()
    - Filters listings for the specified date and brands
    - Enforces specific data types using datatype_dictionary()    
    - Sorts results by brand, reference_number, and date
    - Logs processing statistics using logger

    Raises:
    -------
    Any database connection or query execution errors will be propagated.
    """
    conn = get_db_connection()
    
    ACTIVE_LISTINGS_QUERY = """
    SELECT
        de.date,
        de.brand,
        de.reference_number,   
        de.currency,
        de.price,
        de.parent_model,
        de.specific_model,
        de.year_introduced,
        de.condition,
        de.source       
    FROM public.metrics_wcc_data_enriched de
    WHERE de.brand IN %s
        AND de.date = %s
    """

    try:
        # New week's date
        # active_date = datetime(2024, 3, 31).date()

        # Calculate prior Sunday from data_interval_start_date
        active_date = get_prior_sunday(data_interval_start_date)
        logger.info(f"Active listings date: {active_date}")

        # Execute the query. Open a cursor to perform database operation.
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(ACTIVE_LISTINGS_QUERY, (tuple(brands_list), active_date))
            results = cursor.fetchall()

        # Create DataFrame
        active_listings = pd.DataFrame(results)
        dtype_dict = datatype_dictionary()
        active_listings = enforce_datatypes(active_listings, dtype_dict)        
        # Sort and reset index
        active_listings = active_listings.sort_values(['brand', 'reference_number', 'date'], ascending=True).reset_index(drop=True)        
        
        # Task status logs/prints       
        # Get unique values
        unique_brands = active_listings['brand'].unique().tolist()
        # Get number if unique values
        num_unique = active_listings['brand'].nunique()
        logger.info(f'Data for the week ending on {active_date} has {len(active_listings)} active listings')
        logger.info(f"{num_unique} unique brands in the dataframe")
        logger.info(f"Brands included: {unique_brands} ")
        
        return active_listings

    finally:
        conn.close()


