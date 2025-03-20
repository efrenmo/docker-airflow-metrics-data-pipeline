# airflow-metrics-workflow/dags/topic4_parent_model_metrics/price_hist_and_stats_mixed_cond/util_parent_mdl_price_hist_and_stats_incremental_mixed_cond.py

import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import warnings

# sys.path.append('/home/ubuntu/airflow-metrics-workflow/dags/')
# sys.path.append('C:\\Users\\emora\\WCC_Project\\airflow-metrics-workflow\\dags')

# Modules


import modules.utilities
from modules.utilities import (
    setup_logging
)
import modules.data_aggregation
from modules.data_aggregation import (
    agg_func 
)


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


### ***                                    *** ###
### ** WEEKLY AGGREGATION BY PARENT MODEL ** ###
### ***                                    *** ###

# def weekly_aggregation_by_specific_mdl(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Aggregate watch listings data by brand, specific model, and date, computing various
#     price statistics and other metrics.

#     Parameters
#     ----------
#     df : pd.DataFrame
#         Input DataFrame containing watch listings with columns:
#         - brand: Watch brand name
#         - specific_model: Specific model name
#         - date: Listing date
#         - parent_model: Parent model name
#         - currency: Price currency
#         - price: Watch price
#         - reference_number: Watch reference number
#         - source: Listing source

#     Returns
#     -------
#     pd.DataFrame
#         Aggregated DataFrame with columns:
#         - brand: Watch brand name
#         - specific_model: Specific model name
#         - date: Listing date
#         - parent_model: First non-null parent model
#         - currency: First currency value
#         - median_price: Median price
#         - mean_price: Rounded mean price (2 decimals)
#         - high: Maximum price
#         - low: Minimum price
#         - count: Number of listings
#         - source: Aggregated source information
#         - condition: Set to 'Mixed'

#     Notes
#     -----
#     - Groups data by brand, specific_model, and date
#     - Adds 'Mixed' condition to all aggregated records
#     - Sorts results by brand, specific_model, and date
#     - Uses custom aggregation functions:
#         - first_non_null: Returns first non-null value
#         - agg_func: Aggregates unique values into a list. 
#             * See function definition in modules.data_aggregation
#     """

#     result_df = df.groupby(['brand', 'specific_model', 'date']).agg(    
#         parent_model=pd.NamedAgg(column='parent_model', aggfunc=first_non_null),
#         currency=pd.NamedAgg(column='currency', aggfunc='first'),  
#         median_price=pd.NamedAgg(column='price', aggfunc='median'),
#         mean_price=pd.NamedAgg(column='price', aggfunc=lambda x: round(x.mean(), 2)),     
#         high=pd.NamedAgg(column='price', aggfunc='max'),
#         low=pd.NamedAgg(column='price', aggfunc='min'),  
#         count=pd.NamedAgg(column='reference_number', aggfunc='size'),          
#         source=pd.NamedAgg(column='source', aggfunc=agg_func) 
#     ).reset_index()  

#     result_df['condition'] = 'Mixed'
#     result_df  = result_df .sort_values(['brand', 'specific_model', 'date'], ascending=True).reset_index(drop=True)     

#     return result_df


def weekly_aggregation_by_parent_mdl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate watch listings data by brand, parent model, and date, computing various
    price statistics and other metrics.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing watch listings with columns:
        - brand: Watch brand name
        - parent_model: parent model name
        - date: Listing date     
        - currency: Price currency
        - price: Watch price
        - reference_number: Watch reference number
        - source: Listing source

    Returns
    -------
    pd.DataFrame
        Aggregated DataFrame with columns:
        - brand: Watch brand name       
        - date: Listing date
        - parent_model: First non-null parent model
        - currency: First currency value
        - median_price: Median price
        - mean_price: Rounded mean price (2 decimals)
        - high: Maximum price
        - low: Minimum price
        - count: Number of listings
        - source: Aggregated source information
        - condition: Set to 'Mixed'

    Notes
    -----
    - Groups data by brand, parent_model, and date
    - Adds 'Mixed' condition to all aggregated records
    - Sorts results by brand, parent_model, and date
    - Uses custom aggregation functions:
        - first_non_null: Returns first non-null value
        - agg_func: Aggregates unique values into a list. 
            * See function definition in modules.data_aggregation
    """

    result_df = df.groupby(['brand', 'parent_model', 'date']).agg(        
        currency=pd.NamedAgg(column='currency', aggfunc='first'),  
        median_price=pd.NamedAgg(column='price', aggfunc='median'),
        mean_price=pd.NamedAgg(column='price', aggfunc=lambda x: round(x.mean(), 2)),     
        high=pd.NamedAgg(column='price', aggfunc='max'),
        low=pd.NamedAgg(column='price', aggfunc='min'),  
        count=pd.NamedAgg(column='reference_number', aggfunc='size'),          
        source=pd.NamedAgg(column='source', aggfunc=agg_func) 
    ).reset_index()  

    result_df['condition'] = 'Mixed'
    result_df  = result_df .sort_values(['brand', 'parent_model', 'date'], ascending=True).reset_index(drop=True)     

    return result_df




