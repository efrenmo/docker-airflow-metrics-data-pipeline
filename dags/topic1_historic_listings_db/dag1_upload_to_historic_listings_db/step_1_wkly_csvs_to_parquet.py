# csvs_to_single_parquet.py 

"""
1st scritp to run in the processing of new weekly csv files. 

This script cleans and concatenate weekly csv files from s3, and converts them to parquet file format.

With the actvation of 1 functon, clean_up_combined() which is commented out within the load_csv_data_from_s3() 
funciton in the data_loading.py module, the script can be anabled to re-apply clean_up_combined.py cleaning 
functions to older raw files.

"""  


############# Dependencies ############

# from modules.data_cleaning import clean_data
# from modules.currency_conversion import get_exchange_rates, process_dataframe
# from modules.data_aggregation import aggregate_data
# from modules.database_operations import get_db_connection, load_values_2_rds
import sys 
import io
import boto3
import pandas as pd
import re
from datetime import datetime, timedelta
# from modules.scraped_listings_clean_up import clean_up_combined
from dotenv import load_dotenv
import os


# sys.path.append('/home/ubuntu/metrics_efren/')
# sys.path.append('C:\\Users\\emora\\WCC_Project\\metrics_efren')

import modules.data_cleaning
from modules.data_cleaning import * #   convert_na_to_none, convert_col_dtype_using_dict
import modules.data_loading
from modules.data_loading import * #load_csv_data_from_s3, bucket_subfolders_list, load_env_vars, get_nearest_sunday, column_names_list
import modules.datalake_operations
from modules.datalake_operations import * # convert_to_parquet_and_save_to_s3
import modules.database_operations
from modules.database_operations import *
import modules.utilities
from modules.utilities import *
import modules.config
from modules.config import * # datatype_dictionary


def main(column_names, data_types, data_interval_end_date):
    logger = setup_logging(__name__)
    logger.info("Starting main application")

    # Load environment variables
    load_env_vars()
    
    bucket_subfolders =  bucket_subfolders_list(given_date = data_interval_end_date) 

    output_bucket_name = 'wcc-dl'
    output_bucket_subfolder = 'listings_data/raw/'
    # output_bucket_subfolder = 'listings_data/testing/'

    single_weekly_file = pd.DataFrame()
    current_date = None

    for bucket_subfolder in bucket_subfolders:
        # Load data from S3
        subfolder_df, weekly_files_found = load_csv_data_from_s3(bucket_subfolder, column_names, data_types)

        # Ensure subfolder_df is not empty or all-NA 
        # if subfolder_df.empty:
        #     logger.warning(f"No data loaded from {bucket_subfolder} - skipping to next subfolder")
        #     continue
               
        if subfolder_df.isna().all().all():
            logger.warning(f"All NA values in data from {bucket_subfolder} - skipping to next subfolder")
            continue

        if not weekly_files_found:
            logger.warning(f"Skipping {bucket_subfolder} - no weekly files found")
            continue


        # column_dtypes = datatype_dictionary()
        # convert_col_dtype_using_dict(subfolder_df, column_dtypes)

         # Re-organaize column order
        column_order = [
            "date", "brand", "reference_number", "group_reference", "parent_model", "specific_model", "currency",
            "price", "condition", "status", "year_introduced", "year_of_production", "source",
            "listing_title", "watch_url", "auction_end_time", "authenticity_guarantee",
            "between_lugs", "bezel_color", "bezel_material", "bracelet_color", "bracelet_material",
            "brand_slug", "caliber", "case_finish", "case_material", "case_shape", "case_thickness",
            "caseback", "clasp_type", "country", "crystal", "dial_color", "diameter", "features",
            "frequency", "jewels", "listing_type", "lug_to_lug", "made_in", "movement", "numerals",
            "power_reserve", "seller_type", "serial_number", "specific_location", "style", "type",
            "water_resistance", "weight", "time_updated"
        ]

        subfolder_df = subfolder_df[column_order]

        # Get the date from subfolder_df
        subfolder_date = subfolder_df['date'].iloc[1]

        
        # Convert to string format only if it's a datetime object        
        if isinstance(subfolder_date, datetime):
            # Convert the datetime 2023-08-06 00:00:0 to a string in the format 'YYYY-MM-DD'
            subfolder_date = subfolder_date.strftime('%Y-%m-%d')        


        if current_date is None:
            current_date = subfolder_date

        if subfolder_date == current_date:
            # Append subfolder_df to single_weekly_file
            single_weekly_file = pd.concat([single_weekly_file, subfolder_df], ignore_index=True)
        else: 
            # Remove all rows where the column source is null
            single_weekly_file = single_weekly_file.dropna(subset=['source'])
            # Save the current single_weekly_file to s3            
            convert_to_parquet_and_save_to_s3(single_weekly_file, current_date, output_bucket_name, output_bucket_subfolder)

            # Reinitialize single_weekly_file and update current_date
            single_weekly_file = subfolder_df
            current_date = subfolder_date 

    # Save the final single_weekly_file to s3
    if not single_weekly_file.empty:
        # Remove all rows where the column source is null
        single_weekly_file = single_weekly_file.dropna(subset=['source'])
        convert_to_parquet_and_save_to_s3(single_weekly_file, current_date, output_bucket_name, output_bucket_subfolder)               
        

if __name__ == "__main__":    
    # Load column names list
    column_names = column_names_list()
    
    # Load data types dictionary
    data_types = datatype_dictionary()
    
    main(column_names=column_names, data_types=None)
    