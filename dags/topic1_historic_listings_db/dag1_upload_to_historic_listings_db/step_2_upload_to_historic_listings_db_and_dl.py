#  raw_to_stage_dl.py

"""
Script:
- Assigns row identifiers as hash_ids
- Currency conversion 
- Further reference number clean up
- Deduplicate rows
- Upload to watch_listings_phase_0 RDS table
- Save to s3://wcc-dl/listings_data/staging/YYYY/MM/DD/All_Sources_YYYY-MM-DD_WEEKLY.parquet
"""

import pandas as pd
import numpy as np # pip install numpy==1.21.5
from datetime import datetime
from dotenv import load_dotenv
import boto3
import re
import logging
import sys

# # sys.path.append('/home/ubuntu/metrics_efren/')
# sys.path.append('C:\\Users\\emora\\WCC_Project\\metrics_efren')

import modules.data_loading
from modules.data_loading import bucket_subfolders_list, dl_bucket_subfolders_list
import modules.datalake_operations
from modules.datalake_operations import load_parquet_data_from_s3_v2, convert_to_parquet_and_save_to_s3
import modules.data_cleaning
from modules.data_cleaning import clean_data, convert_na_to_none
import modules.utilities
from modules.utilities import add_hash_id, load_env_vars, setup_logging, get_value_counts_special
import modules.currency_conversion
from modules.currency_conversion import get_exchange_rates, process_dataframe
import modules.database_operations
from modules.database_operations import get_db_connection, upload_dataframe_to_rds
import modules.config
from modules.config import datatype_dictionary

# To conform with future versions of pandas > v3
# pd.set_option('future.no_silent_downcasting', True) # Line not valid with python 3.11

def main(data_interval_end_date):
    logger = setup_logging(__name__)
    logger.info("Starting main application")
    # Load environment variables
    load_env_vars()
    bucket_name = 'wcc-dl'    
    bucket_subfolders =  dl_bucket_subfolders_list(given_date = data_interval_end_date)

    # batch_df = pd.DataFrame()
    for bucket_subfolder in bucket_subfolders:
        # Load data from S3
        dtype_dict = datatype_dictionary()
        subfolder_df = load_parquet_data_from_s3_v2(bucket_name, bucket_subfolder, dtype_dict=dtype_dict)

        # For columns with object datatype pandas does its best to assign the most approporate datatype for the column if possible.
        subfolder_df = subfolder_df.infer_objects(copy=False)

        # Data Cleaning
        cleaned_df = clean_data(subfolder_df)

        # Create row identifier using sha-256 hash
        cleaned_df = add_hash_id(cleaned_df, ['date', 'watch_url', 'source'])

        # Currency Conversion
        exchange_rates = get_exchange_rates()
        all_usd_df = process_dataframe(cleaned_df, exchange_rates)

        # Re-organaize column order
        column_order = [
            "date", "brand", "reference_number", "group_reference", "parent_model", "specific_model", "currency",
            "price", "condition", "status", "year_introduced", "year_of_production", "source",
            "hash_id", "listing_title", "watch_url", "auction_end_time", "authenticity_guarantee",
            "between_lugs", "bezel_color", "bezel_material", "bracelet_color", "bracelet_material",
            "brand_slug", "caliber", "case_finish", "case_material", "case_shape", "case_thickness",
            "caseback", "clasp_type", "country", "crystal", "dial_color", "diameter", "features",
            "frequency", "jewels", "listing_type", "lug_to_lug", "made_in", "movement", "numerals",
            "power_reserve", "seller_type", "serial_number", "specific_location", "style", "type",
            "water_resistance", "weight", "time_updated"
        ]

        all_usd_df = all_usd_df[column_order]

        # Apply the function to the entire DataFrame
        all_usd_df = all_usd_df.map(convert_na_to_none)

        #  Upload to historic listings database in RDS
        schema_name = 'public'
        table_name = 'metrics_watch_listings_phase_0'
        success_rds = upload_dataframe_to_rds(all_usd_df, schema_name=schema_name, table_name=table_name, chunksize=25000)
        if success_rds:
            print("")
            logger.info(f'Upload to RDS successful to {schema_name}.{table_name}')
        else:
            logger.error("Upload to RDS failed")

        
        # Upload to S3
        output_bucket_name = 'wcc-dl'
        output_bucket_subfolder = 'listings_data/staging/'

        # Get the date from all_usd_df and converts its datatype to string
        all_usd_df_date_str = all_usd_df['date'].iloc[1].strftime('%Y-%m-%d')

        # Save dataframe to parquet in AWS s3
        success_s3 = convert_to_parquet_and_save_to_s3(all_usd_df, all_usd_df_date_str, output_bucket_name, output_bucket_subfolder)
        if success_s3:
            logger.info(f'Upload to S3 successful to {output_bucket_name}/{output_bucket_subfolder}')
            
        else:
            logger.error("Upload to S3 failed")
      
             
                
if __name__ == "__main__":    
 
    main()