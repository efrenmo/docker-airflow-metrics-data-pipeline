# modules/data_loading.py
import os
import io
import boto3
import pandas as pd
import re
from dotenv import load_dotenv
from datetime import datetime, timedelta
import numpy as np 
from modules.utilities import timestamp
# from modules.scraped_listings_clean_up import clean_up_combined
from modules.utilities import setup_logging, get_prior_sunday
import modules.data_cleaning
from modules.data_cleaning import convert_na_to_none



logger = setup_logging(__name__)


def bucket_subfolders_list(given_date: datetime.date) -> list:
    """
    Generate S3 bucket subfolder paths for dates surrounding and including a given date,
    going back to the previous Thursday.

    Args:
        given_date (datetime.date): The input date, expected to be a Sunday through Wednesday

    Returns:
        list: List of S3 path strings in the format 'Raw/Pricing_data_cleaned/YYYY/MM/DD',
              ordered from earliest (Thursday) to latest date.

    Examples:
        For Sunday input:    Returns paths for [Thursday, Friday, Saturday, Sunday]
        For Monday input:    Returns paths for [Thursday, Friday, Saturday, Sunday, Monday]
        For Tuesday input:   Returns paths for [Thursday, Friday, Saturday, Sunday, Monday, Tuesday]
        For Wednesday input: Returns paths for [Thursday, Friday, Saturday, Sunday, Monday, Tuesday, Wednesday]

    Note:
        weekday() mapping:
        - Monday = 0
        - Tuesday = 1
        - Wednesday = 2
        - Thursday = 3
        - Friday = 4
        - Saturday = 5
        - Sunday = 6
    """
    weekday = given_date.weekday()
    
    # Validate input date is Sunday through Wednesday
    if weekday not in [6, 0, 1, 2]:  # Sunday=6, Monday=0, Tuesday=1, Wednesday=2
        raise ValueError("Given date must be a Sunday through Wednesday")
    
    # Calculate how many days to include based on the weekday
    if weekday == 6:  # Sunday
        days_to_include = 4  # Sunday through Thursday
    else:  # Monday(0), Tuesday(1), or Wednesday(2)
        days_to_include = weekday + 5  # Current day through Thursday
    
    # Generate list of dates from Thursday to given_date
    date_list = []
    for i in range(days_to_include-1, -1, -1):  # Reverse the range to get ascending order
        current_date = given_date - timedelta(days=i)
        date_list.append(current_date)
    
    # Convert dates to subfolder paths
    subfolders_list = [
        'Raw/Pricing_data_cleaned/' + date.strftime('%Y/%m/%d') 
        for date in date_list
    ]
    
    return subfolders_list



def dl_bucket_subfolders_list(given_date: datetime.date) -> datetime.date:
    """
    Generate S3 bucket subfolder paths based on date logic for listings data.

    This function determines the appropriate date for the S3 path based on whether
    the given date is a Sunday. If the given date is a Sunday, it uses that date directly.
    If not, it calculates the previous Sunday's date.

    Args:
        given_date (datetime.date): The input date to process.

    Returns:
        list: A single-element list containing the S3 path string in the format:
              'listings_data/raw/YYYY/MM/DD'

    Examples:
        >>> dl_bucket_subfolders_list(datetime.date(2024, 2, 18))  # Sunday
        ['listings_data/raw/2024/02/18']
        >>> dl_bucket_subfolders_list(datetime.date(2024, 2, 20))  # Wednesday
        ['listings_data/raw/2024/02/18']

    Note:
        weekday() mapping:
        - Monday = 0
        - Tuesday = 1
        - Wednesday = 2
        - Thursday = 3
        - Friday = 4
        - Saturday = 5
        - Sunday = 6
    """
    # Check if given date is a Sunday (weekday() == 6)
    if given_date.weekday() == 6:
        target_date = given_date
    else:
        # Get days since last Sunday
        days_since_sunday = given_date.weekday() + 1
        # Subtract those days to get to the prior Sunday
        target_date = given_date - timedelta(days=days_since_sunday)
    
    dl_bucket_subfolders = ['listings_data/raw/'+target_date.strftime('%Y/%m/%d')]

    return dl_bucket_subfolders



# def bucket_subfolders_list(given_date):
#     """
#     For the moment this fucntion is used by both scripts:
#     1. step_1_csvs_to_single_parquet.py
#     2. step_2_raw_to_stage_dl.py

#     These 2 scripts handle the processing and import of new weekly listings to table watch_listings_phase_0 in RDS 
#     """
#     bucket_subfolders = [        
#         'Raw/Pricing_data_cleaned/2025/02/07',
#         'Raw/Pricing_data_cleaned/2025/02/08',
#         # 'Raw/Pricing_data_cleaned/2025/01/17',
#         # 'Raw/Pricing_data_cleaned/2025/01/18',              
#     ]
#     return bucket_subfolders


# def dl_bucket_subfolders_list(given_date):
#     dl_bucket_subfolders = [       
#         # 'listings_data/raw/2025/01/12',
#         'listings_data/raw/2025/02/09',             
#     ]
#     return dl_bucket_subfolders


def column_names_list():
    column_names = [
                    "date",
                    "brand",
                    "reference_number",
                    "group_reference",
                    "parent_model",
                    "specific_model",
                    "currency",
                    "price",
                    "condition",
                    "status",
                    "year_introduced",
                    "year_of_production",
                    "source",
                    "listing_title",
                    "watch_url",
                    "auction_end_time",
                    "authenticity_guarantee",
                    "between_lugs",
                    "bezel_color",
                    "bezel_material",
                    "bracelet_color",
                    "bracelet_material",
                    "brand_slug",
                    "caliber",
                    "case_finish",
                    "case_material",
                    "case_shape",
                    "case_thickness",
                    "caseback",
                    "clasp_type",
                    "country",
                    "crystal",
                    "dial_color",
                    "diameter",
                    "features",
                    "frequency",
                    "jewels",
                    "listing_type",
                    "lug_to_lug",
                    "made_in",
                    "movement",
                    "numerals",
                    "power_reserve",
                    "seller_type",
                    "serial_number",
                    "specific_location",
                    "style",
                    "type",
                    "water_resistance",
                    "weight",
                    "time_updated"
    ]
    return column_names


def get_nearest_sunday(date):

    """
    Returns the date for the nearest sunday relative to the date of the file.
    For consistency, the data in the weekly scrapes will be given the date 
    of the nearest sunday. 

    date.weekday() returns an integer representing the day of the week (Monday is 0, Sunday is 6).
    """

    # Calculate the number of days to the previous Sunday
    days_to_previous_sunday = date.weekday() + 1    
    
    # Calculate the number of days to the next Sunday
    days_to_next_sunday = 7 - (date.weekday() + 1 )
   
    
    # Determine which Sunday is closer
    if days_to_previous_sunday <= days_to_next_sunday:
        return date - timedelta(days=days_to_previous_sunday)
    else:
        return date + timedelta(days=days_to_next_sunday)


def get_date_1_5_years_ago(max_date):
    """
    ** Deprecate - Use function get_date_x_years_ago() instead **
    Returns date 1.5 years ago from the input date
    """
    days_in_1_5_years = int(1.5 * 365.25)
    return max_date - timedelta(days=days_in_1_5_years)


def get_date_x_years_ago(max_date, year_multiplier):
    """
    Returns the date that is x years ago from the input date.

    Parameters:
        max_date (datetime): The reference date from which to calculate the past date.
        year_multiplier (float): The number of years (can be a fraction) to go back in time.

    Returns:
        datetime: The date that is x years ago from the input date.

    Example usage:
    >>> max_date = datetime.now()
    >>> print(get_date_x_years_ago(max_date, 1.5))  # Returns the date 1.5 years ago from today
    """
    # Calculate the number of days in x years, considering leap years
    days_in_x_years = int(year_multiplier * 365.25)
    
    # Subtract the calculated days from the input date
    return max_date - timedelta(days=days_in_x_years)


def load_csv_data_from_s3(bucket_subfolder, column_names=None, data_types=None):
    """
    Load and combine CSV files from an S3 bucket subfolder into a single DataFrame.

    This function retrieves CSV files from a specified S3 bucket subfolder, processes them,
    and combines them into a single DataFrame. It can optionally filter columns and specify
    data types for the columns.

    Args:
        bucket_subfolder (str): The subfolder within the S3 bucket to search for CSV files.
        column_names (list, optional): A list of column names to include in the DataFrame.
            If None, all columns from the CSV files will be included. Default is None.
        data_types (dict, optional): A dictionary specifying the data types for columns.
            Keys should be column names, and values should be the desired data types.
            If None, pandas will infer data types. Default is None.

    Returns:
        pandas.DataFrame: A combined DataFrame containing data from all processed CSV files.

    Notes:
        - The function only processes files with 'Weekly' in their names.
        - Each file's date is extracted from its name and adjusted to the nearest Sunday.
        - For files containing 'ebay_completed_sold' in their names, the 'source' column
          is set to 'eBay Sold'.
        - The S3 bucket name is hardcoded as 'airflow-files-scrapes'.

    Raises:
        boto3.exceptions.Boto3Error: If there are issues accessing the S3 bucket.
        pandas.errors.EmptyDataError: If a CSV file is empty or cannot be read.

    Example:
        df = load_data_from_s3('my_subfolder', 
                               column_names=['brand', 'price', 'condition'],
                               data_types={'price': 'float', 'condition': 'category'})
    """
    
    bucket_name = 'airflow-files-scrapes'
    s3 = boto3.client('s3')

    try:

        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=bucket_subfolder)

        # Check if 'Contents' key is present in the response
        if 'Contents' not in response:
            logger.error(f"No files found in subfolder: {bucket_subfolder}")
            return pd.DataFrame(), False  # Return tuple with empty DataFrame and False flag if zero files found
        
        # Create a list of the files in the specified bucket that contains the word 'Weekly' in its file name
        files = [file['Key'] for file in response['Contents'] if 'weekly' in file['Key'].lower()]
        # files = [file['Key'] for file in response['Contents'] if 'weekly' in file['Key'].lower() and 'chrono24' in file['Key'].lower()]    
        # files = [file['Key'] for file in response['Contents']]

        # Add this check
        if not files:
            logger.info(f"No weekly files found in subfolder: {bucket_subfolder}")
            return pd.DataFrame(), False # Return tuple with DataFrame and False flag if none of the files are weekly files

        logger.info(f"Found {len(files)} weekly files in subfolder: {bucket_subfolder}")

        combined_df = pd.DataFrame()
        for file_key in files:
            # Extract the date from the file name
            date_str = re.search(r'\d{4}_\d{2}_\d{2}', file_key).group()
            file_date = pd.to_datetime(date_str, format='%Y_%m_%d', errors='coerce')
            
            # Find the nearest Sunday
            nearest_sunday = get_nearest_sunday(file_date)

            # Read the CSV file(s) from S3
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            csv_content = response['Body'].read().decode('utf-8')

            # First, read the CSV without specifying columns to get all available columns
            temp_df = pd.read_csv(io.StringIO(csv_content), low_memory=False)
            available_columns = temp_df.columns.tolist()

            
            # If column_names is provided, filter it to only include available columns
            if column_names is not None:
                usecols = [col for col in column_names if col in available_columns]
            else:
                usecols = available_columns

            # Prepare arguments for pd.read_csv
            read_csv_kwargs = {'usecols': usecols}       
            
        
            if data_types is not None:
                read_csv_kwargs['dtype'] = {col: dtype for col, dtype in data_types.items() if col in usecols}

            # Read the CSV file with the prepared arguments
            df = pd.read_csv(io.StringIO(csv_content), **read_csv_kwargs, low_memory=False)
            
            
            # Add missing columns (including 'status') with NaN values
            # This ensures that all specified columns are present in the DataFrame, even if they were missing in the CSV file
            for col in column_names or []:
                if col not in df.columns:
                    df[col] =  pd.NA        

            
            df['date'] = nearest_sunday  # Use the nearest Sunday instead of file_date

            # Check if the file name contains 'Ebay_Completed_Sold' and modify the source column if true
            if 'ebay_completed_sold' in file_key.lower(): 
                df['source'] = 'eBay Sold' # Change to differentiate from regular eBay source
            
            # ###### ***** Temporary Lines Start ****** #######
            # print(f'Started cleaning {file_key} file at {timestamp()}')    
            # df = clean_up_combined(df, file_key) # filekey is passed as a parameter to retreive store name (the name of the site)
            # print(f'{file_key} finished cleaning process at {timestamp()}')
            # ##### ***** Temporary Lines End ****** #######
            
            # Ensure subfolder_df is not empty or all-NA
            if df.empty or df.isna().all().all():
                logger.info(f"Warning: subfolder_df from {file_key} is empty or all-NA.")
                continue     
            
            
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        
        logger.info(f'Data Subfolder {bucket_subfolder} extracted')

        return combined_df, True # Return tuple with DataFrame and True flag

    except boto3.exceptions.ClientError as e:
        logger.error(f"AWS S3 error accessing subfolder {bucket_subfolder}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing subfolder {bucket_subfolder}: {str(e)}")
        raise