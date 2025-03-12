# modules/data_cleaning.py
import pandas as pd
import re
import string
import logging
import numpy as np

import modules.utilities
from modules.utilities import setup_logging

logger = setup_logging(__name__)


def convert_na_to_none(value):
    """
    Converts value from pandas.NA to None

    Sample use (convert all pandas.NA in df to None): 
    df = df.map(convert_na_to_none)    
    """
    if pd.isna(value):
        return None
    return value



def convert_col_dtype_using_dict(df, column_dtypes):
    """
    Converts the columns of a DataFrame to specified data types using a dictionary.

    Parameters:
    df (pandas.DataFrame): The DataFrame to be converted.
    column_dtypes (dict): A dictionary where keys are column names and values are desired data types ('str', 'int', 'float').

    Returns:
    pandas.DataFrame: The DataFrame with converted column data types.
    """
    for column, dtype in column_dtypes.items():
        if column in df.columns:
            try:
                if dtype == 'string':
                    # Convert to string
                    df[column] = df[column].astype('string')
                elif dtype == 'int64':
                    # Convert to nullable integer
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                elif dtype == 'float':
                    # Convert to float
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
            except Exception as e:
                print(f"Error converting column '{column}' to {dtype}: {e}")
    return df

def handle_null_columns(df):
    """ 
    Used to To prevent errors: 
    - ERROR - Error uploading to S3: ("Expected bytes, got a 'float' object", 'Conversion failed for column caliber with type object')
    - ERROR - Error uploading to S3: ("Expected bytes, got a 'float' object", 'Conversion failed for column reference_number with type object')
    # when executing datalake_operations.convert_to_parquet_and_save_to_s3()
    # Convert all values in the 'caliber' column to strings
    """
    
    columns_to_convert = ['caliber', 'reference_number', 'group_reference']
    
    for column in columns_to_convert:
        if column in df.columns:
            df[column] = df[column].astype(str)
    
    # If continues to produce error, uncomment the lines below
    ## Replace 'None' and 'nan' strings with a placeholder, e.g., 'unknown'
    # df['caliber'].replace(['None', 'nan'], 'unknown', inplace=True)
    return df


def clean_and_standarize_year_column(df, column_name):
    """
    Cleans and standardizes year columns in a DataFrame by extracting years from various formats
    and converting them to proper numeric format for parquet storage.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame containing the year column to clean
    column_name : str
        Name of the column containing year data
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with the cleaned year column
    """
    # First extract years using the existing robust method
    year_pattern = re.compile(r'\d{4}')
    
    def extract_and_convert_year(series):
        # Handle numeric values
        numeric_mask = pd.to_numeric(series, errors='coerce').notna()
        result = pd.Series(index=series.index, dtype='float64')
        result[numeric_mask] = pd.to_numeric(series[numeric_mask], errors='coerce')
        
        # Handle string values
        string_mask = ~numeric_mask
        string_series = series[string_mask].astype(str)
        
        # Remove 's' and split
        string_series = string_series.str.split('s').str[0]
        
        # Extract years using regex
        extracted_years = string_series.str.extract(f'({year_pattern.pattern})', expand=False)
        result[string_mask] = pd.to_numeric(extracted_years, errors='coerce')
        
        return result
    
    # Extract years
    df[column_name] = extract_and_convert_year(df[column_name])
    
    # Convert to integer and handle NaN values for parquet compatibility
    df[column_name] = df[column_name].apply(lambda x: int(x) if pd.notna(x) else None)
    df[column_name] = df[column_name].fillna(0).astype('Int64')
    
    return df


# def extract_year_from_string(series):
#     """
#        Vectorized function to extract years from a pandas Series containing various date formats.

#     This function processes a Series of mixed data types (numeric and string) to extract
#     4-digit years. It handles several scenarios:
#     1. Numeric values (int or float) are converted directly.
#     2. String values ending with 's' (e.g., '1960s') have the 's' removed.
#     3. For strings with multiple years (e.g., '1950s 1960s'), it extracts the first 4-digit year.
#     4. Any other string format is searched for a 4-digit number.

#     Parameters:
#     -----------
#     series : pandas.Series
#         A Series containing year information in various formats.

#     Returns:
#     --------
#     pandas.Series
#         A Series of extracted years as floating-point numbers. 
#         Values that couldn't be converted are returned as NaN.

#     Examples:
#     ---------
#     >>> import pandas as pd
#     >>> s = pd.Series(['2020', '1990s', '1980.0', '1950s 1960s', 'unknown'])
#     >>> extract_year_vectorized(s)
#     0    2020.0
#     1    1990.0
#     2    1980.0
#     3    1950.0
#     4       NaN
#     dtype: float64

#     Notes:
#     ------
#     - The function uses vectorized operations for improved performance on large datasets.
#     - Numeric values are prioritized over string parsing.
#     - For string values, the first 4-digit number found is extracted.
#     - The returned Series contains float values to accommodate NaN for non-convertible entries.
    
#     How to apply it:
#     Example:
#     >>> df['year_introduced'] = extract_year_from_string(df['year_introduced'])

#     """
#     # Compile the regex pattern inside the function
#     year_pattern = re.compile(r'\d{4}')

#     # Handle numeric values
#     numeric_mask = pd.to_numeric(series, errors='coerce').notna()
#     result = pd.Series(index=series.index, dtype='float64')
#     result[numeric_mask] = pd.to_numeric(series[numeric_mask], errors='coerce')
    
#     # Handle string values
#     string_mask = ~numeric_mask
#     string_series = series[string_mask].astype(str)
    
#     # Remove 's' and split
#     string_series = string_series.str.split('s').str[0]
    
#     # Extract years using regex
#     extracted_years = string_series.str.extract(f'({year_pattern.pattern})', expand=False)
#     result[string_mask] = pd.to_numeric(extracted_years, errors='coerce')
    
#     return result

def remove_eBay_eBaySold_duplicates(df):
    """
    Remove duplicates by prioritizing 'eBay Sold' over 'eBay' for identical rows,
    while retaining rows with other source values.
        
    The overall expression is designed to create a boolean mask that selects rows based on the following logic:
    - The mask keeps rows where the source is 'eBay Sold', and 
    - keep rows where the source is 'eBay' and they are duplicates based on the ['date', 'watch_url'] columns.
    
    Args:
    df (pandas.DataFrame): The DataFrame containing 'date', 'watch_url', and 'source' columns.
    
    Returns:
    pandas.DataFrame: DataFrame with duplicates removed, prioritizing 'eBay Sold'.
    """
    # Create a mask to identify rows to keep
    mask = (df['source'] == 'eBay Sold') | ((df['source'] == 'eBay') & df.duplicated(['date', 'watch_url'], keep=False))

    # Filter the DataFrame based on the mask and sort to prioritize 'eBay Sold'
    df_filtered = df[mask].sort_values('source', ascending=False)

    # Drop duplicates on 'date' and 'watch_url', keeping the first occurrence which will be 'eBay Sold' if present.
    df_unique = df_filtered.drop_duplicates(subset=['date', 'watch_url'], keep='first')

    # Append rows with other source values
    other_sources = df[~mask]
    df_result = pd.concat([df_unique, other_sources], ignore_index=True)

    return df_result


def remove_duplicates(df, columns, keep='first'):
    """
    Remove duplicate rows from a DataFrame based on specified columns.

    Parameters:
    df (pandas.DataFrame): The input DataFrame.
    columns (list): List of column names to consider for identifying duplicates.
    keep (str, optional): Which duplicate to keep. 
                          Options are 'first' (default), 'last', or False.

    Returns:
    pandas.DataFrame: DataFrame with duplicates removed and index reset.
    """
    deduplicated_df = df.drop_duplicates(subset=columns, keep=keep)
    deduplicated_df.reset_index(drop=True, inplace=True)

    return deduplicated_df


# def replc_nan_for_none(df):
#     # Replace all numpy NaN, pandas NA, and None with None (which PostgreSQL understands as NULL)
#     # To be used right before uploading data to RDS PostgreSQL database table
#     return df.replace({np.nan: None, pd.NA: None})


def uppercase_alphabet(df):
    """
    Converts all alphabetical characters in the 'reference_number' column to uppercase
    while keeping the rest of the characters (numbers, punctuation) unchanged.
    """
    lowercase_chars = string.ascii_lowercase
    uppercase_chars = string.ascii_uppercase
    trans_table = str.maketrans(lowercase_chars, uppercase_chars)
    df['reference_number'] = df['reference_number'].str.translate(trans_table)
    return df

def remove_bom(text):
    """
    Remove ZERO WIDTH NO-BREAK SPACE (U+FEFF) from the given text.
    """
    return text.replace('\ufeff', '')

def remove_non_ascii(text):
    """
    Remove RIGHT-TO-LEFT MARK (U+200F), RIGHT-TO-LEFT OVERRIDE (U+202E), and EN DASH (U+2013) from the given text.
    """
    return re.sub(r'[\u200F\u202E\u2013]', '', text)

def contains_non_english(text):
    """
    Check if the given string contains any remaining non-ASCII characters.
    """
    pattern = r'[^\x00-\x7F\x20-\x2F\x3A-\x40\x5B-\x60\x7B-\x7E]'
    return bool(re.search(pattern, text))

def clean_reference_number(ref_num):
    """
    Remove leading non-alphanumeric characters and spaces, and remove enclosing brackets, parentheses, and quotes.
    """
    # Remove leading non-alphanumeric characters and spaces
    cleaned_ref_num = re.sub(r'^[\W\s]+', '', ref_num)
    
    # Remove enclosing brackets, parentheses, and quotes
    cleaned_ref_num = re.sub(r'[\'"()\[\]]', '', cleaned_ref_num)
    
    return cleaned_ref_num

def remove_suffix(text, suffix):
    """
    Remove the specified suffix from the text if it exists.
    """
    if text.endswith(suffix):
        return text[:-len(suffix)]
    return text

# def clean_and_convert_to_int32(df, column_name):
#     """
#     Clean and convert a specified column to Int64 type.
    
#     Parameters:
#     df (pandas.DataFrame): The input DataFrame
#     column_name (str): The name of the column to clean and convert
    
#     Returns:
#     pandas.DataFrame: The DataFrame with the specified column cleaned and converted
#     """

#     # Fill NaN values with a placeholder 
#     df[column_name] = df[column_name].fillna(0) 

#     # Convert to integer type where possible    
#     df[column_name] = df[column_name].astype('int32')
    
#     return df


def clean_and_convert_to_int32(df, column_name):
    """
    Clean and convert a specified column to Int64 type.
    
    Parameters:
    df (pandas.DataFrame): The input DataFrame
    column_name (str): The name of the column to clean and convert
    
    Returns:
    pandas.DataFrame: The DataFrame with the specified column cleaned and converted
    """

    # Fill NaN values with a placeholder 
    df[column_name] = df[column_name].fillna(0) 

    df[column_name] = df[column_name].astype(float)

    # Convert to integer type where possible    
    df[column_name] = np.floor(df[column_name]).astype('int32')
    
    return df


def clean_data(combined_df):  
    logger.info(f'{len(combined_df)} rows before starting cleaning')
    
    # Retain rows where status is NaN or not 'Deleted'
    # Earlier raw files did not have a status column so the values for those rows would have been nan and we would
    # like to retain those
    cleaned_df = combined_df[(combined_df['status'].isna()) | (combined_df['status'] != 'Deleted')]
    
    logger.info(f'{len(cleaned_df)} rows after removing rows with deleted status')

    # cleaned_df = cleaned_df[cleaned_df['reference_number'].notnull()]
    cleaned_df = cleaned_df[cleaned_df['reference_number'].notnull() & (cleaned_df['reference_number'] != '')].copy()
    cleaned_df = uppercase_alphabet(cleaned_df)

    # Remove Duplicates
    cleaned_df = remove_eBay_eBaySold_duplicates(cleaned_df)    
    cleaned_df = remove_duplicates(cleaned_df, columns=['date', 'watch_url'])

    
    ### **** Temporary *** ###
    ## Created because a file from feb 2014 didnt not have watch_url values
    # cleaned_df = remove_duplicates(cleaned_df, columns=['date', 'listing_title'])
    ### **** Temporary *** ###

    logger.info(f'{len(cleaned_df)} rows after removing duplicates')
    
    values_to_exclude = {
        'UNAVAILABLE', 'DOES NOT APPLY', 'NOT', 'NO', 'WATCH', 'NOT APPLICABLE', 'UNKNOWN',
        'NA', 'NONE', 'REFERENCE NUMMBER:', 'NON APPLICABLE', 'DOSE NOT APPLY', 'NOT APPLY',
        'N A', 'N/A', 'ASK', 'SHOW ALL', 'NO REFERENCE'
    }
    cleaned_df = cleaned_df[~cleaned_df['reference_number'].isin(values_to_exclude)]

    logger.info(f'{len(cleaned_df)} rows after values_to_exclude')

    # Check the datatype of the 'price' column
    if pd.api.types.is_string_dtype(cleaned_df['price']) or pd.api.types.is_object_dtype(cleaned_df['price']):
        # Apply the function only if the column is string or object type
        cleaned_df['price'] = cleaned_df['price'].apply(lambda x: pd.to_numeric(x.replace(',', '') if isinstance(x, str) else x, errors='coerce'))
    else:
        # If the column is already numeric, no need to do anything
        pass

    # Optionally, we can ensure the final type is float
    # cleaned_df['price'] = cleaned_df['price'].astype(float)

    # Previous logic
    # cleaned_df['price'] = cleaned_df['price'].str.replace(',', '').apply(lambda x: pd.to_numeric(x, errors='coerce'))

    cleaned_df['reference_number'] = cleaned_df['reference_number'].apply(remove_non_ascii)
    cleaned_df['reference_number'] = cleaned_df['reference_number'].apply(remove_bom)

    # Commenting it out for now. 
    # cleaned_df = cleaned_df[~cleaned_df['reference_number'].apply(contains_non_english)]

    
    # The next two lines remove rows where the reference_number column contain strings that consit entirely 
    # of any combination of the characters in the regex pattern including empty strings.
    # whitespaces, asterisks, zero, hyphen, en dash, em dash, period, underscore 
    pattern = r'^[\s*0\-\–—\._]*$'
    cleaned_df = cleaned_df[~cleaned_df['reference_number'].str.contains(pattern, regex=True)]

    logger.info(f'{len(cleaned_df)} rows after removing rows with only non-alphanumeric characters')

    cleaned_df = cleaned_df.dropna(subset=['brand'])
    cleaned_df = cleaned_df.dropna(subset=['price'])    
    cleaned_df = cleaned_df[cleaned_df["currency"].notnull()].reset_index(drop=True)

    logger.info(f'{len(cleaned_df)} rows after removing rows with nulls in brand, price, or currency')

    cleaned_df['reference_number'] = cleaned_df['reference_number'].apply(clean_reference_number)

    # Remove the 'REF.' leading characters if present in the reference_number column
    cleaned_df['reference_number'] = cleaned_df['reference_number'].str.replace(r'^REF\.', '', regex=True)

    # Apply the new suffix removal function
    cleaned_df['reference_number'] = cleaned_df['reference_number'].apply(lambda x: remove_suffix(x, ', DOES NOT APPLY'))

    # Change 'ebay' to 'eBay' in the 'source' column
    cleaned_df['source'] = cleaned_df['source'].str.replace('ebay', 'eBay', case=False)

    # Converts null values to zero 0 and cast datatype to int32
    cleaned_df = clean_and_convert_to_int32(cleaned_df, 'year_of_production')
    cleaned_df = clean_and_convert_to_int32(cleaned_df, 'year_introduced')

    logger.info(f'{len(cleaned_df)} rows at the end of the script')

    logger.info("Data Cleaned")

    return cleaned_df

   
# def ref_num_cleaning_function_applicator(df):
#     # Create a mask for Omega watches
#     omega_mask = df['brand'] == 'Omega'
    
#     # Get unique Omega reference numbers
#     unique_omega_refs = df.loc[omega_mask, 'reference_number'].unique()
    
#     # Clean unique Omega reference numbers
#     cleaned_unique_refs = pd.Series(unique_omega_refs).apply(omega_ref_num_cleaning)
    
#     # Create a mapping dictionary
#     ref_mapping = dict(zip(unique_omega_refs, cleaned_unique_refs))
    
#     # Create a copy of the reference_number column
#     cleaned_reference = df['reference_number'].copy()
    
#     # Update Omega references using the mapping
#     cleaned_reference.loc[omega_mask] = cleaned_reference.loc[omega_mask].map(ref_mapping)
    
#     return cleaned_reference

