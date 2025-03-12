# modules/utilities.py
import pandas as pd
import logging
import os
import boto3
from datetime import datetime, timedelta
import hashlib
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session
from slugify import slugify
import re
import unicodedata
# import IPython.display
from IPython.display import display, HTML



def load_env_vars():    
    load_dotenv()
    return {
        "AWS_ACCESS_KEY_ID" : os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY" : os.getenv("AWS_SECRET_ACCESS_KEY"),
        "DB_USER" : os.getenv("DB_USER"),
        "DB_PASSWORD" : os.getenv("DB_PASSWORD"),
        "CURRENCY_API_KEY" : os.getenv('CURRENCY_API_KEY'),
        "AWS_REGION" : "us-east-2"
    }
    
def get_s3_client():
    try:
        return boto3.client('s3')  # Uses IAM role if available
    except:
        # Fallback to access keys
        env_vars = load_env_vars()
        return boto3.client(
            's3',
            aws_access_key_id=env_vars["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=env_vars["AWS_SECRET_ACCESS_KEY"],
            region_name=env_vars["AWS_REGION"]
        )

def timestamp():
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M")
    return current_datetime


def convert_date_column(df, column_name='date'):
    """
    Convert a specified column to datetime format if it's not already.
    
    Parameters:
    df (pandas.DataFrame): The DataFrame containing the date column.
    column_name (str): The name of the column to convert. Default is 'date'.
    
    Returns:
    pandas.DataFrame: The DataFrame with the converted date column.
    """
    # Get the data type of the specified column
    date_dtype = df[column_name].dtype
    
    # Check if the data type is not already datetime
    if not pd.api.types.is_datetime64_any_dtype(date_dtype):
        print(f"Converting '{column_name}' column from {date_dtype} to datetime")
        # Convert the specified column to datetime and normalize
        df[column_name] = pd.to_datetime(df[column_name]).dt.normalize()
    else:
        print(f"'{column_name}' column is already in datetime format")
    
    return df


def enforce_datatypes(df, dtype_dict):
    """
    Enforce data types on DataFrame columns based on a provided dictionary.
    Also handles date column conversion using a specialized method.
    
    Parameters:
    df (pandas.DataFrame): The input DataFrame.
    dtype_dict (dict): Dictionary of column names and their desired data types.
    
    Returns:
    pandas.DataFrame: The DataFrame with enforced data types.
    """
    # Create a copy of the DataFrame to avoid modifying the original
    df_enforced = df.copy()
    
    for column in df_enforced.columns:
        if column == 'date':
            # Use the specialized date conversion method
            df_enforced = convert_date_column(df_enforced, column)
        elif column in dtype_dict:
            try:
                df_enforced[column] = df_enforced[column].astype(dtype_dict[column])
            except ValueError as e:
                print(f"Warning: Could not convert column '{column}' to {dtype_dict[column]}. Error: {e}")
        else:
            # If the column is not in the dictionary, let pandas infer the data type
            pass  # pandas will keep the inferred data type
    
    return df_enforced


def sha256_hash(series):
    """Apply SHA-256 hashing to a pandas Series."""
    return series.apply(lambda x: hashlib.sha256(x.encode()).hexdigest())


def add_hash_id(df, columns, hash_column_name='hash_id', prefix=None):
    """
    Add a SHA-256 hash identifier to a DataFrame based on specified columns.

    Parameters:
    - df (pd.DataFrame): The DataFrame to which the hash column will be added.
    - columns (list): List of column names to be used for generating the hash.
    - hash_column_name (str): Name of the new hash column. Default is 'hash_id'.
    - prefix (str): Optional prefix to add before the hash. Default is None.

    Returns:
    pd.DataFrame: The DataFrame with the new hash column added.

    Example usage:
    df = pd.DataFrame({
        'column1': ['A', 'B', 'C'],
        'column2': ['X', 'Y', 'Z'],
        'column3': [1, 2, 3]
    })
    df = add_hash_id(df, ['column1', 'column2', 'column3'], prefix='WATCH')
    print(df)
    """
    # Concatenate the specified columns into a single string
    df['hash_input'] = df[columns].astype(str).agg('-'.join, axis=1)

    # Generate SHA-256 hash for the concatenated strings in a vectorized manner
    # df[hash_column_name] = sha256_hash(df['hash_input'])
    hashed_values = sha256_hash(df['hash_input'])

    # Add prefix if specified
    if prefix:
        df[hash_column_name] = prefix + '-' + hashed_values
    else:
        df[hash_column_name] = hashed_values

    # Drop the temporary hash_input column
    df.drop(columns=['hash_input'], inplace=True)
    
    return df



def setup_logging(module_name, log_file_name=None):
    """
    Set up logging configuration for a specific module.
    
    Parameters:
    module_name (str): The name of the module.
    log_file_name (str, optional): Name of the log file. If None, a default name with timestamp will be used.
    
    Returns:
    logging.Logger: Configured logger object for the module
    """
    if log_file_name is None:
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M")
        log_file_name = f"{module_name}_{timestamp}.log"

    # Ensure the logs directory exists
    log_directory = "logs"
    os.makedirs(log_directory, exist_ok=True)
    log_file_path = os.path.join(log_directory, log_file_name)

    # Create a logger
    logger = logging.getLogger(module_name)

    # Check if the logger already has handlers to avoid duplicate logs
    if not logger.hasHandlers():
        # Configure file handler
        file_handler = logging.FileHandler(log_file_path, mode='a')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Add a stream handler to also print logs to console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        logger.setLevel(logging.INFO)
        logger.info("Logging setup completed.")

    return logger


def custom_slugify(value):
    """
    Converts to lowercase, removes non-word characters (alphanumerics and
    underscores) and converts spaces to hyphens. Also strips leading and
    trailing whitespace.
    """
    value = unicodedata.normalize('NFKD', str(value)).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value).strip().lower()
    return re.sub(r'[-\s]+', '-', value)



def create_identifier(df, column):
    """
    Create an identifier by combining a slugified brand name with a specified column.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame
    column (str): Name of the column to combine with the brand slug
    
    Returns:
    pd.DataFrame: DataFrame with new 'brand_slug' and 'identifier' columns
    """
    # Generate brand_slug using slugify
    # df['brand_slug'] = df['brand'].apply(slugify)
    df['brand_slug'] = df['brand'].apply(custom_slugify)
    
    # Create the identifier by combining brand_slug with the specified column
    df['identifier'] = df['brand_slug'] + '-' + df[column]
    
    return df



############# Data Exploration ##################

def get_value_counts_special(df, column_name):
    """
    Calculate value counts for a specified column in a DataFrame.

    Parameters:
    df (pandas.DataFrame): The input DataFrame
    column_name (str): The name of the column to analyze

    Returns:
    pandas.DataFrame: A DataFrame with value counts, sorted in descending order
    """
    value_counts_special = (
        df[column_name].value_counts(dropna=False)
        .reset_index()        
        .sort_values('count', ascending=False)        
        )
    return value_counts_special


def get_value_counts_gold(df, column_names):
    """   

    Parameters:
    df (pandas.DataFrame): The input DataFrame
    column_name (list): A lsit of column names to include in the group by.

    Returns:
    pandas.DataFrame: A DataFrame with value counts, sorted in descending order
    """
    value_counts_gold = (
        df.groupby(column_names, dropna=False)
        .size()
        .reset_index(name='count')        
        .sort_values([column_names[0], 'count'], ascending=[True, False])        
        )
    return value_counts_gold


# from ipython.display import display, HTML

def print_html_df(df, start=None, end=None, numb_rows=10, section='head'):
    """
    Prints a dataframe in a vertically compact form while maintaining full visibility of the data 
    by allowing horizontal scrolling. 

    Parameters:
    df (pandas.DataFrame): The input DataFrame to be displayed.
    start (int, optional): The starting index for slicing the DataFrame. If provided, must be used with 'end'.
    end (int, optional): The ending index (exclusive) for slicing the DataFrame. If provided, must be used with 'start'.
    numb_rows (int, optional): The number of rows to display when using 'head' or 'tail'. Default is 10.
    section (str, optional): Determines whether to show the first ('head') or last ('tail') rows when not using start/end. 
                             Default is 'head'.

    Returns:
    None. Displays the HTML representation of the DataFrame.

    Behavior:
    - If both 'start' and 'end' are provided, it displays the specified range of rows.
    - If 'start' and 'end' are not provided, it uses 'section' to determine whether to show the first or last 'numb_rows'.
    - The output is displayed in an HTML format that allows horizontal scrolling for wide DataFrames.

    Raises:
    - Prints an error message if the provided 'start' and 'end' values are invalid for the DataFrame's size.

    Note:
    This function requires the `display` and `HTML` functions from IPython.display to work properly,
    typically used in Jupyter notebooks or similar interactive Python environments.
    """
    # Get the total number of rows in the DataFrame
    total_rows = len(df)

    # If start and end are provided, use them to slice the DataFrame
    if start is not None and end is not None:
        if 0 <= start < total_rows and start < end <= total_rows:
            df_subset = df.iloc[start:end]
        else:
            print(f"Invalid range. The DataFrame has {total_rows} rows (0 to {total_rows-1}).")
            return
    else:
        # Use head or tail based on the section parameter
        if section.lower() == 'tail':
            df_subset = df.tail(numb_rows)
        else:
            df_subset = df.head(numb_rows)

    # Convert the DataFrame subset to HTML
    html_content = df_subset.to_html()

    # Display HTML with style for horizontal scrolling
    display(HTML(f'<div style="overflow-x: auto; white-space: nowrap;">{html_content}</div>'))


def get_rows_by_null_condition(df, column_name, get_null=True):
    """
    Get rows from a DataFrame based on whether a specific column contains null values or not.

    Parameters:
    df (pandas.DataFrame): The input DataFrame
    column_name (str): The name of the column to check for null values
    get_null (bool): If True, return null rows; if False, return non-null rows (default is True)

    Returns:
    pandas.DataFrame: A DataFrame containing the requested rows
    """
    condition = (
        (df[column_name].isna()) | 
        (df[column_name] == '<NA>') |
        (df[column_name] == "")
    )

    if not get_null:
        condition = ~condition  # Invert the condition for non-null rows

    return df[condition]


def investigate_column(df, column_name):
    """
    Investigate and print detailed information about a specific column in a DataFrame.

    This function provides a comprehensive analysis of a given column, including:
    - Unique data types present in the column
    - Sample values for each data type
    - Count of occurrences for each data type
    - Count of null or NaN values

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the column to investigate.
    column_name (str): The name of the column to investigate.

    Prints:
    - Column name being investigated
    - List of unique data types in the column
    - Sample values for each unique data type (up to 5 samples)
    - Count of occurrences for each data type
    - Count of null or NaN values in the column

    Returns:
    None
   
    Example usage:
    >>> # List of columns to investigate    
    >>> columns_to_check = ['year_of_production' ]

    >>> # Investigate each column
    >>> for column in columns_to_check:
    >>>     investigate_column(wcc_wkly_price_df, column)

    Investigating column: rolling_avg_of_median_price
    Unique types: [<class 'decimal.Decimal'>]

    Sample values for <class 'decimal.Decimal'>:
    50    12892.81
    51    12892.81
    52    12892.81
    53    12892.81
    54    12892.81
    Name: rolling_avg_of_median_price, dtype: object

    Type counts:
    rolling_avg_of_median_price
    <class 'decimal.Decimal'>    597506
    Name: count, dtype: int64

    Null or NaN count: 0

    """

    print(f"\nInvestigating column: {column_name}")
    
    # Get unique types
    unique_types = df[column_name].apply(type).unique()
    print("Unique types:", unique_types)
    
    # Sample values for each type
    for dtype in unique_types:
        sample = df[df[column_name].apply(type) == dtype][column_name].head()
        print(f"\nSample values for {dtype}:")
        print(sample)
    
    # Count of each type
    type_counts = df[column_name].apply(type).value_counts()
    print("\nType counts:")
    print(type_counts)
    
    # Check for null or NaN values
    null_count = df[column_name].isnull().sum()
    print(f"\nNull or NaN count: {null_count}")

############## Airflow Related Functions ####################

def get_prior_sunday(given_date: datetime) -> datetime.date:
    """
    It's intended use is to get the date of the Sunday prior to the 
    data interval end date. The date for said Sunday is needed since the metrics 
    timeseries dataset weekly closing are on Sundays. 
    
    Args:
        current_date: datetime object representing the current date
        
    Returns:
        date object representing the prior Sunday
    
    Explanation:
        - weekday() returns a number 0-6 representing the day of the week:
          Monday = 0
          Tuesday = 1
          Wednesday = 2
          Thursday = 3
          Friday = 4
          Saturday = 5
          Sunday = 6
        - Adding 1 to weekday() gives us the number of days to subtract to reach the prior Sunday.
    """
    # Get days since last Sunday (where Monday=0 and Sunday=6)
    days_since_sunday = given_date.weekday() + 1
    # Subtract those days to get to the prior Sunday
    prior_sunday = given_date - timedelta(days=days_since_sunday)
    return prior_sunday.date()

def sanitize_group_id(text: str) -> str:
    """Sanitizes text to create a valid Airflow TaskGroup ID.
    
    Converts input text to a format suitable for Airflow TaskGroup IDs by:
    - Replacing special characters with underscores
    - Converting to lowercase
    - Ensuring compliance with Airflow naming conventions
    
    Args:
        text (str): The input text to be sanitized. Can contain any characters.
        
    Returns:
        str: A sanitized string containing only lowercase letters, numbers, 
             underscores, and hyphens.
    
    Examples:
        >>> sanitize_group_id("Brand Name (2023)")
        'brand_name_2023'
        >>> sanitize_group_id("Task.Group#1")
        'task_group_1'
    
    Note:
        Airflow TaskGroup IDs must:
        - Start with a letter or underscore
        - Contain only letters, numbers, underscores, and hyphens
        - Be unique within their DAG
    """
    import re
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', text)
    return sanitized.lower()


def split_brands_list(lst: list, chunk_size: int) -> list:
    """Splits a list into smaller chunks of specified size.
    
    Creates a list of sublists where each sublist contains at most 'chunk_size' 
    elements from the original list. Useful for batch processing and parallel 
    execution in Airflow tasks.
    
    Args:
        lst (list): The input list to be split. Can contain elements of any type.
        chunk_size (int): The maximum size of each chunk. Must be a positive integer.
        
    Returns:
        list: A list of sublists, where each sublist has at most chunk_size elements.
        
    Raises:
        ValueError: If chunk_size is less than or equal to 0.
        
    Examples:
        >>> split_brands_list(['A', 'B', 'C', 'D', 'E'], 2)
        [['A', 'B'], ['C', 'D'], ['E']]
        >>> split_brands_list([1, 2, 3], 3)
        [[1, 2, 3]]
        
    Note:
        - The last chunk may contain fewer elements than chunk_size
        - Returns an empty list if the input list is empty
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
