# modules/datalake_operations.py
import boto3
import io
from io import BytesIO
from pathlib import Path
import pandas as pd
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
from modules.utilities import get_s3_client,setup_logging
from datetime import datetime
from modules.data_cleaning import (
    handle_null_columns, 
    clean_and_standarize_year_column, 
    clean_and_convert_to_int32,
    convert_na_to_none
)



logger = setup_logging(__name__)


def convert_to_parquet_and_save_to_s3(df, date_str, output_bucket_name, output_bucket_subfolder):
    """
    Converts a Pandas DataFrame to a Parquet file and uploads it to an S3 bucket.

    This function takes a DataFrame, converts it to a Parquet file format, and uploads it to a specified 
    Amazon S3 bucket and subfolder. The file is organized in a directory structure based on the provided date.

    Parameters:
    df (pandas.DataFrame): The DataFrame to be converted and uploaded.
    date_str (str): A string representing the date in 'YYYY-MM-DD' format, used to organize the file in S3.
    output_bucket_name (str): The name of the S3 bucket where the file will be uploaded.
    output_bucket_subfolder (str): The subfolder path within the S3 bucket where the file will be stored.

    Returns:
    bool: Returns True if the upload is successful, False otherwise.

    Raises:
    Exception: If there is an error during the conversion or upload process, an exception is caught and logged.

    Example:
    date_variable = datetime.datetime(2023, 8, 6)
    date_str = date_variable.strftime('%Y-%m-%d')
    success = convert_to_parquet_and_save_to_s3(df, date_str, 'my-bucket', 'data/exports/')
    if success:
        print("Upload to S3 successful")
    else:
        print("Upload to S3 failed")
    """
    try:
        # Handle null columns first
        # To prevent error: "Expected bytes, got a 'float' object", 'Conversion failed for column caliber with type object'
        df = handle_null_columns(df)
        
        # Clean and standarize year column
        year_columns = ['year_of_production', 'year_introduced']  # Add any other year columns
        for col in year_columns:
            if col in df.columns:
                df = clean_and_standarize_year_column(df, col)        

        s3 = boto3.client('s3')
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        
        date_parts = date_str.split('-')
        file_key = f"{output_bucket_subfolder}{date_parts[0]}/{date_parts[1]}/{date_parts[2]}/All_Sources_{date_str}_WEEKLY.parquet"
        # print(f'file:{file_key}')

        s3.upload_fileobj(buffer, output_bucket_name, file_key)
        print("")
        logger.info(f'{file_key} uploaded to S3')
        print("")

        return True  # Indicate success
    
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        return False  # Indicate failure


def save_parquet_to_s3_general(df, output_bucket_name, file_name, output_bucket_subfolder):
    # Function to save DataFrame to S3 as a parquet file
    s3 = boto3.client('s3')
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    # current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M")
    # file_key = f"{output_bucket_subfolder}/{file_name}_processed_{current_datetime}_.parquet"
    file_key = f"{output_bucket_subfolder}/{file_name}.parquet"
    # print(f'file:{file_key}')

    s3.upload_fileobj(buffer, output_bucket_name, file_key)
    print(f'{file_key} uploaded to S3')


def load_parquet_data_from_s3_v2(bucket_name, bucket_subfolder, dtype_dict=None):    
    """
    Load and combine Parquet files from an S3 bucket subfolder into a single DataFrame.

    This function retrieves Parquet files from a specified S3 bucket subfolder, processes them,
    and combines them into a single DataFrame. It applies the provided data types to the columns.

    Created to process weekly parquet files in bulk.

    Args:
        bucket_subfolder (str): The subfolder within the S3 bucket to search for Parquet files.
        dtype_dict (dict, optional): A dictionary specifying column names and their desired data types.
            If None, all columns from the Parquet files will be included with their default data types. Default is None.

    Returns:
        pandas.DataFrame: A combined DataFrame containing data from all processed Parquet files.

    Raises:
        boto3.exceptions.Boto3Error: If there are issues accessing the S3 bucket.
        pandas.errors.EmptyDataError: If a Parquet file is empty or cannot be read.

    Example:
        dtype_dict = {
            'column1': 'string',
            'column2': 'int64',
            'column3': 'float64',
            # Add more columns as needed
        }
        df = load_parquet_data_from_s3('my_subfolder', dtype_dict=dtype_dict)
    """
    
    # bucket_name = 'wcc-dl'
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=bucket_subfolder)

    # Check if 'Contents' key is present in the response
    if 'Contents' not in response:
        logger.warning(f"No files found in subfolder: {bucket_subfolder}")
        return pd.DataFrame()  # Return an empty DataFrame if no files are found

    # Create a list of the files in the specified bucket that contains the word 
    # 'Weekly' in its file name
    files = [file['Key'] for file in response['Contents'] if 'weekly' in file['Key'].lower()]

    
    combined_df = pd.DataFrame()
    s3_fs = s3fs.S3FileSystem()

    for file_key in files:
        print(file_key)
        
        s3_path = f"s3://{bucket_name}/{file_key}"
        logger.info(s3_path)

       
        # First, read the parquet file metadata to get available columns
        # It reads the Parquet file metadata to get the available columns without loading the entire file.
        parquet_file = pa.parquet.ParquetFile(s3_fs.open(s3_path))
        available_columns = parquet_file.schema.names      
       
              
        # If dtype_dict is provided, filter it to only include available columns
        if dtype_dict is not None:
            usecols = [col for col in dtype_dict.keys() if col in available_columns]
        else:
            usecols = available_columns
        
        
        
        df = pd.read_parquet(
                            s3_path, 
                            filesystem=s3_fs, 
                            engine='pyarrow', 
                            columns=usecols
                            )

        # Apply the specified data types to the DataFrame
        # if dtype_dict is not None:
        #     for col in usecols:
        #         if col in dtype_dict:
        #             df[col] = df[col].astype(dtype_dict[col])

        if dtype_dict is not None:
            for col in usecols:
                if col in dtype_dict:
                    if 'int' in dtype_dict[col]:
                        df[col] = df[col].fillna(0).astype(dtype_dict[col])
                    else:
                        df[col] = df[col].astype(dtype_dict[col])

        # Add missing columns (including 'status') with NaN values
        # This ensures that all specified columns are present in the DataFrame, even if they were missing in the CSV file
         # Add missing columns with NaN values
        if dtype_dict is not None:
            for col, dtype in dtype_dict.items():
                if col not in df.columns:
                    # df[col] = np.nan
                    df[col] = pd.Series(pd.NA, dtype=pd.StringDtype())

        # for col in column_names or []:
        #     if col not in df.columns:
        #         df[col] =  np.nan

        combined_df = pd.concat([combined_df, df], ignore_index=True)

    logger.info(f'Data Subfolder {bucket_subfolder} extracted')    
    
    return(combined_df)


def load_specific_parquet_from_s3(s3_path):
    """
    Load a Parquet file from S3 into a pandas DataFrame.

    Parameters:
    s3_path (str): The full S3 path to the Parquet file, including the 's3://' prefix.

    Returns:
    pandas.DataFrame: The loaded DataFrame.
    """
    # Create an S3 file system object
    s3 = s3fs.S3FileSystem()
    
    # Read the Parquet file into a Pandas DataFrame
    df = pd.read_parquet(s3_path, engine='pyarrow')

    # Check and convert the 'date' column if necessary
    if 'date' in df.columns:
        date_dtype = df['date'].dtype
        
        if not pd.api.types.is_datetime64_any_dtype(date_dtype):
            print(f"Converting 'date' column from {date_dtype} to datetime")
            df['date'] = pd.to_datetime(df['date']).dt.normalize()
        else:
            print("'date' column is already in datetime format")
    else:
        print("Warning: 'date' column not found in the DataFrame")

    return df



def save_df_to_s3_airflow_temp(df: pd.DataFrame, dag_id: str, task_id: str, ts: str, convert_rolling_avg_col=False) -> str:
    """
    Used in Airflow workflow DAGs tasks as temporary storage to save a DataFrame to S3.
    Save DataFrame to S3 and return the path.
    Converts the rolling_avg_of_median_pricecolumn to int32 if convert_rolling_avg_col is True. 
    Default value is False.
    """
    
    # Check and clean specific columns if they exist
    if 'year_introduced' in df.columns:
        df = clean_and_convert_to_int32(df, 'year_introduced').copy()

    # Conditionally drop the 'source' column if the parameter is set to True
    if convert_rolling_avg_col and 'rolling_avg_of_median_price' in df.columns:
        df = clean_and_convert_to_int32(df, 'rolling_avg_of_median_price').copy()            

    # if 'rolling_avg_of_median_price' in df.columns:
    #     df = clean_and_convert_to_int32(df, 'rolling_avg_of_median_price').copy()
    
    if 'count' in df.columns:
        df = clean_and_convert_to_int32(df, 'count').copy()
    
    if 'median_price' in df.columns:
        df['median_price'] = pd.to_numeric(df['median_price'], errors='coerce')

    # Apply the convert_na_to_none() function to the entire DataFrame
    df = df.map(convert_na_to_none)

    buffer = BytesIO()
    try:        
        # Create S3 client
        s3_client = get_s3_client()
        
        # Prepare data        
        df.to_parquet(buffer)
        key = str(Path("airflow_temp") / dag_id / f"{task_id}_{ts}.parquet")
        # key = f"airflow_temp/{dag_id}/{task_id}_{ts}.parquet"        
        s3_path = f"s3://wcc-dl/{key}"
        
        # Upload to S3
        s3_client.put_object(
            Bucket="wcc-dl",
            Key=key,
            Body=buffer.getvalue()
        )
        
        return s3_path
        
    except Exception as e:
        logger.error(f"Failed to save DataFrame to S3: {str(e)}")
        raise
    finally:
        buffer.close()


def load_df_from_s3_airflow_temp(s3_path: str) -> pd.DataFrame:
    """
       Load DataFrame from S3 path
       Used in Airflow workflow DAGs tasks to retreive a DataFrame from airflow 
       temporary storage in S3. 
    """
    try:
        # Create S3 client
        s3_client = get_s3_client()
        # Parse S3 path
        bucket = s3_path.split('/')[2]
        key = '/'.join(s3_path.split('/')[3:])
        
        # Get object from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    except Exception as e:
        logger.error(f"Failed to load DataFrame from S3: {str(e)}")
        raise


def save_dict_of_dfs_to_s3_airflow_temp(dict_data: dict[str, pd.DataFrame], dag_id: str, task_id: str, ts: str) -> dict[str, str]:
    """
    Used in Airflow workflow DAGs tasks as temporary storage to save a dictionary to S3.
    The dictionary is a key-value pair where the key is a string and the value is a pandas DataFrame.
    Save dictionary to S3 and return the paths in a dictionary.
    """
    s3_paths = {}
    for dict_key, df in dict_data.items():
        # Create a unique path for each DataFrame
        s3_path = save_df_to_s3_airflow_temp(
            df=df, 
            dag_id=dag_id, 
            task_id=f"{task_id}_{dict_key}",
            ts=ts
        )
        s3_paths[dict_key] = s3_path

    return s3_paths


def load_dict_of_dfs_from_s3_airflow_temp(s3_paths: dict[str, str]) -> dict[str, pd.DataFrame]:
    """ 
    Load a dictionary of DataFrames from a dictionary of S3 paths.
    """
    return {
        dict_key: load_df_from_s3_airflow_temp(s3_path)
        for dict_key, s3_path in s3_paths.items()       
    }

def cleanup_airflow_temp_files(bucket_name: str, directory_prefix: str) -> None:
    """
    Delete temporary files from a specified directory in an S3 bucket.

    This function performs cleanup operations by deleting all files under a specified
    directory prefix in an S3 bucket. After deletion, it checks if the airflow_temp
    directory is empty.

    Parameters
    ----------
    bucket_name : str
        The name of the S3 bucket containing the files to be cleaned up.
    directory_prefix : str
        The directory prefix path within the bucket where files should be deleted.
        For example: "airflow_temp/dag_name/"

    Returns
    -------
    None

    Raises
    ------
    Exception
        If there are any errors during the S3 operations, the exception is logged
        and re-raised.

    Notes
    -----
    - The function uses the get_s3_client() utility to establish S3 connection
    - All deletions are logged using the logger utility
    - After deletion, it checks and logs whether the airflow_temp directory is empty

    Example
    -------
    >>> cleanup_airflow_temp_files('my-bucket', 'airflow_temp/my_dag/')
    """
    try:
        s3_client = get_s3_client()
        
        # First, delete all files in the DAG's directory
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                logger.info(f"Deleted {obj['Key']} from S3 bucket {bucket_name}")
        
        # Check if airflow_temp is empty
        airflow_temp_prefix = "airflow_temp/"
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=airflow_temp_prefix)
        
        if 'Contents' not in response:
            logger.info("airflow_temp directory is now empty")
        else:
            logger.info("airflow_temp directory still contains other DAG files")
            
    except Exception as e:
        logger.error(f"Failed to clean up S3 files: {str(e)}")
        raise