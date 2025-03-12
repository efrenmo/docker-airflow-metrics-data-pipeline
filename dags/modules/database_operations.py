# modules/database_operations.py
import psycopg2
import psycopg2.extras as extras
from psycopg2 import sql
from sqlalchemy import create_engine
from contextlib import contextmanager
from airflow.utils.context import Context   ####**** REMEMEBR TO UNCOMMENT THIS LINE ****####
from typing import Optional
import os
import pandas as pd
import time
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from io import BytesIO
import logging
from datetime import datetime
from typing import List, Tuple
import os
from typing import Dict, Any
from pathlib import Path

from modules.data_cleaning import (   
    convert_na_to_none,
    clean_and_convert_to_int32
)
from modules.data_loading import get_date_1_5_years_ago, get_nearest_sunday, get_date_x_years_ago
from modules.utilities import (   
    load_env_vars, 
    setup_logging,     
    create_identifier, 
    enforce_datatypes,
    get_prior_sunday  
)
import modules.config
from modules.config import (
    brand_to_library_dict, 
    datatype_dictionary, 
    brands_ready_for_metrics,
    metrics_configurations
)

# Context = None  #### ***** TEMPORARY LINE ***** ####

# rds_table = 'watch_listings_prices_aggregated'
# schema_name = 'public'

logger = setup_logging(__name__)


def get_db_connection():
    # Connecting with RDS Postgres using Psycopg2
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    conn = psycopg2.connect(
        dbname=DB_NAME,       
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,       
    )
    return conn


def get_db_conn_w_sqlalchemy():
    """
    pool_size and max_overflow: Control the number of connections in the pool, which can help manage resource usage.
    pool_recycle: Ensures that connections are refreshed periodically, which can help avoid "stale" connections.
    pool_pre_ping: Checks the health of a connection before using it, which can help detect and replace broken connections.
    pool_use_lifo: Uses a Last-In-First-Out strategy for the connection pool, which can help reduce the number of idle connections.
    connect_args with keepalive settings: Helps maintain the connection over longer periods of inactivity.
    """
    # Connecting with RDS Postgres using SQLAlchemy
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")   

    # Create the connection string
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Create and return the SQLAlchemy engine
    return create_engine(
        DATABASE_URL,        
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600,  # Recycle connections after 1 hour
        pool_pre_ping=True,  # Enable connection health checks
        pool_use_lifo=True,  # Use LIFO to reduce number of idle connections
        connect_args={
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5
        }
    )

@contextmanager
def get_sqlalchemy_connection():    
    """Create and manage a SQLAlchemy database connection.

    This context manager ensures proper handling of database connections by:
    - Creating a new database engine
    - Yielding the engine for database operations
    - Automatically disposing of the engine when operations are complete

    Yields
    ------
    engine : sqlalchemy.engine.Engine
        A SQLAlchemy engine instance for database operations

    Examples
    --------    
    with get_connection() as engine:
        preowned_boundaries = pd.read_sql(query_preowned, engine)
        new_boundaries = pd.read_sql(query_new, engine)

    """
    engine = get_db_conn_w_sqlalchemy()
    try:
        yield engine
    finally:
        engine.dispose()

def upload_rds_query_results_to_s3(results, bucket_name, bucket_subfolder, file_name):
    """
    Uploads a pandas DataFrame to S3 as a Parquet file.

    Args:
    df (pandas.DataFrame): DataFrame to be uploaded.
    bucket_name (str): Name of the S3 bucket.
    bucket_subfolder (str): Subfolder path in the S3 bucket.
    file_name (str): Base name for the file (without extension).

    Returns:
    str: The full S3 key of the uploaded file.
    """
        
    # Construct the full file key
    # full_file_key = f'{bucket_subfolder}/{file_name}_processed_{current_datetime}.parquet'
    full_file_key = f'{bucket_subfolder}/{file_name}.parquet'

     # Convert DataFrame to PyArrow table
    table = pa.Table.from_pandas(results)

    # Create a BytesIO object to store the Parquet data
    parquet_buffer = BytesIO()

    # Write the PyArrow table to the BytesIO object
    pq.write_table(table, parquet_buffer)

    # Get the Parquet data as bytes
    parquet_data = parquet_buffer.getvalue()

    # Create an S3 client
    s3 = boto3.client('s3')

    # Upload the Parquet data to S3
    s3.put_object(Body=parquet_data, Bucket=bucket_name, Key=full_file_key)

    logger.info(f"Query has been saved to: s3://{bucket_name}/{full_file_key}")

    return f"s3://{bucket_name}/{full_file_key}"
    # return full_file_key



# def upload_rds_query_results_to_s3(results, bucket_name, bucket_subfolder, file_name):
#     """
#     Uploads an RDS query (ex: made with psycopg2) results to S3 as a Parquet file.

#     Args:
#     results (list of tuples): Query results to be uploaded.
#     bucket_name (str): Name of the S3 bucket.
#     bucket_subfolder (str): Subfolder path in the S3 bucket.
#     file_key (str): Base name for the file (without timestamp).

#     Returns:
#     str: The full S3 key of the uploaded file.
#     """
#     # Generate current datetime string
#     current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M")
    
#     # Construct the full file key
#     # full_file_key = f'{bucket_subfolder}/{file_name}_processed_{current_datetime}.parquet'
#     full_file_key = f'{bucket_subfolder}/{file_name}.parquet'

#     # Convert results to PyArrow table
#     fields = [('date', pa.date32()), ('brand', pa.string()), ('reference_number', pa.string()), 
#               ('price', pa.float64()), ('currency', pa.string()), ('source', pa.string())]
#     arrays = [pa.array(x) for x in zip(*results)]
#     table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))

#     # Create a BytesIO object to store the Parquet data
#     parquet_buffer = BytesIO()

#     # Write the PyArrow table to the BytesIO object
#     pq.write_table(table, parquet_buffer)

#     # Get the Parquet data as bytes
#     parquet_data = parquet_buffer.getvalue()

#     # Create an S3 client
#     s3 = boto3.client('s3')

#     # Upload the Parquet data to S3
#     s3.put_object(Body=parquet_data, Bucket=bucket_name, Key=full_file_key)

#     logger.info(f"Query has been saved to: {full_file_key}")

#     # return full_file_key


def upload_dataframe_to_rds(df, schema_name, table_name, chunksize=25000, context: Context = None):
    conn = None
    cur = None
    skipped_batches = 0
    problematic_rows = []  # List to store problematic row ids
    temp_table_name = None  # Initialize temp_table_name
    try:
        conn = get_db_connection()
        conn.autocommit = False  # Disable autocommit to manually control the transaction
        cur = conn.cursor()

        column_names = df.columns.tolist()      

        # Create a temporary table

        if context: # Context parameter is passed when using this function in an Airflow DAG (specific scenarios)
            ti = context['ti']
            unique_id = f"{ti.dag_id}_{ti.run_id}_{ti.task_id}_{ti.try_number}"
            temp_table_name = f"temp_{table_name}_{unique_id}"
            logger.info(f"temp_table_name using context: {temp_table_name}")
        else:
            temp_table_name = f"temp_{table_name}"
            logger.info(f"temp_table_name without using context: {temp_table_name}")

        create_temp_table_query = sql.SQL("""
            CREATE TEMPORARY TABLE {temp_table} (
                LIKE {schema}.{table}
                INCLUDING DEFAULTS
                INCLUDING CONSTRAINTS
                EXCLUDING INDEXES        
            )
        """).format(
            temp_table= sql.Identifier(temp_table_name),
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name)
        )
        cur.execute(create_temp_table_query)

        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i+chunksize]
            values = [tuple(row) for row in chunk.values]

            try:
                # Insert into temporary table
                temp_insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.Identifier(temp_table_name),
                    sql.SQL(', ').join(map(sql.Identifier, column_names))
                )
                psycopg2.extras.execute_values(cur, temp_insert_query, values)
                logger.info(f"Inserted rows {i} to {i+len(chunk)} into temporary table")
                skipped_batches = 0  # Reset skipped_batches on successful insert
            except Exception as e:
                logger.error(f"Chunk insert failed for rows {i} to {i+len(chunk)}: {e}")

                if 'hash_id' in chunk.columns:
                    problematic_rows.extend(chunk['hash_id'].tolist())  # Log problematic row ids

                skipped_batches += 1  # Increment the skipped batch count

                # Abort if more than 2 consecutive batches are skipped
                if skipped_batches >= 2:
                    logger.error("Aborting upload operation due to too many skipped batches.")
                    conn.rollback()
                    return False

                continue  # Skip this batch and continue to the next

        # Log problematic rows for further inspection
        if problematic_rows:
            logger.error(f"Problematic rows with hash_id: {problematic_rows}")

        # Move data from temporary table to actual table
        offset = 0
        batch_size = 50000  # Adjust as needed
        total_rows_moved = 0

        while True:
            move_batch_query = sql.SQL("""
                INSERT INTO {}.{}
                SELECT * FROM {}                
                LIMIT {} OFFSET {}
            """).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.Identifier(temp_table_name),
                sql.Literal(batch_size),
                sql.Literal(offset)
            )
            cur.execute(move_batch_query)
            rows_affected = cur.rowcount
            
            if rows_affected == 0:
                logger.info(f"Data transfer complete. Total rows moved: {total_rows_moved}")
                break
            
            conn.commit()
            total_rows_moved += rows_affected
            offset += batch_size
            
            logger.info(f"Batch inserted: {rows_affected} rows. Total rows moved so far: {total_rows_moved}")

        logger.info("Data transfer process finished.")

        # Commit the transaction
        conn.commit()
        logger.info("Data upload successful. Transaction committed.")

        return True
    except Exception as error:
        logger.error(f"Error: {error}")
        if conn:
            conn.rollback()
            logger.info("Transaction rolled back due to error.")
        return False
    finally:
        if cur:
            # Cleanup: Drop the temporary table if it exists
            if temp_table_name: # Only attempt to drop if temp_table_name was set
                try:
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
                        sql.Identifier(temp_table_name)
                    ))
                except Exception as e:
                    logger.error(f"Error dropping temporary table: {e}")
            cur.close()
        if conn:
            conn.close()


def upload_formatted_df_to_rds(
    df: pd.DataFrame, 
    schema_name: str, 
    table_name: str,     
    chunksize: int = 25000, 
    drop_source: bool =False, 
    context: Optional[Context] = None
):
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
        success = upload_dataframe_to_rds(df, schema_name, table_name, chunksize, context)
        
        if not success:
            raise Exception("Failed to load table to RDS")
            
        logger.info(f"Table loaded to RDS successfully")
            
    except Exception as e:
        logger.error(f"Error loading data to RDS: {str(e)}")
        raise

############## price history brand ini ######################


def get_pre_wcc_data_enriched(brands: List[str]) -> List[Tuple]:
    """
    Retrieve watch listings from the database based on brand and date criteria.

    Args:
    brands (List[str]): List of watch brands to query.
    cutoff_date (datetime.date): The date before which to retrieve listings.
    Example: cutoff_date = datetime(2024, 1, 28).date()

    Returns:
    List[Tuple]: A list of tuples containing the query results.
    """
    cutoff_date = datetime(2024, 1, 28).date() # Date Watchcharts and contractors data ends

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql_query = """
            SELECT
                pa.date,
                pa.brand,
                pa.reference_number,    
                pa.price,
                pa.currency,
                pa.source
            FROM public.watch_listings_prices_aggregated pa
            WHERE pa.brand IN %s
                AND pa.date < %s
            """
            cursor.execute(sql_query, (tuple(brands), cutoff_date))
            results = cursor.fetchall()
        return results
    finally:
        conn.close()



from pathlib import Path
import os


def load_sql_query(filename: str) -> str:
    """
    Function developed for the metrics airflow pipeline workflow.

    Load SQL query from a file in the sql/ directory.

    This function works in both regular Python scripts (DAG files) and Jupyter notebooks.
    It automatically detects the execution environment and navigates to the sql/ directory
    accordingly.

    Parameters
    ----------
    filename : str
        Name of the SQL file to load (e.g., 'latest_week_listings.sql')

    Returns
    -------
    str
        Contents of the SQL file as a string

    Raises
    ------
    FileNotFoundError
        If the specified SQL file doesn't exist in the sql/ directory

    Notes
    -----
    The function expects a specific directory structure:
    - When called from a DAG file: navigates up three levels to find sql/
    - When called from a Jupyter notebook: navigates up three levels from current directory

    Example
    -------
    >>> query = load_sql_query('latest_week_listings.sql')
    >>> print(query)
    SELECT *
    FROM table
    WHERE date = '2024-01-01'
    """
    try:
        # For Python scripts (DAG files)
        current_file = Path(__file__)
        logger.info(f"Current file path: {current_file}")
        sql_dir = current_file.parents[1] / 'sql'
        logger.info(f"SQL directory path: {sql_dir}")
    except NameError:
        # For Jupyter notebooks in development workbooks dir when testing
        current_dir = Path.cwd()
        sql_dir = current_dir.parents[2] / 'dags' / 'sql'
    
    sql_path = sql_dir / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
        
    with open(sql_path, 'r') as f:
        return f.read()


def execute_sql_query(conn, query: str, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Execute a parameterized SQL query and return results as a DataFrame.

    Parameters
    ----------
    conn : psycopg2.extensions.connection
        Active database connection object
    query : str
        SQL query string with named parameters (e.g., %(param_name)s)
    params : Dict[str, Any]
        Dictionary of parameter names and values to be used in the query

    Returns
    -------
    pd.DataFrame
        Query results as a pandas DataFrame, with columns matching the query output

    Raises
    ------
    psycopg2.Error
        If there's an error executing the query
    
    Notes
    -----
    Uses RealDictCursor to preserve column names in the resulting DataFrame
    
    """
    
    with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()
    return pd.DataFrame(results)


def log_dataframe_statistics(df: pd.DataFrame, latest_week_date: datetime) -> None:
    """
    This function is used to log the statistics of the dataframe.
    It is used to log the number of listings and the number of unique brands in the dataframe.
    It is used to log the brands included in the dataframe.
    """
    unique_brands = df['brand'].unique().tolist()
    num_unique = df['brand'].nunique()
    logger.info(f'Data for the week ending on {latest_week_date} has {len(df)} listings')
    logger.info(f"{num_unique} unique brands in the dataframe")
    logger.info(f"Brands included: {unique_brands} ")


def load_latest_week_listings(brands_list: list[str], metrics_config_key: str, data_interval_end_date: datetime) -> pd.DataFrame:
    """
    Load and process the latest week's listings from RDS PostgreSQL database for specified brands.

    Parameters
    ----------
    brands_list : list[str]
        List of brand names to filter the watch listings
    data_interval_end_date : datetime
        End date of the data interval, used to calculate the latest week's date

    Returns
    -------
    pd.DataFrame
        Processed DataFrame containing latest week's listings

    Notes
    -----
    - Connects to database using get_db_connection()
    - Calculates prior Sunday from data_interval_end_date
    - Applies data type enforcement using datatype_dictionary()
    - Logs processing statistics including number of listings and unique brands

    Raises
    ------
    psycopg2.Error
        For database connection or query execution errors
    """
    latest_week_date = get_prior_sunday(data_interval_end_date) # Airflow's DAG run data_interval_end date
    logger.info(f"Latest Week Date: {latest_week_date}")

    # Get configurations
    CONFIGURATIONS = metrics_configurations()
    # Load and execute query
    sql_query_file = CONFIGURATIONS[metrics_config_key]['sql_query']['latest_week_listings']    
    schema_name = CONFIGURATIONS[metrics_config_key]['database']['data_source']['schema']
    table_name = CONFIGURATIONS[metrics_config_key]['database']['data_source']['no_outliers_tbl']

    logger.info(f"SQL Query File used: {sql_query_file} for latest week listings")
    logger.info(f"Schema Name: {schema_name}")
    logger.info(f"Table Name: {table_name}")  

    latest_week_listings_query_template = load_sql_query(sql_query_file)
    LATEST_WEEK_LISTINGS_QUERY = latest_week_listings_query_template.format(schema_name=schema_name, table_name=table_name) 

    params = {
        'brands': tuple(brands_list),
        'latest_week_date': latest_week_date
    }
    
    conn = get_db_connection() 
    try:    
        # Execute the query and get data
        latest_week_listings = execute_sql_query(conn, LATEST_WEEK_LISTINGS_QUERY, params)       

        # Apply data transformations
        dtype_dict = datatype_dictionary()
        latest_week_listings = enforce_datatypes(latest_week_listings, dtype_dict)        
        # active_listings = active_listings.sort_values(['brand', 'reference_number', 'date'],ascending=True).reset_index(drop=True)        
        
        # Log statistics
        log_dataframe_statistics(latest_week_listings, latest_week_date)
        
        return latest_week_listings

    finally:
        conn.close()


def load_historical_data(brands_list, 
                         metrics_config_key: str, 
                         condition: str, 
                         year_multiplier=1.5, 
                         sql_query_file: str = None,
                         schema_name: str = None,
                         table_name: str = None) -> pd.DataFrame:
    """
    Load historical data from RDS table specified in the query.
    The query can be found in the dags/sql/ directory.

        For the purpose of calculating weekly prices and stats, We need to query
    historical data (from atleast 1.5 yrs ago) and combine it with the most recent 
    weeks data to to process and calculate the necessary metrics. For other use cases, 
    we are free to use a different year_multiplier value.

    NOTE:
    ------
        As of feb 14 2025, the function is evolving to support wider array of use cases.
    For this reason, the function now supports schema_name, table_name, and sql_query_file parameters,
    while still supporting the default behavior when schema_name, table_name, and sql_query_file are not provided.

        We should look into making it mandatory to provide values for schema and table_name parameters.
    We should also ammend tasks that were put in place prior to this date to ensure they are using the new
    function parameters exclusively.    
    
    Parameters:
        - brands_list (list): List of brand names to query
        - year_multiplier (float): Multiplier to calculate the amout 
          of historic data. Ex: 1.5 years ago (default value)
        - schema_name (str): Name of the schema to query
        - table_name (str): Name of the table to query
        - sql_query_file (str): Full name of the sql query file to query
    Returns:
        pd.DataFrame: DataFrame containing historical data with proper datatypes 
    """

    # Get configurations
    CONFIGURATIONS = metrics_configurations() 
    if schema_name:
        schema_name = schema_name
    else:
        schema_name = CONFIGURATIONS[metrics_config_key]['database']['production']['schema']
    
    if table_name:
        table_name = table_name
    else:
        table_name = CONFIGURATIONS[metrics_config_key]['database']['production']['price_hist_and_stats_tbl']['condition'][condition]
    
    if sql_query_file:
        sql_query_file = sql_query_file
    else:
        sql_query_file = CONFIGURATIONS[metrics_config_key]['sql_query']['price_hist_last_1_dot_5_yrs_ago']

    # Load and format the SQL query
    max_date_query_template = load_sql_query('get_max_date_for_load_historical_data.sql')
    MAX_DATE_QUERY = max_date_query_template.format(schema_name=schema_name, table_name=table_name)
    logger.info(f"MAX_DATE_QUERY: {MAX_DATE_QUERY}")

    # Load and format the SQL query
    historical_data_query_template = load_sql_query(sql_query_file)
    HISTORICAL_DATA_QUERY = historical_data_query_template.format(schema_name=schema_name, table_name=table_name) 
    logger.info(f"HISTORICAL_DATA_QUERY: {HISTORICAL_DATA_QUERY}")
    conn = get_db_connection()    
    try:
        # Execute the max date query
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            params_0 = {                
                'brands': tuple(brands_list)
            }
            logger.info(f"params_0: {params_0}")

            cursor.execute(MAX_DATE_QUERY, params_0)
            # cursor.execute(MAX_DATE_QUERY, {'brand': brands_list[0]})  # Taking the first brand from brands_list parameter to get the max date
            max_date_result = cursor.fetchone()
            max_date = max_date_result['max_date']

            logger.info(f"The max_date (most recent date) in the retreived historical data: {max_date}")

            # max_date = datetime(2024, 4, 7).date() # Temporary line

            # Calculate the date 1.5 years ago and adjust to the nearest Sunday 
            date_x_years_ago = get_date_x_years_ago(max_date, year_multiplier=year_multiplier)
            adjusted_start_date = get_nearest_sunday(date_x_years_ago)
            logger.info(f"The date {year_multiplier} years ago (adjusted_start_date) for the historical data: {adjusted_start_date}")
                    
            params = {
                'start_date': adjusted_start_date,
                'end_date': max_date,
                'brands': tuple(brands_list)
            }
            logger.info(f"params: {params}")
            # with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute( HISTORICAL_DATA_QUERY, params )
            result = cursor.fetchall()

        # Create DataFrame
        df = pd.DataFrame(result)
        dtype_dict = datatype_dictionary()
        df = enforce_datatypes(df, dtype_dict)

        # log statistics
        # Checking the actual date range of our dataframe to be compared with max_date and adjusted_start_date
        actual_max_date = df["date"].max().date()
        actual_min_date =  df["date"].min().date()

        unique_brands = df['brand'].unique().tolist()
        num_unique = df['brand'].nunique()
        logger.info(f'Historical data has {len(df)} rows')
        logger.info(f'Historical dataframe ranges from {actual_min_date} to {actual_max_date}')
        logger.info(f"{num_unique} unique brands in the dataframe")
        logger.info(f"Brands included: {unique_brands} ")
              

        return df

    finally:
        conn.close()


def combine_new_and_historical_data(new_weekly_data: pd.DataFrame, 
                                    brands_list: list[str], 
                                    metrics_config_key: str, 
                                    condition: str, 
                                    year_multiplier: float) -> pd.DataFrame:
    """Combine new weekly resampled data with historical weekly data (stored in RDS) and calculate metrics"""
    historical_data = load_historical_data(brands_list, metrics_config_key, condition, year_multiplier)
   
    # Concatenating historical data with this weeks data
    combined_data = pd.concat([historical_data, new_weekly_data], ignore_index=True, sort=False)

    # combined_data = combined_data.sort_values(['brand', 'reference_number', 'date'], ascending=[True, True, True]).reset_index(drop=True)
    max_date = combined_data["date"].max().date()
    min_date =  combined_data["date"].min().date()
    logger.info(
        f"There are {len(combined_data)} rows after combining historical weekly data and the new week's weekly data \n"
        f"the earliest date in the combined_df is {min_date}, and latest day is {max_date} \n"
    )   
    
    return combined_data