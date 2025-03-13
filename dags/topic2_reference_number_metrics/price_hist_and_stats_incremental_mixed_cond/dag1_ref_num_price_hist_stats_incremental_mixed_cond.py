# Production Dag

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
from pathlib import Path
import boto3
import sys 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# sys.path.append('/home/ubuntu/airflow-metrics-workflow/dags/')

from modules.utilities import load_env_vars
from modules.data_cleaning import (    
    convert_na_to_none, 
    clean_and_convert_to_int32)
from topic2_reference_number_metrics.price_hist_and_stats_incremental_mixed_cond.util_ref_num_price_hist_and_stats_incremental_mixed_cond import (
    load_new_listings,
    enrich_data,
    load_data_to_rds_tbl,
    remove_outliers,
    resampling_by_weekly_frequency,
    combine_new_and_historical_data,
    extend_and_forward_fill,
    calculate_rolling_average,
    calculate_dollar_and_pct_change
) 
from modules.utilities import setup_logging, get_s3_client
from modules.config import brands_ready_for_metrics

logger = setup_logging(__name__)

def save_df_to_s3_airflow_temp(df: pd.DataFrame, dag_id: str, task_id: str, ts: str, convert_rolling_avg_col=False) -> str:
    """
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
    """Load DataFrame from S3 path"""
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

def sanitize_group_id(text):
    """Convert text to valid TaskGroup ID format."""
    # Replace spaces and dots with underscores
    # Remove special characters
    # Convert to lowercase for consistency
    import re
    sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', text)
    return sanitized.lower()

def split_brands_list(lst, chunk_size):
    """Split a list into chunks of specified size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'wcc',
    'depends_on_past': True,
    #'wait_for_downstream': True, # Waits for the entire previous DAG (all tasks) run to complete successfully
    # Data interval start date for the week you want to start processing. 
    # Which is one week before first desired run
    'start_date': datetime(2024, 11, 20), # Same as Data interval start date 
    # 'end_date': datetime(2025, 2, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
@dag(
    'ref_num_price_hist_and_stats_mixed',    
    default_args=default_args,
    description='A DAG to process reference number mixed condition (New, and Pre-Owned) metrics',    
    # schedule='0 0 * * 3', # Run weekly on Wednesday at midnight
    schedule=None,
    catchup=False,
    max_active_runs=1,    
    tags=['metrics', 'reference_number', 'mixed_condition']
)

def ref_num_price_hist_and_stats_mixed():
    # Define constants
    brands_list = brands_ready_for_metrics()
    chunk_size = 2 # Adjust as needed
    list_of_chunks = split_brands_list(brands_list, chunk_size)
    stage_schema_name = 'public'
    stage_table_1_name = 'metrics_wcc_data_enriched'
    stage_table_2_name = 'metrics_wcc_data_enriched_rm_outl'
    stage_table_3_name = 'metrics_wcc_wkl_resampled_enriched'
    prod_schema_name = 'metrics'
    prod_table_1_name = 'wcc_wkl_prices_and_stats_mixed'  
    
    # Task 1: Load New Weekly Listings
    @task(
        task_id='load_new_weekly_listings',
        pool='ref_num_price_hist_and_stats_mixed_pool',        
        retry_delay=timedelta(minutes=5),
        retries=1,
        doc_md="""
        ### Load New Weekly Listings
        
        Loads the latest week's watch listings for brands that are ready for metrics calculation.
        
        **Input:**
        - chunks_of_brands: List of brand names to process. A subset of the full list of brands (brands_list).
        
        **Output:**
        - Saves DataFrame to S3 temporary location and returns the path
        
        **Process:**
        1. Queries database for new listings
        2. Applies data type enforcement
        3. Filters out zero-price listings
        4. Sorts by brand, reference number, and date
        5. Saves to S3
        
        **Dependencies:**
        - Requires database connection
        - Uses S3 storage
        """       
    )
    def task_load_new_listings(chunk, chunk_identifier, **context):
        try:
            logger.info(f"Processing chunk: {chunk}")

            data_interval_end_date = context['data_interval_end']
            # data_interval_end_date = datetime(2025, 1, 29) # When need to set the date manually

            logger.info(f"Data interval start date: {data_interval_end_date}")

            # chunk is a list of a handful of brands.
            # Query data from public.metrics_watch_listings_phase_0 rds table directly related to the brands that are part of the chunk.
            df = load_new_listings(chunk, data_interval_end_date)
            # Saves df to the airflow temp directory in S3 and returns the path
            s3_path = save_df_to_s3_airflow_temp(df,
                                                context['dag'].dag_id, # dag_id
                                                f"{context['task'].task_id}_{chunk_identifier}", # task_id
                                                context['ts'] # ts
            )
            logger.info(f"Successfully processed chunk: {chunk}")
            return s3_path
        except Exception as e:
            logger.error(f"Failed to process chunk {chunk}: {str(e)}")
            raise
    
    # Task 2: Enrich Data
    @task(task_id='enrich_data',
          pool='ref_num_price_hist_and_stats_mixed_pool', 
    )
    def task_enrich_data(s3_path, chunk_identifier, **context):
        df = load_df_from_s3_airflow_temp(s3_path)
        enriched_df = enrich_data(df)
        s3_path = save_df_to_s3_airflow_temp(enriched_df,
                                             context['dag'].dag_id,
                                             f"{context['task'].task_id}_{chunk_identifier}",
                                             context['ts']
    )
        return s3_path
    
    # Task 3: Load Enriched Data to RDS
    @task(
        task_id='load_enriched_data_to_rds',        
        doc_md="""
        ### Load Enriched Data to RDS 
        Loads enriched data to the public.metrics_wcc_data_enriched RDS staging table before removing outliers.
        """
    )
    def task_load_enriched_data_to_rds(s3_path, stage_schema_name, stage_table_1_name):
        df = load_df_from_s3_airflow_temp(s3_path)
        load_data_to_rds_tbl(df, stage_schema_name, stage_table_1_name, False)         
    
    # Task 4: Remove Outliers
    @task(task_id='remove_outliers',
          pool='ref_num_price_hist_and_stats_mixed_pool', 
    )
    def task_remove_outliers(s3_path, chunk, chunk_identifier, **context):
        df = load_df_from_s3_airflow_temp(s3_path)
        filtered_df = remove_outliers(df, chunk)
        s3_path = save_df_to_s3_airflow_temp(filtered_df, 
                                             context['dag'].dag_id,
                                             f"{context['task'].task_id}_{chunk_identifier}",                                             
                                             context['ts']
    )
        return s3_path

    # Task 5: Load Filtered Data to RDS        
    @task(task_id='load_filtered_data_to_rds',
        doc_md="""
        ### Load filtered Data to RDS 
        This is the enriched data after having removed outliers that is being saved to the 
        public.metrics_wcc_data_enriched_rm_outl RDS staging table.
        """
    )
    def task_load_filtered_data_to_rds(s3_path, stage_schema_name, stage_table_2_name):
        df = load_df_from_s3_airflow_temp(s3_path)
        load_data_to_rds_tbl(df, stage_schema_name, stage_table_2_name, False)        
    
    # Task 6: Weekly Resample Aggregation
    @task(task_id='weekly_resample_aggregation',
          pool='ref_num_price_hist_and_stats_mixed_pool',
          doc_md="""
          ### Weekly Resample Aggregation
          Resamples the data by week and aggregates the metrics using outlier-free data.
          """          
    )
    def task_weekly_resample_aggregation(s3_path, chunk_identifier, **context):
        df = load_df_from_s3_airflow_temp(s3_path)
        wkl_df = resampling_by_weekly_frequency(df)
        s3_path = save_df_to_s3_airflow_temp(wkl_df, 
                                             context['dag'].dag_id,
                                             f"{context['task'].task_id}_{chunk_identifier}",                                             
                                             context['ts']
    )
        return s3_path
    
    # Task 7: Load Weekly Aggregation to RDS
    @task(task_id='load_weekly_aggregation_to_rds',)
    def task_load_weekly_aggregation_to_rds(s3_path, stage_schema_name, stage_table_3_name):
        df = load_df_from_s3_airflow_temp(s3_path)
        load_data_to_rds_tbl(df, stage_schema_name, stage_table_3_name, False)
    
    # Task 8: Combine New and Historical Data
    @task(task_id='combine_new_and_historical_data',
          pool='ref_num_price_hist_and_stats_mixed_pool', 
          doc_md="""
            ### Combine New and Historical Data
            Combines the new weekly resampled data with historical weekly data (stored in RDS) and calculate metrics.
            """
    )
    def task_combine_new_and_historical_data(s3_path, chunk, chunk_identifier, **context):
        df = load_df_from_s3_airflow_temp(s3_path)
        combined_data = combine_new_and_historical_data(df, chunk)
        s3_path = save_df_to_s3_airflow_temp(combined_data,
                                             context['dag'].dag_id,
                                             f"{context['task'].task_id}_{chunk_identifier}",                                             
                                             context['ts'],
                                             convert_rolling_avg_col=False
    )
        return s3_path
    
    # Task 9: Extend and Forward Fill
    @task(task_id='extend_and_ffill',
          pool='ref_num_price_hist_and_stats_mixed_pool', 
          doc_md="""
            ### Extend and Forward Fill
            Extends the data to the latest date in the dataframe and forward fills missing values
            for each brand and reference number combination.
            """
    )
    def task_extend_and_ffill(s3_path, chunk_identifier, **context):
        df = load_df_from_s3_airflow_temp(s3_path)
        extended_and_filled_df = extend_and_forward_fill(df)
        s3_path = save_df_to_s3_airflow_temp(extended_and_filled_df, 
                                             context['dag'].dag_id, 
                                             f"{context['task'].task_id}_{chunk_identifier}", 
                                             context['ts']
    )
        return s3_path
    
    # Task 10: Calculate Rolling Average
    @task(task_id='calculate_rolling_avg',
          pool='ref_num_price_hist_and_stats_mixed_pool', 
    )
    def task_calculate_rolling_avg(s3_path, chunk_identifier, **context):
        df = load_df_from_s3_airflow_temp(s3_path)
        rolling_avg_df = calculate_rolling_average(df)
        s3_path = save_df_to_s3_airflow_temp(rolling_avg_df, 
                                             context['dag'].dag_id, 
                                             f"{context['task'].task_id}_{chunk_identifier}", 
                                             context['ts'],
                                             convert_rolling_avg_col=True
    )
        return s3_path
    
    # Task 11: Calculate Dollar and Percentage Change
    @task(task_id='calculate_dollar_and_pct_change',
          pool='ref_num_price_hist_and_stats_mixed_pool', 
    )
    def task_calculate_dollar_and_pct_change(s3_path, chunk_identifier, **context):
        df = load_df_from_s3_airflow_temp(s3_path)
        final_df = calculate_dollar_and_pct_change(df)
        s3_path = save_df_to_s3_airflow_temp(final_df, 
                                             context['dag'].dag_id, 
                                             f"{context['task'].task_id}_{chunk_identifier}",  
                                             context['ts']
    )
        return s3_path        
    
    # Task 12: Save Final Data
    @task(task_id='save_final_data')
    def task_save_final_data(s3_path, prod_schema_name, prod_table_1_name):
        df = load_df_from_s3_airflow_temp(s3_path)
        load_data_to_rds_tbl(df, prod_schema_name, prod_table_1_name, True)
     
    
    # Task 13: Cleanup Temporary S3 Files
    @task(task_id='cleanup_airflow_temp',
    trigger_rule='all_success',
        doc_md="""
        ### Cleanup Temporary S3 Files
        Removes all temporary files created during DAG execution from the S3 airflow_temp folder.
        Only executes if all upstream tasks complete successfully.
        """
    )
    def task_cleanup_temp_files(**context):
        bucket_name = 'wcc-dl'
        dag_prefix = f"airflow_temp/{context['dag'].dag_id}/"
        
        try:
            s3_client = get_s3_client()
            
            # First, delete all files in the DAG's directory
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=dag_prefix)
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

    # DAG Execution
            
    # A list that stores Airflow TaskGroup objects where each TaskGroup contains multiple tasks
    # These TaskGroup objects can then be used to set dependencies using operators like >>
    chunk_groups = []

    # Process each chunk independently through the entire pipeline
    for chunk in list_of_chunks:
        chunk_identifier = '-'.join(chunk)
        # Sanitize the group_id
        group_id = f"process_chunk_{sanitize_group_id(chunk_identifier)}"

        with TaskGroup(group_id=group_id) as chunk_group:
            # Define task dependencies
            new_listings_path = task_load_new_listings(chunk, chunk_identifier)
            enriched_data_path = task_enrich_data(new_listings_path, chunk_identifier)

            # Split into parallel paths after enrichment
            # Path 1: Save to RDS
            task_load_enriched_data_to_rds(enriched_data_path, stage_schema_name, stage_table_1_name)
            # Path 2: Continue processing
            filtered_data_path = task_remove_outliers(enriched_data_path, chunk, chunk_identifier)

            # Split into parallel paths after removing outliers
            # Path 1: Save to RDS
            task_load_filtered_data_to_rds(filtered_data_path, stage_schema_name, stage_table_2_name)
            # Path 2: Continue processing
            weekly_agg_path = task_weekly_resample_aggregation(filtered_data_path, chunk_identifier)

            # Split into parallel paths after weekly resampling aggregation
            # Path 1: Save to RDS
            task_load_weekly_aggregation_to_rds(weekly_agg_path, stage_schema_name, stage_table_3_name)
            # Path 2: Continue processing
            combined_data_path = task_combine_new_and_historical_data(weekly_agg_path, chunk, chunk_identifier)
            extended_data_path = task_extend_and_ffill(combined_data_path, chunk_identifier)
            rolling_avg_path = task_calculate_rolling_avg(extended_data_path, chunk_identifier)
            final_changes_path = task_calculate_dollar_and_pct_change(rolling_avg_path, chunk_identifier)
            # Save to RDS
            task_save_final_data(final_changes_path, prod_schema_name, prod_table_1_name)

            # Append the Airflow TaskGroup object to chunk_groups (list) 
            chunk_groups.append(chunk_group)
    
    # Cleanup group
    # Clean-up is executed once all tasks and chunks have been processed
    with TaskGroup(group_id="cleanup_group") as cleanup_group:
        task_cleanup_temp_files()

    # Add TriggerDagRunOperator
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_specific_mdl_price_hist_and_stats_incremental_mixed_cond_dag',
        trigger_dag_id='specific_mdl_price_hist_and_stats_incremental_mixed',
        wait_for_completion=False,
    )
    
    # Set dependencies between groups
    chunk_groups >> cleanup_group >> trigger_next_dag

dag = ref_num_price_hist_and_stats_mixed()


