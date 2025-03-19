from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO
from pathlib import Path
import boto3
import sys 

from modules.utilities import (
    add_hash_id, 
    load_env_vars, 
    setup_logging, 
    get_s3_client,      
    create_identifier,
    custom_slugify,
    sanitize_group_id,
    split_brands_list    
)
from modules.data_cleaning import (    
    convert_na_to_none, 
    clean_and_convert_to_int32)

from modules.datalake_operations import (
    save_df_to_s3_airflow_temp,
    load_df_from_s3_airflow_temp,
    save_dict_of_dfs_to_s3_airflow_temp,
    load_dict_of_dfs_from_s3_airflow_temp,
    cleanup_airflow_temp_files
)
from modules.database_operations import (
    load_latest_week_listings,    
    combine_new_and_historical_data,
    upload_formatted_df_to_rds,
    load_historical_data
)
from modules.data_aggregation import (
    apply_incremental_calc_rolling_avg, 
    apply_dollar_and_pct_change_calc,
    apply_extend_and_forward_fill
)
from modules.advanced_financial_metrics import (
    analyze_listing_durations    
)
from modules.utilities import setup_logging, get_s3_client
from modules.config import brands_ready_for_metrics, metrics_configurations


logger = setup_logging(__name__)

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'wcc',
    'depends_on_past': True,
    'wait_for_downstream': True, # Waits for the entire previous DAG run (all tasks) to complete successfully    
    # Data interval start date for the week you want to start processing. 
    # Which is one week before first desired run
    'start_date': datetime(2024, 9, 11), # Same as Data interval start date 
    # 'end_date': datetime(2025, 1, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
@dag(
    'listings_numb_weeks_on_market',    
    default_args=default_args,
    description='A DAG to calculate the listings weeks on market for each brand',    
    # schedule='0 0 * * 3', # Run weekly on Wednesday at midnight
    schedule=None,
    catchup=False,
    max_active_runs=1,    
    max_consecutive_failed_dag_runs=1, 
    tags=['metrics','liquidity', 'weeks_on_market']
)
def listings_numb_weeks_on_market_dag():
    # varibles definitions
    brands_list = brands_ready_for_metrics()
    chunk_size = 2 # Adjust as needed
    list_of_chunks = split_brands_list(brands_list, chunk_size)
    condition = 'mixed'
    metrics_config_key = 'ref_num_metrics_config_key'    

    @task.branch
    def check_biweekly_execution(**kwargs):
        """Branch execution based on the ISO week number."""
        execution_date = kwargs['execution_date']  # Retrieve execution_date
        week_number = execution_date.isocalendar()[1]  # Get ISO week number
        print(f"Execution Date: {execution_date}, Week Number: {week_number}")

        if week_number % 2 == 0:
            return [task.task_id for task in chunk_groups]  # Execute chunk tasks
        else:
            return 'trigger_stop_self_ec2_dag'  # Skip execution if odd week

    # Task 1: Get the required historical listings data to calculate the number of weeks a listing remains on the market
    @task(
        task_id='get_hist_listings_from_db',
        pool='ref_num_listings_weeks_on_market_pool',
        retry_delay=timedelta(minutes=5),
        retries=1,
        depends_on_past=True,
        wait_for_downstream=True,
        email_on_failure=False,        
    )
    def get_hist_listings_from_db(chunk: list[str], 
                                chunk_identifier: str, 
                                metrics_config_key: str,
                                condition: str,                               
                                **context) -> str:
        
        # Load the sql query file name
        # Get configurations
        CONFIGURATIONS = metrics_configurations()  
        sql_query_file = CONFIGURATIONS[metrics_config_key]['sql_query']['weeks_on_market_required_historic_listings']
        schema_name = CONFIGURATIONS[metrics_config_key]['database']['data_source']['schema']
        table_name = CONFIGURATIONS[metrics_config_key]['database']['data_source']['no_outliers_tbl']
        year_multiplier = 1 # Will search up to 1-year of price history but will return as much as it is available. 

        try:
            logger.info(f"Processing chunk {chunk}")         
            df = load_historical_data(brands_list = chunk, 
                                    metrics_config_key = metrics_config_key, 
                                    condition = condition,                                   
                                    sql_query_file = sql_query_file,
                                    year_multiplier = year_multiplier,
                                    schema_name = schema_name,
                                    table_name = table_name
            )
            # Save the df to s3
            s3_path = save_df_to_s3_airflow_temp(df,
                                                context['dag'].dag_id,
                                                f"{context['task'].task_id}_{chunk_identifier}",
                                                context['ts']
            )
            logger.info(f"Successfully processed chunk: {chunk}")

           
            return s3_path
        
        except Exception as e:
            logger.error(f"""Error in get_required_price_history() \n
                         Failed to process chunk {chunk}: {str(e)}""")
            raise 
    
    # Task 2: Analyzed listings duration
    @task(
        task_id='analyze_listings_duration',
        pool='ref_num_listings_weeks_on_market_pool',
        retry_delay=timedelta(minutes=5),
        retries=1
    )
    def task_analyze_listings_duration(s3_path: str, chunk_identifier: str, **context) -> str:
        
        logger.info(f"Analyzing listings durations from {s3_path}")
        df = load_df_from_s3_airflow_temp(s3_path)
        listings_weeks_on_market = analyze_listing_durations(df)
        s3_path = save_df_to_s3_airflow_temp(listings_weeks_on_market,
                                            context['dag'].dag_id,
                                            f"{context['task'].task_id}_{chunk_identifier}",
                                            context['ts']
        )
        logger.info(f"Successfully analyzed listings durations from {s3_path}")
        return s3_path  
    
    # Task 3: Upload the listings weeks on market to RDS
    @task(
        task_id='upload_listings_weeks_on_market_to_rds',
        pool='ref_num_listings_weeks_on_market_pool',
        retry_delay=timedelta(minutes=5),
        retries=1
    )
    def task_upload_listings_weeks_on_market_to_rds(s3_path: str, metrics_config_key: str, condition: str, **context) -> None:

        CONFIGURATIONS = metrics_configurations()
        schema_name = CONFIGURATIONS[metrics_config_key]['database']['production']['schema']
        table_name = CONFIGURATIONS[metrics_config_key]['database']['production']['liquidity_calc_tbl']\
                        ['condition'][condition]
        description = CONFIGURATIONS[metrics_config_key]['database']['production']['liquidity_calc_tbl']\
                        ['tbl_description'][condition]
        
        try:
            logger.info(f"Uploading {description} to RDS {schema_name}.{table_name}")
            df = load_df_from_s3_airflow_temp(s3_path)
            df = create_identifier(df, 'reference_number')

            if df.empty:
                logger.warning(f"Empty DataFrame for {description}. Skipping upload.")
                return

            upload_formatted_df_to_rds(
                df=df, 
                schema_name=schema_name, 
                table_name=table_name,
                drop_source=True, 
                context=context
            )
            logger.info(f"Successfully uploaded {description} DataFrame to RDS {schema_name}.{table_name}")
        except Exception as e:
            logger.error(f"Upload failed for {description}: {str(e)}")
            raise

    # Task 4: Clean up Airflow temp files
    @task(
        task_id='cleanup_airflow_temp_files',
        trigger_rule='all_success',
        doc_md="""
        ### Cleanup Airflow Temp Files
        This task cleans up the airflow temp files after the DAG has completed.
        """
    )
    def task_cleanup_airflow_temp_files(**context) -> None:
        bucket_name = 'wcc-dl'
        directory_prefix = f"airflow_temp/{context['dag'].dag_id}/"
        cleanup_airflow_temp_files(bucket_name=bucket_name, directory_prefix=directory_prefix)

    
    # A list that stores Airflow TaskGroup objects where each TaskGroup contains multiple tasks
    # These TaskGroup objects can then be used to set dependencies using operators like >>
    chunk_groups = []

    # Process each chunk independently through the entire pipeline
    for chunk in list_of_chunks:
        chunk_identifier = '-'.join(chunk)
        # Sanitize the group_id
        group_id = f"process_chunk_{sanitize_group_id(chunk_identifier)}"

        with TaskGroup(group_id=group_id) as chunk_group:
            # Task 1: Get the required historical listings data to calculate the number of weeks a listing remains on the market
            historical_listings_path = get_hist_listings_from_db(chunk=chunk,
                                                                  chunk_identifier=chunk_identifier,
                                                                  metrics_config_key=metrics_config_key,
                                                                  condition=condition
                                                                  )
            # Task 2: Analyze listings duration
            listings_duration_path = task_analyze_listings_duration(s3_path=historical_listings_path,
                                                                   chunk_identifier=chunk_identifier
                                                                   )
            # Task 3: Upload the listings weeks on market to RDS
            task_upload_listings_weeks_on_market_to_rds(s3_path=listings_duration_path,
                                                        metrics_config_key=metrics_config_key,
                                                        condition=condition
                                                        )
            
            # Append the Airflow TaskGroup object to chunk_groups (list) to signal the task group instance is complete
            # Move to the next chunk/taskgroup instance if there are any remaining chunks. 
            # Otherwise, move to the next taskgroup/cleanup task.
            chunk_groups.append(chunk_group)

    # Cleanup group
    # Clean-up is executed once all tasks and chunks have been processed
    with TaskGroup(group_id="cleanup_group") as cleanup_group:
        task_cleanup_airflow_temp_files()

    trigger_stop_self_ec2_dag = TriggerDagRunOperator(
    task_id='trigger_stop_self_ec2_dag',  # Task ID
    trigger_dag_id='stop_self_ec2',  # ID of DAG to be triggered
    wait_for_completion=False,  # As soon as the second dag in question gets triggered this dag will be marked as successful
    # poke_interval=60,  # Time in seconds to wait between checks
    # timeout=5400  # Timeout after 1.5 hours
    )

    trigger_stop_self_ec2_dag_2 = TriggerDagRunOperator(
    task_id='trigger_stop_self_ec2_dag_2',  # Task ID
    trigger_dag_id='stop_self_ec2',  # ID of DAG to be triggered
    wait_for_completion=False,  # As soon as the second dag in question gets triggered this dag will be marked as successful
    # poke_interval=60,  # Time in seconds to wait between checks
    # timeout=5400  # Timeout after 1.5 hours
    )
    # Set dependencies between taskgroups
    # chunk_groups >> cleanup_group
    check_execution = check_biweekly_execution()
    check_execution >> chunk_groups + [trigger_stop_self_ec2_dag]  # Branch to either execute or skip
    chunk_groups >> cleanup_group >> trigger_stop_self_ec2_dag_2
dag = listings_numb_weeks_on_market_dag()