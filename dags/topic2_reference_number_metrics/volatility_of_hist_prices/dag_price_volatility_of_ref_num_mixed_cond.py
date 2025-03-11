import pandas as pd
from dotenv import load_dotenv
import numpy as np
import sys 
from io import BytesIO
from pathlib import Path
import boto3
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

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

from modules.advanced_financial_metrics import (
    models_that_qualify_for_volatility_calculations,
    resample_and_calc_volatility_precursors,
    apply_volatility_calculations
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
    'price_volatility_of_ref_num_mixed_cond',    
    default_args=default_args,
    description='A DAG to calculate the price volatility of reference numbers mixed condition (New, and Pre-Owned)',    
    # schedule='0 0 * * 3', # Run weekly on Wednesday at midnight
    schedule=None,
    catchup=False,
    max_active_runs=1,    
    max_consecutive_failed_dag_runs=1, 
    tags=['metrics','volatility', 'reference_number', 'mixed_condition']
)

def price_volatility_of_ref_num_mixed_cond_dag():
    # varibles definitions
    brands_list = brands_ready_for_metrics()
    chunk_size = 3 # Adjust as needed
    list_of_chunks = split_brands_list(brands_list, chunk_size)
    condition = 'mixed'
    metrics_config_key = 'ref_num_metrics_config_key'
    override_sql_query_file = 'ref_num_volatility_get_required_price_hist.sql'
    year_multiplier = 10 # Will search up to 10-year of price history but will return as much as it is available. 

    # task definitions

    # Task 1: Get the required price history to calculate the price volatility
    @task(
        task_id='get_reference_numbers_price_history',
        pool='ref_num_volatility_pool',
        retry_delay=timedelta(minutes=5),
        retries=1,
        depends_on_past=True,
        wait_for_downstream=True,
        doc_md="""
            ### Get the required price history to calculate the price volatility
            This task gets the required price history to calculate the price volatility
            """
    )
    def task_get_reference_numbers_price_history(chunk: list[str], 
                                   chunk_identifier, 
                                   metrics_config_key: str,
                                   condition: str,
                                   sql_query_file: str, 
                                   year_multiplier: int,
                                   **context) -> dict[str, str]:
        try:
            logger.info(f"Processing chunk {chunk}")         
            df = load_historical_data(brands_list = chunk, 
                                    metrics_config_key = metrics_config_key, 
                                    condition = condition,                                   
                                    sql_query_file = sql_query_file,
                                    year_multiplier = year_multiplier
            )
            # Date of the most recent price history data point in the chunk
            date_most_recent_data_point = df['date'].max().date()
            # Push df_max_date value to xcom
            context['task_instance'].xcom_push(key='date_most_recent_data_point', value=date_most_recent_data_point)

            s3_path = save_df_to_s3_airflow_temp(df,
                                                context['dag'].dag_id,
                                                f"{context['task'].task_id}_{chunk_identifier}",
                                                context['ts']
            )
            logger.info(f"Successfully processed chunk: {chunk}")

            task1_output_dict = {
                'required_price_history_path': s3_path,
                'date_most_recent_data_point': date_most_recent_data_point
            }

            return task1_output_dict
        
        except Exception as e:
            logger.error(f"""Error in get_required_price_history() \n
                         Failed to process chunk {chunk}: {str(e)}""")
            raise 
    
    # Task 2: Process historical data
    @task(
        task_id='select_models_for_volatility_calculations',
        pool='ref_num_volatility_pool',
        retry_delay=timedelta(minutes=5),
        retries=1
    )
    def task_select_models_for_volatility_calculations(task1_output_dict: dict[str, str], chunk_identifier: str, **context) -> dict[str, str]:
        s3_path = task1_output_dict['required_price_history_path']
        df = load_df_from_s3_airflow_temp(s3_path)

        filtered_groups = models_that_qualify_for_volatility_calculations(df)

        # Save the filtered groups to S3
        s3_paths = save_dict_of_dfs_to_s3_airflow_temp(dict_data=filtered_groups,
                                                    dag_id=context['dag'].dag_id,
                                                    task_id=f"{context['task'].task_id}_{chunk_identifier}",
                                                    ts=context['ts']
        )

        return s3_paths
    
        
    # Task 3: Resample and calculate the volatility precursors
    @task(
        task_id='resample_and_calc_volatility_precursors',
        pool='ref_num_volatility_pool',
        retry_delay=timedelta(minutes=5),
        retries=1
    )
    def task_resample_and_calc_volatility_precursors(s3_paths: dict[str, str], chunk_identifier: str, **context) -> dict[str, str]:
        # Takes in a dictionary of s3 paths and returns a dictionary of dataframes recreating the filtered_groups dictionary
        # created in task_select_models_for_volatility_calculations 
        filtered_groups = load_dict_of_dfs_from_s3_airflow_temp(s3_paths)
        # Keeping the dictionary structure of the input data (filtered_groups) resampling and other calculations are applied
        # to each dataframe in the dictionary. The keys of the dictionary are the interval time periods:
        #  1 month ('1M'), 3 months ('3M'), 6 months ('6M'), 1 year ('1Y')  
        resampled_data_and_precursors = resample_and_calc_volatility_precursors(filtered_groups)    

        # Save the resampled data and precursors to S3
        s3_paths = save_dict_of_dfs_to_s3_airflow_temp(dict_data=resampled_data_and_precursors,
                                                    dag_id=context['dag'].dag_id,
                                                    task_id=f"{context['task'].task_id}_{chunk_identifier}",
                                                    ts=context['ts']
        )
        return s3_paths
    
    # Task 4: Apply the volatility calculations
    @task(
        task_id='apply_volatility_calculations',
        pool='ref_num_volatility_pool',
        retry_delay=timedelta(minutes=5),
        retries=1
    )
    def task_apply_volatility_calculations(s3_paths: dict[str, str], chunk_identifier: str, task1_output_dict: dict[str, str], **context) -> pd.DataFrame:
        # Unpack the dictionary of s3 paths to get dictionary of dataframes
        resampled_data_and_precursors = load_dict_of_dfs_from_s3_airflow_temp(s3_paths)
        # Unpack the task1_output_dict to get the date of the most recent data point
        date_most_recent_data_point = task1_output_dict['date_most_recent_data_point']
        volatility_df = apply_volatility_calculations(resampled_data=resampled_data_and_precursors, 
                                                      date_most_recent_data_point=date_most_recent_data_point
                        )

        # Save the volatility dataframe to S3
        s3_path = save_df_to_s3_airflow_temp(volatility_df,
                                            context['dag'].dag_id,
                                            f"{context['task'].task_id}_{chunk_identifier}",
                                            context['ts']
        )
        return s3_path
    
    # Task 5: Upload the final volatility dataframe to RDS
    @task(
        task_id='upload_final_volatility_df_to_rds',
        pool='ref_num_volatility_pool',
        retry_delay=timedelta(minutes=5),
        retries=1
    )
    def task_upload_final_volatility_df_to_rds(s3_path: str, metrics_config_key: str, condition: str, **context) -> None:
        # Unlock configurations using metrics_config_key 
        CONFIGURATIONS = metrics_configurations()
        schema_name = CONFIGURATIONS[metrics_config_key]['database']['production']['schema']
        table_name = CONFIGURATIONS[metrics_config_key]['database']['production']['volatility_calc_tbl']['condition'][condition]
        description = CONFIGURATIONS[metrics_config_key]['database']['production']['volatility_calc_tbl']['tbl_description'][condition]

        try:
            logger.info(f"Uploading {description} to RDS {schema_name}.{table_name}")
            df = load_df_from_s3_airflow_temp(s3_path)

            if df.empty:
                logger.warning(f"Empty DataFrame for {description}. Skipping upload.")
                return None  # Explicitly return None to show intentional early exit

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
    
    # Task 6: Cleanup Airflow Temp Files
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
            # Task 1: Get the required price history
            task1_output_dict = task_get_reference_numbers_price_history(
                                chunk = chunk,
                                chunk_identifier = chunk_identifier,
                                metrics_config_key = metrics_config_key,
                                condition = condition,
                                sql_query_file = override_sql_query_file,
                                year_multiplier = year_multiplier
                                )

            # Task 2: Select watch models for volatility calculations
            models_that_qualify_path = task_select_models_for_volatility_calculations(
                                            task1_output_dict = task1_output_dict, # Single s3 path
                                            chunk_identifier = chunk_identifier
                                            )

            # Task 3: Resample and calculate volatility precursors
            resampled_data_and_precursors_path = task_resample_and_calc_volatility_precursors(
                                                    s3_paths = models_that_qualify_path, # Dictionary of s3 paths
                                                    chunk_identifier = chunk_identifier
                                                    )


            # Task 4: Apply volatility calculations
            final_volatility_df_path = task_apply_volatility_calculations(
                                            s3_paths = resampled_data_and_precursors_path, # Dictionary of s3 paths
                                            chunk_identifier = chunk_identifier,
                                            # The .output tells Airflow to resolve the XCom value at runtime.
                                            task1_output_dict = task1_output_dict
                                            )
            
            # Task 5: Upload the final volatility dataframe to RDS
            task_upload_final_volatility_df_to_rds(
                s3_path = final_volatility_df_path,
                metrics_config_key = metrics_config_key,
                condition = condition
            )
            
            # Append the Airflow TaskGroup object to chunk_groups (list) to signal the task group instance is complete
            # Move to the next chunk/taskgroup instance if there are any remaining chunks. 
            # Otherwise, move to the next taskgroup/cleanup task.
            chunk_groups.append(chunk_group)

    # Cleanup group
    # Clean-up is executed once all tasks and chunks have been processed
    with TaskGroup(group_id="cleanup_group") as cleanup_group:
        task_cleanup_airflow_temp_files()

    # Set dependencies between taskgroups
    chunk_groups >> cleanup_group
    
dag = price_volatility_of_ref_num_mixed_cond_dag()