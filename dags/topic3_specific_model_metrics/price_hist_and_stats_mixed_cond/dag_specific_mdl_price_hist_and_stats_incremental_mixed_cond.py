# Production Dag

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

# sys.path.append('/home/ubuntu/airflow-metrics-workflow/dags/')

from modules.utilities import (
    add_hash_id, 
    load_env_vars, 
    setup_logging, 
    get_s3_client, 
    sanitize_group_id,
    split_brands_list    
)
from modules.datalake_operations import (
    save_df_to_s3_airflow_temp,
    load_df_from_s3_airflow_temp  
)
from modules.database_operations import (
    load_latest_week_listings,    
    combine_new_and_historical_data,
    upload_formatted_df_to_rds
)
from modules.data_aggregation import (
    apply_incremental_calc_rolling_avg, 
    apply_dollar_and_pct_change_calc,
    apply_extend_and_forward_fill
)

from modules.utilities import setup_logging, get_s3_client
from modules.config import brands_ready_for_metrics, metrics_configurations

from topic3_specific_model_metrics.price_hist_and_stats_mixed_cond.util_specific_mdl_price_hist_and_stats_incremental_mixed_cond import (    
    weekly_aggregation_by_specific_mdl       
)


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
    'specific_mdl_price_hist_and_stats_incremental_mixed',    
    default_args=default_args,
    description='A DAG to process incrementally specific model mixed condition (New, and Pre-Owned) metrics',    
    # schedule='0 0 * * 3', # Run weekly on Wednesday at midnight
    schedule=None,
    catchup=False,
    max_active_runs=1,    
    max_consecutive_failed_dag_runs=1, 
    tags=['metrics','price_history', 'specific_model', 'mixed_condition']
)

def specific_mdl_price_hist_and_stats_incremental_mixed_dag():  
    # Variables definitions
    brands_list = brands_ready_for_metrics()
    chunk_size = 3 # Adjust as needed
    list_of_chunks = split_brands_list(brands_list, chunk_size)
    condition = 'mixed'
    metrics_config_key = 'specific_mdl_metrics_config_key'
    # prod_schema_name = 'metrics'
    # prod_table_1_name = 'wcc_specific_mdl_wkl_price_and_stats_mixed' 
    # description = 'Specific Model Weekly Price and Stats Mixed Condition Data'

    # Task 1: Load new listings
    @task(
        task_id='load_latest_week_listings',
        pool='specific_model_metrics_pool',
        retry_delay=timedelta(minutes=5),
        retries=1,
        depends_on_past=True,  
        wait_for_downstream=True,
        doc_md="""
        ### Load the latest week listings from the RDS Postgresql database table 
        Loads the latest week's watch listings for brands that are ready for metrics calculation.

        **Input:**
        - chunks_of_brands: List of brand names to process. A subset of the full list of brands (brands_list).
        
        **Output:**
        - Saves DataFrame to S3 temporary location and returns the path
        
        **Process:**
        1. Queries database for new listings
        2. Applies data type enforcement
        3. Saves to S3 temporary location         
        
        **Dependencies:**
        - Requires database connection
        - Uses S3 storage
        """,
    )
    def task_load_latest_week_listings(chunk: list[str], chunk_identifier, metrics_config_key: str, **context) -> str:
        try:
            logger.info(f"Processing chunk {chunk}")            

            data_interval_start_date = context['data_interval_start']
            data_interval_end_date = context['data_interval_end']
            logger.info(f"""Data interval start date: {data_interval_start_date} \n
                            Data interval end date: {data_interval_end_date}
                        """)

            
            # Load the latest week's listings for the specified brands
            # chunk is a list of a handful of brands.
            # Query data from wcc_specific_mdl_wkl_price_and_stats_mixed rds table directly related to the brands that are part of the chunk.
            df = load_latest_week_listings(brands_list=chunk, metrics_config_key=metrics_config_key, data_interval_end_date=data_interval_end_date)
            # Save the DataFrame to S3 temporary location
            s3_path = save_df_to_s3_airflow_temp(df,
                                                context['dag'].dag_id,
                                                f"{context['task'].task_id}_{chunk_identifier}",
                                                context['ts']
            )
            logger.info(f"Successfully processed chunk: {chunk}")
            return s3_path
        except Exception as e:
            logger.error(f"Failed to process chunk {chunk}: {str(e)}")
            raise
        
    # Task 2: weekly_aggregation_by_specific_mdl
    @task(task_id='weekly_aggregation_by_specific_mdl',
          pool='specific_model_metrics_pool',
          doc_md="""
          ### Weekly Resample Aggregation
          Resamples the data by week and aggregates the metrics using outlier-free data.
          """          
    )
    def task_weekly_aggregation_by_specific_mdl(s3_path, chunk_identifier, **context):
        # TODO: Suggested improvements - Generalize this function to handle specific model, parent model, brand, and reference number aggregations
        df = load_df_from_s3_airflow_temp(s3_path)
        wkl_df = weekly_aggregation_by_specific_mdl(df)
        s3_path = save_df_to_s3_airflow_temp(wkl_df, 
                                             context['dag'].dag_id,
                                             f"{context['task'].task_id}_{chunk_identifier}",                                             
                                             context['ts']
        )
        return s3_path
    
    # Task 3: combine_new_and_historical_data
    @task(task_id='combine_new_and_historical_data',
          pool='specific_model_metrics_pool',
          doc_md="""
          ### Combine New and Historical Data
          Combines the new and historical data for the specific model.
          """
    )
    def task_combine_new_and_historical_data(s3_path, chunk: list[str], chunk_identifier, metrics_config_key: str, condition: str, **context):
        df = load_df_from_s3_airflow_temp(s3_path)
        # Year multiplier is set to 1.5 to get 1.5 years of historical data
        combined_df = combine_new_and_historical_data(new_weekly_data=df, 
                                                      brands_list=chunk, 
                                                      metrics_config_key=metrics_config_key, 
                                                      condition=condition, 
                                                      year_multiplier=1.5
        )

        s3_path = save_df_to_s3_airflow_temp(combined_df, 
                                             context['dag'].dag_id,
                                             f"{context['task'].task_id}_{chunk_identifier}",                                             
                                             context['ts'],
                                             convert_rolling_avg_col=False
        )
        return s3_path

    # Task 4: Extend and Forward Fill
    @task(task_id='extend_and_forward_fill',
          pool='specific_model_metrics_pool',
          doc_md="""
          ### Extend and Forward Fill
          Extends the data to the latest date in the dataframe and forward fills missing values
            for each brand and specific model combination.
          """
    )
    def task_extend_and_forward_fill(s3_path, chunk_identifier, metrics_config_key, **context):
        df = load_df_from_s3_airflow_temp(s3_path)
        extended_and_ffilled = apply_extend_and_forward_fill(df, metrics_config_key)
        s3_path = save_df_to_s3_airflow_temp(extended_and_ffilled, 
                                             context['dag'].dag_id,
                                             f"{context['task'].task_id}_{chunk_identifier}",                                             
                                             context['ts']
        )
        return s3_path

    # Task 5: Calculate Rolling Averages
    @task(task_id='calculate_rolling_averages',
          pool='specific_model_metrics_pool',
          doc_md="""
          ### Calculate Rolling Averages
          Calculates the rolling averages for the metrics.
          """
    )

    def task_calculate_rolling_averages(s3_path: str, chunk_identifier: str, metrics_config_key: str, **context):
        df = load_df_from_s3_airflow_temp(s3_path)        
        rolling_avg_df = apply_incremental_calc_rolling_avg(df, metrics_config_key)
        s3_path = save_df_to_s3_airflow_temp(rolling_avg_df, 
                                             context['dag'].dag_id, 
                                             f"{context['task'].task_id}_{chunk_identifier}", 
                                             context['ts'],
                                             convert_rolling_avg_col=True
    )
        return s3_path
    # Task 6: Calculate Dollar and Pct Change
    @task(task_id='calculate_dollar_and_pct_change',
          pool='specific_model_metrics_pool', 
    )
    def task_calculate_dollar_and_pct_change(s3_path: str, chunk_identifier: str, metrics_config_key: str, **context):
        df = load_df_from_s3_airflow_temp(s3_path)        
        final_df = apply_dollar_and_pct_change_calc(df, metrics_config_key)
        s3_path = save_df_to_s3_airflow_temp(final_df, 
                                             context['dag'].dag_id, 
                                             f"{context['task'].task_id}_{chunk_identifier}",  
                                             context['ts']
    )
        return s3_path


    # Task 7: Upload final data to RDS
    @task(
    task_id='generic_upload_to_rds',
    pool='database_operations_pool',
    doc_md="""
    ### Upload Generic DataFrame to RDS
        """
    )
    def task_generic_upload_to_rds(s3_path: str, metrics_config_key: str, condition: str, **context):
        # Unlock configurations using metrics_config_key 
        CONFIGURATIONS = metrics_configurations()
        schema_name = CONFIGURATIONS[metrics_config_key]['database']['production']['schema']
        table_name = CONFIGURATIONS[metrics_config_key]['database']['production']['price_hist_and_stats_tbl']['condition'][condition]
        description = CONFIGURATIONS[metrics_config_key]['database']['production']['price_hist_and_stats_tbl']['tbl_description'][condition]
        
        try:
            logger.info(f"Uploading {description} to RDS {schema_name}.{table_name}")
            df = load_df_from_s3_airflow_temp(s3_path)

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


    # Task 8: Clean up temporary airflow directory in s3
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

    # A list that stores Airflow TaskGroup objects where each TaskGroup contains multiple tasks
    # These TaskGroup objects can then be used to set dependencies using operators like >>
    chunk_groups = []

    # Process each chunk independently through the entire pipeline
    for chunk in list_of_chunks:
        chunk_identifier = '-'.join(chunk)
        # Sanitize the group_id
        group_id = f"process_chunk_{sanitize_group_id(chunk_identifier)}"

        with TaskGroup(group_id=group_id) as chunk_group:
            # Task 1: Load new listings
            latest_week_listings_path = task_load_latest_week_listings(chunk, chunk_identifier, metrics_config_key)
            # Task 2: Weekly Aggregation
            weekly_agg_path = task_weekly_aggregation_by_specific_mdl(latest_week_listings_path, chunk_identifier)
            # Task 3: Combine New and Historical Data
            combined_data_path = task_combine_new_and_historical_data(weekly_agg_path, chunk, chunk_identifier,  metrics_config_key, condition)
            # Task 4: Extend and Forward Fill
            extended_ffill_data_path = task_extend_and_forward_fill(combined_data_path, chunk_identifier, metrics_config_key)
            # Task 5: Calculate Rolling Averages
            rolling_avg_path = task_calculate_rolling_averages(extended_ffill_data_path, chunk_identifier, metrics_config_key)
            # Task 6: Calculate Dollar and Pct Change
            dollar_and_pct_change_path = task_calculate_dollar_and_pct_change(rolling_avg_path, chunk_identifier, metrics_config_key)
            # Task 7: Upload final data to RDS
            task_generic_upload_to_rds(dollar_and_pct_change_path, metrics_config_key, condition)

            # Append the Airflow TaskGroup object to chunk_groups (list) 
            chunk_groups.append(chunk_group)

    # Cleanup group
    # Clean-up is executed once all tasks and chunks have been processed
    with TaskGroup(group_id="cleanup_group") as cleanup_group:
        task_cleanup_temp_files()    
    # Add TriggerDagRunOperator
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_parent_mdl_price_hist_and_stats_incremental_mixed_cond_dag',
        trigger_dag_id='parent_mdl_price_hist_and_stats_incremental_mixed',
        wait_for_completion=False,
    )
    # Set dependencies between groups
    chunk_groups >> cleanup_group >> trigger_next_dag

dag = specific_mdl_price_hist_and_stats_incremental_mixed_dag()