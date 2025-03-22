# Production Dag

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


import modules.datalake_operations
from modules.datalake_operations import (
    save_df_to_s3_airflow_temp,
    load_df_from_s3_airflow_temp
)
import modules.utilities
from modules.utilities import (
    setup_logging, 
    get_s3_client,      
    create_identifier,
    custom_slugify,
    sanitize_group_id,
    split_brands_list    
)
import modules.database_operations
from modules.database_operations import (   
    upload_formatted_df_to_rds
)
import modules.data_aggregation
from modules.data_aggregation import (     
    market_makeup_condition_stats     
)

from modules.config import brands_ready_for_metrics
from topic6_market_makeup_stats.util_mrkt_makeup_pipeline import (
    load_active_listings
)

logger = setup_logging(__name__)



# Define the parameters for the data to be uploaded to RDS
UPLOAD_CONFIGS = [
    {
        "s3_path_reference_name": "brand_mrkt_makeup_path",
        "schema": "metrics",
        "table": "brand_mrkt_makeup",
        "description": "Brand Market Makeup Data"
    },
    {
        "s3_path_reference_name": "specific_mdl_mrkt_makeup_path",
        "schema": "metrics",
        "table": "specific_mdl_mrkt_makeup",
        "description": "Specific Model Market Makeup Data"
    },
    {
        "s3_path_reference_name": "parent_mdl_mrkt_makeup_path",
        "schema": "metrics",
        "table": "parent_mdl_mrkt_makeup",
        "description": "Parent Model Market Makeup Data"
    },
    {
        "s3_path_reference_name": "reference_numb_mrkt_makeup_path",
        "schema": "metrics",
        "table": "reference_numb_mrkt_makeup",
        "description": "Reference Number Market Makeup Data"
    }
]


# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'wcc',
    'depends_on_past': True,
    'start_date': datetime(2024, 12, 11),    
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
@dag(
    'market_makeup_pipeline',    
    default_args=default_args,
    description='A DAG to calculate market makeup statistics of active listings',
    # schedule='0 0 * * 3', # Run weekly on Wednesday at midnight    
    schedule=None,
    catchup=False,
    max_active_runs=1,
   tags=['metrics', 'market_makeup', 'active_listings']
)

def market_makeup_pipeline():
    # Define constants
    brands_list = brands_ready_for_metrics()
    chunk_size = 3 # Adjust as needed
    list_of_chunks = split_brands_list(brands_list, chunk_size)   

    @task(
        task_id='load_active_listings',
        pool='market_makeup_pool',        
        retry_delay=timedelta(minutes=5),
        retries=1,
        doc_md="""
        ### Load Active Listings
        
        Loads the latest week's watch listings for brands that are ready for metrics calculation.
        
        **Input:**
        - chunks_of_brands: List of brand names to process. A subset of the full list of brands (brands_list).
        
        **Output:**
        - Saves DataFrame to S3 temporary location and returns the path
        
        **Process:**
        1. Queries database public.metrics_data_enriched table for the latest week's watch listings
        2. Applies data type enforcement
        3. Sorts by brand, reference number, and date
        5. Saves to S3
        
        **Dependencies:**
        - Requires database connection
        - Uses S3 storage
        """       
    )

    def task_load_active_listings(chunk, chunk_identifier, **context):
        try:
            logger.info(f"Processing chunk: {chunk}")

            data_interval_start_date = context['data_interval_start']
            data_interval_end_date = context['data_interval_end']
            logger.info(f"""Data interval start date: {data_interval_start_date} \n
                            Data interval end date: {data_interval_end_date}
                        """)

            # chunk is a list of a handful of brands.
            # Query data from public.metrics_data_enriched rds table directly related to the brands that are part of the chunk.
            df = load_active_listings(chunk, data_interval_end_date)
            # Saves df to the airflow temp directory in S3 and returns the path
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
    
    
    @task(
        task_id='calculate_brand_mrkt_makeup', 
        pool='market_makeup_pool', 
        doc_md="""
        ### Calculate Brand Market Makeup
        """
    )
    def task_calculate_brand_mrkt_makeup(s3_path, chunk_identifier, **context):
        try:
            logger.info(f"Calculating brand market makeup for {s3_path}")
            df = load_df_from_s3_airflow_temp(s3_path)
            brand_mrkt_makeup = market_makeup_condition_stats(df, ['date', 'brand'] )
            brand_mrkt_makeup['brand_slug'] = brand_mrkt_makeup['brand'].apply(custom_slugify)
            s3_path = save_df_to_s3_airflow_temp(brand_mrkt_makeup,
                                                context['dag'].dag_id,
                                                f"{context['task'].task_id}_{chunk_identifier}",
                                                context['ts']
            )
            logger.info(f"Brand market makeup calculated for {s3_path}")
            return s3_path
        
        except Exception as e:
            logger.error(f"Failed to calculate brand market makeup for {s3_path}: {str(e)}")
            raise

    @task(
        task_id='calculate_parent_mdl_mrkt_makeup',
        pool='market_makeup_pool',
        doc_md="""
        ### Calculate Parent Model Market Makeup
        """
    )
    def task_calculate_parent_mdl_mrkt_makeup(s3_path, chunk_identifier, **context):
        try:
            logger.info(f"Calculating parent model market makeup for {s3_path}")
            df = load_df_from_s3_airflow_temp(s3_path)
            parent_mdl_mrkt_makeup = market_makeup_condition_stats(df, ['date', 'brand', 'parent_model'])
            parent_mdl_mrkt_makeup['brand_slug'] = parent_mdl_mrkt_makeup['brand'].apply(custom_slugify)
            s3_path = save_df_to_s3_airflow_temp(parent_mdl_mrkt_makeup,
                                                context['dag'].dag_id,
                                                f"{context['task'].task_id}_{chunk_identifier}",
                                                context['ts']
            )
            logger.info(f"Parent model market makeup calculated for {s3_path}")
            return s3_path
        
        except Exception as e:
            logger.error(f"Failed to calculate parent model market makeup for {s3_path}: {str(e)}")
            raise
    
    @task(
        task_id='calculate_specific_mdl_mrkt_makeup',
        pool='market_makeup_pool',
        doc_md="""
        ### Calculate Specific Model Market Makeup
        """
    )
    def task_calculate_specific_mdl_mrkt_makeup(s3_path, chunk_identifier, **context):
        try:
            logger.info(f"Calculating specific model market makeup for {s3_path}")
            df = load_df_from_s3_airflow_temp(s3_path)
            specific_mdl_mrkt_makeup = market_makeup_condition_stats(df, ['date', 'brand', 'specific_model'])
            specific_mdl_mrkt_makeup['brand_slug'] = specific_mdl_mrkt_makeup['brand'].apply(custom_slugify)
            s3_path = save_df_to_s3_airflow_temp(specific_mdl_mrkt_makeup,
                                                context['dag'].dag_id,
                                                f"{context['task'].task_id}_{chunk_identifier}",
                                                context['ts']
            )
            logger.info(f"Specific model market makeup calculated for {s3_path}")
            return s3_path
        
        except Exception as e:
            logger.error(f"Failed to calculate specific model market makeup for {s3_path}: {str(e)}")
            raise
    
    @task(
        task_id='calculate_reference_numb_mrkt_makeup',
        pool='market_makeup_pool',
        doc_md="""
        ### Calculate Reference Number Market Makeup
        """
    )
    def task_calculate_reference_numb_mrkt_makeup(s3_path, chunk_identifier, **context):
        try:
            logger.info(f"Calculating reference number market makeup for {s3_path}")
            df = load_df_from_s3_airflow_temp(s3_path)
            reference_numb_mrkt_makeup = market_makeup_condition_stats(df, ['date', 'brand', 'reference_number'])
            reference_numb_mrkt_makeup = create_identifier(reference_numb_mrkt_makeup, 'reference_number')
            s3_path = save_df_to_s3_airflow_temp(reference_numb_mrkt_makeup,
                                                context['dag'].dag_id,
                                                f"{context['task'].task_id}_{chunk_identifier}",
                                                context['ts']
            )
            logger.info(f"Reference number market makeup calculated for {s3_path}")
            return s3_path
        
        except Exception as e:
            logger.error(f"Failed to calculate reference number market makeup for {s3_path}: {str(e)}")
            raise
    
    
    @task(
        task_id='generic_upload_to_rds',
        pool='database_operations_pool',
        doc_md="""
        ### Upload Generic DataFrame to RDS
        """
    )
    def task_generic_upload_to_rds(s3_path: str, schema_name: str, table_name: str, description: str, **context):
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
                context=context
            )
            logger.info(f"Successfully uploaded {description} DataFrame to RDS {schema_name}.{table_name}")
        except Exception as e:
            logger.error(f"Upload failed for {description}: {str(e)}")
            raise
    

    # A list that stores Airflow TaskGroup objects where each TaskGroup contains multiple tasks
    # These TaskGroup objects can then be used to set dependencies using operators like >> 
    
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

    
    
    chunk_groups = []

    # Process each chunk independently through the entire pipeline
    for chunk in list_of_chunks:
        chunk_identifier = '-'.join(chunk)
        # Sanitize the group_id
        group_id = f"process_chunk_{sanitize_group_id(chunk_identifier)}"

        with TaskGroup(group_id=group_id) as chunk_group:
            # Define task dependencies
            active_listings_path = task_load_active_listings(chunk, chunk_identifier)
            
            # Dictionary to store the paths of the calculated dataframes
            s3_paths_to_data_objects = {
                "brand_mrkt_makeup_path": task_calculate_brand_mrkt_makeup(active_listings_path, chunk_identifier),
                "parent_mdl_mrkt_makeup_path": task_calculate_parent_mdl_mrkt_makeup(active_listings_path, chunk_identifier),
                "specific_mdl_mrkt_makeup_path": task_calculate_specific_mdl_mrkt_makeup(active_listings_path, chunk_identifier),
                "reference_numb_mrkt_makeup_path": task_calculate_reference_numb_mrkt_makeup(active_listings_path, chunk_identifier)
            }
            
            # Upload to RDS
            for config in UPLOAD_CONFIGS:
                task_generic_upload_to_rds(
                    s3_paths_to_data_objects[config["s3_path_reference_name"]], 
                    config["schema"], 
                    config["table"], 
                    config["description"]           
                )                     
            
            # Append the Airflow TaskGroup object to chunk_groups (list) 
            chunk_groups.append(chunk_group)

    # Cleanup group
    # Clean-up is executed once all tasks and chunks have been processed
    with TaskGroup(group_id="cleanup_group") as cleanup_group:
        task_cleanup_temp_files()    

    # Add TriggerDagRunOperator
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_price_volatility_of_ref_num_mixed_cond_dag',
        trigger_dag_id='price_volatility_of_ref_num_mixed_cond',
        wait_for_completion=False,
    )    
    # Set dependencies between groups
    chunk_groups >> cleanup_group >> trigger_next_dag

dag = market_makeup_pipeline()

        