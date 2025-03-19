from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task
from modules.data_loading import column_names_list
from modules.config import datatype_dictionary
from modules.utilities import setup_logging
import sys
import os

# Add the path to your modules directory
# sys.path.append('/opt/airflow/dags/modules')

# Import your main functions
from topic1_historic_listings_db.dag1_upload_to_historic_listings_db.step_1_wkly_csvs_to_parquet import main as wkly_csvs_to_parquet_main
from topic1_historic_listings_db.dag1_upload_to_historic_listings_db.step_2_upload_to_historic_listings_db_and_dl import main as upload_to_historic_listings_db_main

logger = setup_logging(__name__)

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'wcc',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Instantiate a DAG
@dag(
    'historic_listings_processing',
    default_args=default_args,
    description='Process freshly scraped weekly CSVs to parquet and upload to the historic listings database',
    schedule="0 0 * * 0",
    catchup=False,
    max_active_runs=1,
    tags=['metrics', 'historic_listings']
)


def historic_listings_processing():

    @task(task_id='wkly_csvs_to_parquet')
    def run_wkly_csvs_to_parquet(**context):

        data_interval_end_date = context['data_interval_end']

        logger.info(f"Data interval end date: {data_interval_end_date}")
        try:
            logger.info("Starting wkly_csvs_to_parquet task")        
            column_names = column_names_list()
            data_types = datatype_dictionary()
            wkly_csvs_to_parquet_main(column_names=column_names, data_types=data_types, data_interval_end_date=data_interval_end_date)
            logger.info("Completed wkly_csvs_to_parquet task")
        except Exception as e:
            logger.error(f"Error in wkly_csvs_to_parquet task: {e}")
            raise

    @task(task_id='upload_to_historic_listings_db')
    def run_upload_to_historic_listings_db(**context):
        data_interval_end_date = context['data_interval_end']
        logger.info(f"Data interval end date: {data_interval_end_date}")
        try:
            logger.info("Starting upload_to_historic_listings_db task")            
            upload_to_historic_listings_db_main(data_interval_end_date=data_interval_end_date)
            logger.info("Completed upload_to_historic_listings_db task")
        except Exception as e:
            logger.error(f"Error in upload_to_historic_listings_db task: {e}")
            raise

    # WHEN DECIDED TO UNCOMMENT THE BELOW MAKE SURE TO DISABLE THE SCHEDULE OF THE DAG THAT IS BEING TRIGGERED.             
    # Trigger DAG 2 after DAG 1 completes
    trigger_ref_num_price_hist_and_stats_incremental_mixed_dag = TriggerDagRunOperator(
        task_id='trigger_ref_num_price_hist_and_stats_incremental_mixed_dag',  # Task ID
        trigger_dag_id='ref_num_price_hist_and_stats_mixed',  # ID of DAG to be triggered
        wait_for_completion=False,  # As soon as the second dag in question gets triggered this dag will be marked as successful
        # poke_interval=60,  # Time in seconds to wait between checks
        # timeout=5400  # Timeout after 1.5 hours
    )


    # Define task dependencies
    run_wkly_csvs_to_parquet() >> run_upload_to_historic_listings_db() >> trigger_ref_num_price_hist_and_stats_incremental_mixed_dag

dag = historic_listings_processing()
