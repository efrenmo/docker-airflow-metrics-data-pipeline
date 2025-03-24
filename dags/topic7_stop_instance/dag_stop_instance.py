import boto3
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def stop_ec2_instance():
    """Stops the current EC2 instance."""
    ec2_client = boto3.client("ec2", region_name="us-east-2")  # Replace with your AWS region

    # Fetch instance ID directly from metadata
    # instance_id = requests.get("http://169.254.169.254/latest/meta-data/instance-id").text
    instance_id = 'i-04534fee33871a0bc'
    # Stop the instance
    ec2_client.stop_instances(InstanceIds=[instance_id])
    print(f"Stopping EC2 instance: {instance_id}")

dag = DAG(
    "stop_self_ec2",
    # schedule_interval="@once",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['metrics', 'stop_instance']
)

stop_instance_task = PythonOperator(
    task_id="stop_ec2_instance",
    python_callable=stop_ec2_instance,
    dag=dag
)

stop_instance_task