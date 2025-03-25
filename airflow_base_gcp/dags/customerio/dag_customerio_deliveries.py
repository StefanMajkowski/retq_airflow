# This dag transfers customerio data from google cloud storage to snowflake, partition data by date to make incremental refreshes
from google.cloud import storage

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def move_deliveries_files(source_bucket_name, destination_prefix):
    """
    Moves files containing 'deliveries' in their names from a source bucket to a new location within the same bucket.

    Parameters:
    - source_bucket_name: The name of the GCS bucket.
    - destination_prefix: The destination prefix within the same bucket where files should be moved.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(source_bucket_name)
    
    blobs = bucket.list_blobs()  # List all objects in the bucket
    for blob in blobs:
        if 'deliveries' in blob.name:
            # Define the new name/path for the blob
            new_name = f"{destination_prefix}/{blob.name.split('/')[-1]}"
            # Copy the blob to the new location
            bucket.copy_blob(blob, bucket, new_name)
            # Delete the original blob
            blob.delete()

            print(f"Moved {blob.name} to {new_name}")

# Example usage
source_bucket_name = 'gs://healthlabs-datateam-prod-datalake-raw/customer_io'
destination_prefix = 'new-location-for-deliveries'
move_deliveries_files(source_bucket_name, destination_prefix)


def move_files_wrapper():
    source_bucket_name = 'your-source-bucket-name'
    destination_prefix = 'new-location-for-deliveries'
    move_deliveries_files(source_bucket_name, destination_prefix)

with DAG('move_deliveries_files_dag',
         start_date=datetime(2023, 1, 1),
         schedule_interval='@daily',  # Adjust as needed
         catchup=False) as dag:

    move_files_task = PythonOperator(
        task_id='move_deliveries_files',
        python_callable=move_files_wrapper
    )

move_files_task
