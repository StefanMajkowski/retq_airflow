from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'credentials', '.env')
logger.info(f"Loading environment variables from: {env_path}")
load_dotenv(env_path)

# Set GCP credentials path
gcp_key_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'credentials', 'retentionq-37d37a6c9a9c.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_key_path
logger.info(f"Set GOOGLE_APPLICATION_CREDENTIALS to: {gcp_key_path}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'postgres_to_bigquery_transfer',
    default_args=default_args,
    description='Transfer specific table from PostgreSQL to BigQuery',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['postgres', 'bigquery', 'data_transfer'],
)

def transfer_postgres_to_bigquery(source_table, columns=None, bq_dataset=None, bq_table=None, **context):
    """
    Extract data from PostgreSQL and load directly to BigQuery
    
    Parameters:
    - source_table: Name of the table to extract
    - columns: List of columns to extract (default is all columns)
    - bq_dataset: BigQuery dataset name
    - bq_table: BigQuery table name (defaults to source_table name)
    """
    try:
        # Get postgres connection details
        pg_host = os.getenv('POSTGRES_HOST')
        pg_database = os.getenv('POSTGRES_DATABASE')
        pg_schema = os.getenv('POSTGRES_SCHEMA')
        pg_user = os.getenv('POSTGRES_USER')
        pg_password = os.getenv('POSTGRES_PASSWORD')
        pg_port = os.getenv('POSTGRES_PORT', '5432')
        
        # Get BigQuery project
        bq_project_id = os.getenv('GCP_PROJECT_ID')
        
        # Create PostgreSQL connection
        connection_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
        engine = create_engine(connection_string)
        
        # Create BigQuery hook
        bq_hook = BigQueryHook(
            gcp_conn_id='google_cloud_default',
            project_id=bq_project_id,
            use_legacy_sql=False,
            impersonation_chain=None,
            location='europe-central2'  # Replace with your BigQuery dataset location if different
        )
        
        # Set schema if provided
        schema_prefix = f"{pg_schema}." if pg_schema else ""
        
        # Single table transfer with parameters
        columns_str = ", ".join(columns) if columns else "*"
        bq_dataset_name = bq_dataset or "default_dataset"
        bq_table_name = bq_table or source_table
        
        logger.info(f"Transferring table: {source_table} to BigQuery {bq_dataset_name}.{bq_table_name}")
        logger.info(f"Columns: {columns_str if columns else 'All columns'}")
        
        # Extract data from PostgreSQL
        query = f"SELECT {columns_str} FROM {schema_prefix}{source_table}"
        df = pd.read_sql(query, engine)
        
        # Load data to BigQuery
        bq_hook.insert_rows_from_dataframe(
            dataset_id=bq_dataset_name,
            table_id=bq_table_name,
            dataframe=df,
            write_disposition='WRITE_TRUNCATE'
        )
        
        logger.info(f"Successfully transferred {len(df)} rows from {source_table} to {bq_dataset_name}.{bq_table_name}")
    
    except Exception as e:
        logger.error(f"Error in transfer process: {str(e)}")
        raise

# Define task for transferring a specific table
transfer_task = PythonOperator(
    task_id='transfer_client_profiles',
    python_callable=transfer_postgres_to_bigquery,
    op_kwargs={
        'source_table': 'client_profiles',  # Replace with your table name
        'columns': ['id', 'first_name', 'city', 'created_at', 'updated_at'],      # Optional: specify columns or remove for all
        'bq_dataset': 'bronze_test',           # BigQuery dataset
        'bq_table': 'client_profiles_test_load'           # BigQuery table name
    },
    dag=dag,
) 