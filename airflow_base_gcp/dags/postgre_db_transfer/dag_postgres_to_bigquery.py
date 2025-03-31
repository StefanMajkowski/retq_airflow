from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
from typing import Dict, List, Optional
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'credentials', '.env'))

# Configure logging
logger = logging.getLogger(__name__)

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
    description='Transfer data from PostgreSQL to BigQuery with data type conversion',
    schedule_interval=None,  # Set this based on your needs
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['postgres', 'bigquery', 'data_transfer'],
)

def get_postgres_connection() -> PostgresHook:
    """
    Get PostgreSQL connection using environment variables
    """
    try:
        # Get connection details from environment variables
        pg_host = os.getenv('POSTGRES_HOST')
        pg_schema = os.getenv('POSTGRES_SCHEMA')
        pg_user = os.getenv('POSTGRES_USER')
        pg_password = os.getenv('POSTGRES_PASSWORD')
        pg_port = os.getenv('POSTGRES_PORT', '5432')

        # Create connection using PostgresHook
        return PostgresHook(
            postgres_conn_id='postgres_default',
            host=pg_host,
            database=pg_schema,
            user=pg_user,
            password=pg_password,
            port=pg_port
        )
    except Exception as e:
        logger.error(f"Failed to establish PostgreSQL connection: {str(e)}")
        raise

def get_bigquery_connection() -> BigQueryHook:
    """
    Get BigQuery connection using environment variables
    """
    try:
        # Get GCP project ID from environment variables
        project_id = os.getenv('GCP_PROJECT_ID')
        
        # Create connection using BigQueryHook
        return BigQueryHook(
            gcp_conn_id='google_cloud_default',
            project_id=project_id,
            use_legacy_sql=False
        )
    except Exception as e:
        logger.error(f"Failed to establish BigQuery connection: {str(e)}")
        raise

def get_table_config() -> Dict:
    """
    Get table configuration from environment variables
    """
    try:
        # Get configuration from environment variables
        config = json.loads(os.getenv('POSTGRES_TO_BIGQUERY_CONFIG', '{}'))
        if config:
            return config
        
        # If not in environment variables, try to read from file
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'postgres_to_bigquery_config.json')
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to get table configuration: {str(e)}")
        raise

def convert_data_types(df: pd.DataFrame, type_mapping: Dict) -> pd.DataFrame:
    """
    Convert data types according to the mapping
    """
    try:
        for column, target_type in type_mapping.items():
            if column in df.columns:
                df[column] = df[column].astype(target_type)
        return df
    except Exception as e:
        logger.error(f"Failed to convert data types: {str(e)}")
        raise

def extract_data_from_postgres(**context) -> None:
    """
    Extract data from PostgreSQL and store in XCom
    """
    pg_hook = get_postgres_connection()
    config = get_table_config()
    
    for table_config in config['tables']:
        table_name = table_config['source_table']
        query = f"SELECT * FROM {table_name}"
        
        try:
            df = pg_hook.get_pandas_df(query)
            context['task_instance'].xcom_push(key=f'data_{table_name}', value=df)
            logger.info(f"Successfully extracted data from {table_name}")
        except Exception as e:
            logger.error(f"Failed to extract data from {table_name}: {str(e)}")
            raise

def transform_data(**context) -> None:
    """
    Transform data according to type mappings
    """
    config = get_table_config()
    
    for table_config in config['tables']:
        table_name = table_config['source_table']
        df = context['task_instance'].xcom_pull(key=f'data_{table_name}')
        
        try:
            df = convert_data_types(df, table_config['type_mapping'])
            context['task_instance'].xcom_push(key=f'transformed_data_{table_name}', value=df)
            logger.info(f"Successfully transformed data for {table_name}")
        except Exception as e:
            logger.error(f"Failed to transform data for {table_name}: {str(e)}")
            raise

def load_to_bigquery(**context) -> None:
    """
    Load transformed data to BigQuery
    """
    bq_hook = get_bigquery_connection()
    config = get_table_config()
    
    for table_config in config['tables']:
        table_name = table_config['source_table']
        df = context['task_instance'].xcom_pull(key=f'transformed_data_{table_name}')
        
        try:
            bq_hook.insert_rows_from_dataframe(
                dataset_id=table_config['bq_dataset'],
                table_id=table_config['bq_table'],
                dataframe=df,
                write_disposition='WRITE_TRUNCATE'
            )
            logger.info(f"Successfully loaded data to BigQuery for {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery for {table_name}: {str(e)}")
            raise

# Define tasks
extract_task = PythonOperator(
    task_id='extract_from_postgres',
    python_callable=extract_data_from_postgres,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task 