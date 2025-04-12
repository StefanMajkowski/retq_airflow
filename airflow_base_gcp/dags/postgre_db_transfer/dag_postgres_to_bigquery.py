from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
import os
import tempfile
from dotenv import load_dotenv
from sqlalchemy import create_engine
from google.cloud import bigquery

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'credentials', '.env')
logger.info(f"Loading environment variables from: {env_path}")
load_dotenv(env_path)

# Set GCP credentials path - try to use key file if it exists
gcp_key_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'credentials', 'decent-genius-449819-r8-d57651c29403.json')
if os.path.exists(gcp_key_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_key_path
    logger.info(f"Set GOOGLE_APPLICATION_CREDENTIALS to: {gcp_key_path}")
else:
    logger.info(f"GCP key file not found at {gcp_key_path}, will use Airflow connection instead")

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
    description='Transfer multiple tables from PostgreSQL to BigQuery',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['postgres', 'bigquery', 'data_transfer'],
)

def transfer_postgres_to_bigquery(source_table=None, query=None, columns=None, bq_dataset=None, bq_table=None, **context):
    """
    Extract data from PostgreSQL and load directly to BigQuery
    
    Parameters:
    - source_table: Name of the table to extract (not needed if query is provided)
    - query: Custom SQL query to extract data (takes precedence over source_table)
    - columns: List of columns to extract (used only with source_table, not with query)
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
        
        # Create BigQuery hook with a more compatible approach
        bq_hook = BigQueryHook(
            gcp_conn_id='google_cloud_default',
            project_id=bq_project_id,
            use_legacy_sql=False,
            location='europe-central2'
        )
        
        # Set schema if provided
        schema_prefix = f"{pg_schema}." if pg_schema else ""
        
        # Determine SQL query and table name
        if query:
            # Use custom query
            sql_query = query
            # For custom query, table name is used only for logging and default BigQuery table name
            table_name = source_table or "custom_query"
            logger.info(f"Executing custom query: {sql_query}")
        else:
            # Use source_table with optional columns
            if not source_table:
                raise ValueError("Either source_table or query must be provided")
                
            columns_str = ", ".join(columns) if columns else "*"
            sql_query = f"SELECT {columns_str} FROM {schema_prefix}{source_table}"
            table_name = source_table
            logger.info(f"Transferring table: {table_name}")
            logger.info(f"Columns: {columns_str if columns else 'All columns'}")
        
        # Set BigQuery destination
        bq_dataset_name = bq_dataset or "default_dataset"
        bq_table_name = bq_table or table_name
        
        # Extract data from PostgreSQL
        logger.info(f"Executing query against PostgreSQL")
        df = pd.read_sql(sql_query, engine)
        logger.info(f"Retrieved {len(df)} rows from PostgreSQL")
        
        # Create a temporary file to store the data
        with tempfile.NamedTemporaryFile(suffix='.csv') as temp_file:
            # Write the DataFrame to a CSV file
            df.to_csv(temp_file.name, index=False)
            logger.info(f"Data written to temporary file: {temp_file.name}")
            
            # Load data to BigQuery using the load_table_from_file method
            client = bigquery.Client(project=bq_project_id)
            
            # Define the table reference
            table_ref = f"{bq_project_id}.{bq_dataset_name}.{bq_table_name}"
            logger.info(f"Loading data to BigQuery table: {table_ref}")
            
            # Define the job config
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip the header row
                autodetect=True,      # Automatically detect schema
                write_disposition='WRITE_TRUNCATE'  # Overwrite the table if it exists
            )
            
            # Load the data from the temp file
            with open(temp_file.name, "rb") as source_file:
                job = client.load_table_from_file(
                    source_file, 
                    table_ref, 
                    job_config=job_config
                )
            
            # Wait for the job to complete
            job.result()
            
            logger.info(f"Successfully transferred {len(df)} rows to {bq_dataset_name}.{bq_table_name}")
    
    except Exception as e:
        logger.error(f"Error in transfer process: {str(e)}")
        raise

def transfer_multiple_tables(**context):
    """
    Transfer multiple tables from PostgreSQL to BigQuery
    """
    # Define the tables to transfer with their configurations
    tables_to_transfer = [
        {
            'source_table': 'client_profiles',
            'bq_dataset': 'BRONZE',
            'bq_table': 'raw_client_client_profiles'
        },
        {
            'source_table': 'teams',
            'bq_dataset': 'BRONZE',
            'bq_table': 'raw_client_teams'
        },
        {
            'source_table': 'client_teams',
            'bq_dataset': 'BRONZE',
            'bq_table': 'raw_client_client_teams'
        },
        {
            'source_table': 'team_invitations',
            'bq_dataset': 'BRONZE',
            'bq_table': 'raw_client_team_invitations'
        }
        # Add more tables as needed
    ]
    
    success_count = 0
    failure_count = 0
    
    # Process each table
    for table_config in tables_to_transfer:
        try:
            logger.info(f"Starting transfer for table: {table_config['source_table']}")
            transfer_postgres_to_bigquery(**table_config)
            success_count += 1
            logger.info(f"Successfully transferred table: {table_config['source_table']}")
        except Exception as e:
            failure_count += 1
            logger.error(f"Failed to transfer table {table_config['source_table']}: {str(e)}")
            # Continue processing other tables even if one fails
            continue
    
    logger.info(f"Transfer process completed. Successfully transferred {success_count} of {len(tables_to_transfer)} tables.")
    if failure_count > 0:
        logger.warning(f"Failed to transfer {failure_count} tables. See logs for details.")
    
    return {
        'success_count': success_count,
        'failure_count': failure_count,
        'total_tables': len(tables_to_transfer)
    }

# Define task for transferring multiple tables
multi_table_transfer_task = PythonOperator(
    task_id='transfer_multiple_tables',
    python_callable=transfer_multiple_tables,
    dag=dag,
) 