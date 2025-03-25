# This dag transfers data from bigquery database to snowflake
# select specific columns definied in special sql folder in this same localization
# first it takes data from bigquery date table and load it into Cloud Storage
# next load it incrementally from cloud storage into snowflake
# created for fixing errors purposes
import datetime

from google.cloud import bigquery

from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from airflow.operators.python_operator import PythonOperator

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider.sensors.fivetran import FivetranSensor



default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 2, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    'google_analytics_manual',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    max_active_runs=1,
    catchup=False
)
def google_analytics_extract(run_date):
    # Authenticate with Google Cloud using a service account key
    gcs_service_account_credentials=Variable.get("GOOGLE_ANALYTICS_GCP_SERVICE_ACCOUNT")
    client = bigquery.Client.from_service_account_json(gcs_service_account_credentials)

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False

    table_name = f'events_{run_date.replace("-", "")}'
    partition_name = run_date.replace("-", "/")

    # Define the BigQuery SQL query to retrieve the data
    query = f"""
    EXPORT DATA
        OPTIONS (
            uri = 'gs://healthlabs-datateam-prod-datalake-raw/google_analytics/{partition_name}/*.parquet',
            format = 'Parquet',
            overwrite = true,
            compression = 'snappy'
        )
    AS (
        SELECT * FROM `bigquery-healthlabs.analytics_259872015.{table_name}`
        )
    """

    # Run the query and export the results to GCS
    query_job = client.query(query, job_config=job_config)
    query_job.result()

run_date = '{{ macros.ds_add(ds, -1) }}'

raw_table_id = "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}"
table_id = f'events_{raw_table_id}'

# Google Analytics tasks

google_analytics_group = TaskGroup(dag=dag, group_id="google_analytics_group", ui_color="CornflowerBlue")

check_google_analytics_table_existence = BigQueryTableExistenceSensor(
    dag=dag,
    task_id='check_google_analytics_table_existence',
    project_id='bigquery-healthlabs',
    dataset_id='analytics_259872015',
    table_id=table_id,
    gcp_conn_id='BIGQUERY_HLC',
    task_group=google_analytics_group
)

extract_google_analytics = PythonOperator(
    task_id='extract_google_analytics',
    python_callable=google_analytics_extract,
    dag=dag,
    op_args=[run_date],
    task_group=google_analytics_group
)