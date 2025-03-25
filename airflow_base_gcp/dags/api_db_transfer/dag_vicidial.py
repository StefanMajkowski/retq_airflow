# This dag transfers data from vicidial source database to snowflake
# select specific columns definied in special sql folder in this same localization
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from extract_vicidial_to_gcs import VicidialToGCSOperator

def slack_alert(context):
    slack_webhook_token = 'https://hooks.slack.com/services/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'

    hook = SlackWebhookOperator(
        http_conn_id=None,
        webhook_token=slack_webhook_token,
        message="Vicidial transfer dag failed",
        channel='#data-warehouse-notifications',
        task_id = "slack_alert"
    )
    
    hook.execute(context=context)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 10, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'depends_on_past': True,
    'on_failure_callback': slack_alert
}

dag = DAG(
    'vicidial_transfer2',
    default_args=default_args,
    schedule_interval="*/120 6-16 * * *",
    catchup=True,
    max_active_runs=1
)

extract_data_endpoint_1 = VicidialToGCSOperator(
    task_id=f"extract_data_endpoint_1",
    url='https://apps.net-tel.it/healthlabsapi/raportAnalityczny1',
    username=Variable.get("SECRET_VICIDIAL_USER"),
    password=Variable.get("SECRET_VICIDIAL_PASSWORD"),
    token=Variable.get("SECRET_VICIDIAL_TOKEN"),
    gcs_service_account_credentials=Variable.get("VICIDIAL_GCP_SERVICE_ACCOUNT"),
    gcs_bucket_name='healthlabs-datateam-prod-datalake-raw',
    gcs_path='vicidial/raportanalityczny1',
    period_start='{{ ds }}',
    period_end='{{ ds }}',
    dag=dag
)

extract_data_endpoint_2 = VicidialToGCSOperator(
    task_id=f"extract_data_endpoint_2",
    url='https://apps.net-tel.it/healthlabsapi/raportAnalityczny2',
    username=Variable.get("SECRET_VICIDIAL_USER"),
    password=Variable.get("SECRET_VICIDIAL_PASSWORD"),
    token=Variable.get("SECRET_VICIDIAL_TOKEN"),
    gcs_service_account_credentials=Variable.get("VICIDIAL_GCP_SERVICE_ACCOUNT"),
    gcs_bucket_name='healthlabs-datateam-prod-datalake-raw',
    gcs_path='vicidial/raportanalityczny2',
    period_start='{{ ds }}',
    period_end='{{ ds }}',
    dag=dag
)
