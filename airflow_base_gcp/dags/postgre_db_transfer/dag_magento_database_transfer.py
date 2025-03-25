# This dag transfers data from magento source database to snowflake during the day from monday to friday
# select specific columns definied in special sql folder in this same localization
import datetime
from airflow import DAG
from pathlib import Path
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from transfer_mysql_to_snowflake import TransferMySQLToSnowflakeOperator, TransferMySQLToSnowflakeConfig

def slack_alert(context):
    slack_webhook_token = 'https://hooks.slack.com/services/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'

    hook = SlackWebhookOperator(
        http_conn_id=None,
        webhook_token=slack_webhook_token,
        message="Magento transfer dag failed",
        channel='#data-warehouse-notifications',
        task_id = "slack_alert"
    )
    
    hook.execute(context=context)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 2, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'on_failure_callback': slack_alert
}

transfer_mysql_to_snowflake_configs = [
    TransferMySQLToSnowflakeConfig(table_name='sales_orders', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='sales_orders_item', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='sales_orders_address', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='care_club', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='careclub_coins_history', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='careclub_level_history', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='care_club_account_balance', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='mgm', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='currency_rate', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='sales_order_status_history', dag_directory = Path(__file__).parent.absolute()),
    TransferMySQLToSnowflakeConfig(table_name='mdm_data', dag_directory = Path(__file__).parent.absolute())
]

dag = DAG(
    'magento_database_transfer',
    default_args=default_args,
    schedule_interval="*/45 6-18 * * 1-5",
    catchup=False,
    concurrency = 1
)

for transfer_mysql_to_snowflake_config in transfer_mysql_to_snowflake_configs:
    load_data_task = TransferMySQLToSnowflakeOperator(
        task_id=f"transfer_{transfer_mysql_to_snowflake_config.table_name}",
        mysql_conn_id='MAGENTO_HLC',
        snowflake_conn_id='SNOWFLAKE_HLC',
        snowflake_ddl_query=transfer_mysql_to_snowflake_config.snowflake_ddl_query,
        magento_dml_query=transfer_mysql_to_snowflake_config.magento_dml_query,
        schema="MAGENTO",
        table=transfer_mysql_to_snowflake_config.table_name,
        dag=dag
    )

def failure_alert(context):
    slack_hook = SlackAPIHook(
        token='EifH2ujnrAU1gl2ChlSa5Wxw',  # if using token-based API
        channel='#data-warehouse-notifications',  # adjust this to your channel
        webhook_token='https://hooks.slack.com/services/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'  # if using webhook-based API
    )
    
    slack_msg = {
        'text': f'DAG {context["dag"].dag_id} failed!',
        'attachments': [{
            'color': '#FF0000',
            'title': f"Task {context['ti'].task_id} failed",
            'footer': context['dag'].dag_id,
            'text': context['exception']  # You can customize this further
        }]
    }
    
    slack_hook.send_message(slack_msg)