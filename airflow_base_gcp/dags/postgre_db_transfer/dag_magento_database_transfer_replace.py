# This dag transfers data from magento source database to snowflake, created for problems fixind purposes
# select specific columns definied in special sql folder in this same localization
import datetime
from airflow import DAG
from pathlib import Path

from transfer_mysql_to_snowflake import TransferMySQLToSnowflakeOperator, TransferMySQLToSnowflakeConfig

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 2, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

transfer_mysql_to_snowflake_configs = [
    TransferMySQLToSnowflakeConfig(table_name='product_mapping', dag_directory = Path(__file__).parent.absolute())
]

dag = DAG(
    'magento_database_transfer_replace',
    default_args=default_args,
    schedule_interval="*/1440 * * * *",
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