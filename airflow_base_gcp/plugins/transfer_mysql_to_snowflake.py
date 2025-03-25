from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from snowflake.connector.pandas_tools import pd_writer

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class TransferMySQLToSnowflakeConfig:
    """
    This class represents the configuration information required for transferring data from a MySQL database
    to a Snowflake database.
    Attributes:
        table_name (str): The name of the table being transferred.
        dag_directory (Path): The directory where the DAG files for Airflow are located.
        magento_dml_query (Optional[str]): The SQL query used to extract data from the MySQL database. If not provided,
            the query is read from the file located at 'dag_directory/sql/{table_name}.sql'.
        snowflake_ddl_query (Optional[str]): The SQL query used to create the table in the Snowflake database. If not
            provided, the query is read from the file located at 'dag_directory/sql/ddl/{table_name}.sql'.
    """
    table_name: str
    dag_directory: Path
    magento_dml_query: Optional[str] = None
    snowflake_ddl_query: Optional[str] = None

    def __post_init__(self):
        """
        A special method in Python that is called after the object has been initialized. It is used to set the default
        values for magento_dml_query and snowflake_ddl_query if they are not provided.
        """
        if self.magento_dml_query is None:
            self.magento_dml_query = open(self.dag_directory / "sql" / f"{self.table_name}.sql", "r").read()
        if self.snowflake_ddl_query is None:
            self.snowflake_ddl_query = open(self.dag_directory / "sql" / "ddl" / f"{self.table_name}.sql", "r").read()


class TransferMySQLToSnowflakeOperator(BaseOperator):
    """
    Airflow operator to execute a provided SQL statement in a MySQL database and load data into a specified Snowflake table.
    :param mysql_conn_id: The connection id for the MySQL database.
    :type mysql_conn_id: str
    :param magento_dml_query: The SQL query used to extract data from the MySQL database.
    :type magento_dml_query: str
    :param snowflake_ddl_query: The SQL query used to create the table in the Snowflake database.
    :type snowflake_ddl_query: str
    :param snowflake_conn_id: The connection id for the Snowflake database.
    :type snowflake_conn_id: str
    :param schema: The schema for the destination Snowflake table.
    :type schema: str
    :param table: The name of the destination Snowflake table.
    :type table: str
    """

    template_fields = ('magento_dml_query')

    def __init__(self, mysql_conn_id: str, magento_dml_query: str, snowflake_ddl_query: str, snowflake_conn_id: str,
                 schema: str, table: str, **kwargs):
        super().__init__(**kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.magento_dml_query = magento_dml_query
        self.snowflake_ddl_query = snowflake_ddl_query
        self.snowflake_conn_id = snowflake_conn_id
        self.schema = schema
        self.table = f"src_magento__{table}"

    def execute(self, context):
        # create a MySqlHook using the specified connection
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        # execute the SQL statement in MySQL and fetch the results
        results = mysql_hook.get_pandas_df(sql=self.magento_dml_query)


            # create a SnowflakeHook using the specified connection
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        if context['dag_run'].conf.get('full_refresh'):
            snowflake_hook.run(sql=f'DROP TABLE IF EXISTS {self.schema}.{self.table};')

        snowflake_hook.run(sql=self.snowflake_ddl_query)

        snowflake_engine = snowflake_hook.get_sqlalchemy_engine()

        # load the data into the specified Snowflake table
        with snowflake_engine.connect() as connection:
            results.to_sql(name=self.table,
                        schema=self.schema,
                        con=connection,
                        if_exists='append',
                        method=pd_writer,
                        index=False
                        )
