# PostgreSQL to BigQuery Transfer DAG

This DAG handles the transfer of data from PostgreSQL to BigQuery with data type conversion capabilities.

## Prerequisites

1. Airflow with PostgreSQL and BigQuery providers installed
2. PostgreSQL connection configured in Airflow
3. Google Cloud connection configured in Airflow
4. Required Python packages:
   - pandas
   - apache-airflow-providers-postgres
   - apache-airflow-providers-google

## Setup Instructions

1. Configure Airflow Variables:
   You need to set up the following variables in Airflow (Admin -> Variables):
   
   PostgreSQL Connection Variables:
   - `postgres_host`: Your PostgreSQL server hostname
   - `postgres_schema`: Your database name
   - `postgres_user`: Your database username
   - `postgres_password`: Your database password
   - `postgres_port`: Your database port (default: 5432)

   Google Cloud Variables:
   - `gcp_project_id`: Your Google Cloud project ID

   Table Configuration:
   - `postgres_to_bigquery_config`: JSON configuration for table mappings
     ```json
     {
         "tables": [
             {
                 "source_table": "your_source_table",
                 "bq_dataset": "your_bigquery_dataset",
                 "bq_table": "your_bigquery_table",
                 "type_mapping": {
                     "column_name": "target_data_type"
                 }
             }
         ]
     }
     ```

2. Configure Airflow Connections:
   - PostgreSQL Connection:
     - Connection ID: `postgres_default`
     - Connection Type: PostgreSQL
     - Host: Your PostgreSQL host
     - Schema: Your database name
     - Login: Your username
     - Password: Your password
     - Port: Your PostgreSQL port

   - Google Cloud Connection:
     - Connection ID: `google_cloud_default`
     - Connection Type: Google Cloud
     - Project ID: Your GCP project ID
     - Keyfile JSON: Your service account key JSON

3. Configure Table Mappings:
   You can either:
   a. Set the configuration in Airflow Variables as JSON
   b. Use the default configuration file at `config/postgres_to_bigquery_config.json`

## Usage

1. The DAG will appear in your Airflow UI as `postgres_to_bigquery_transfer`
2. You can trigger it manually or set up a schedule
3. The DAG will:
   - Extract data from PostgreSQL
   - Convert data types according to the mapping
   - Load the transformed data to BigQuery

## Data Type Mappings

Common data type mappings:
- PostgreSQL INTEGER -> BigQuery INT64
- PostgreSQL VARCHAR/TEXT -> BigQuery STRING
- PostgreSQL TIMESTAMP -> BigQuery DATETIME
- PostgreSQL DECIMAL/NUMERIC -> BigQuery FLOAT64
- PostgreSQL BOOLEAN -> BigQuery BOOL

## Error Handling

- The DAG includes comprehensive error handling and logging
- Failed tasks will be retried once after 5 minutes
- All errors are logged with detailed messages

## Monitoring

Monitor the DAG execution in the Airflow UI:
- Task status
- Logs
- XCom values (for debugging)
- Task duration
- Success/failure rates

## Security Considerations

1. All sensitive credentials are stored in Airflow Variables
2. Database passwords and service account keys should be encrypted
3. Use Airflow's built-in encryption for sensitive variables
4. Follow the principle of least privilege for database users
5. Regularly rotate credentials and service account keys 