# Credentials Setup Guide

This directory contains sensitive credentials and configuration files. Follow these steps to set up your credentials:

## 1. Service Account Setup
1. Place your GCP service account JSON key file in this directory
2. Name it `gcp-service-account.json`
3. Make sure this file is in your `.gitignore` (it should be by default)

## 2. Airflow Variables Setup
Set the following variables in your Airflow environment:

```bash
# PostgreSQL Variables
postgres_host=<your-postgres-host>
postgres_schema=<your-database-name>
postgres_user=<your-username>
postgres_password=<your-password>
postgres_port=5432  # Optional, defaults to 5432

# GCP Variables
gcp_project_id=<your-gcp-project-id>

# Table Configuration (JSON format)
postgres_to_bigquery_config={
    "tables": [
        {
            "source_table": "your_source_table",
            "bq_dataset": "your_bigquery_dataset",
            "bq_table": "your_bigquery_table",
            "type_mapping": {
                "column1": "string",
                "column2": "integer"
            }
        }
    ]
}
```

## 3. Airflow Connections Setup
Set up the following connections in your Airflow environment:

### PostgreSQL Connection
- Connection ID: `postgres_default`
- Connection Type: `Postgres`
- Host: `<your-postgres-host>`
- Schema: `<your-database-name>`
- Login: `<your-username>`
- Password: `<your-password>`
- Port: `5432`

### Google Cloud Connection
- Connection ID: `google_cloud_default`
- Connection Type: `Google Cloud`
- Project ID: `<your-gcp-project-id>`
- Keyfile Path: `/path/to/credentials/gcp-service-account.json`
- Scopes: `https://www.googleapis.com/auth/cloud-platform`

## Security Notes
- Never commit credentials to version control
- Use Airflow's built-in encryption for sensitive values
- Keep your service account key secure and rotate it regularly
- Use the minimum required permissions for your service account 