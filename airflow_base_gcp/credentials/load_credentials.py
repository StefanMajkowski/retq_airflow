import os
from dotenv import load_dotenv
from airflow.models import Variable
from airflow.models import Connection
from airflow.settings import Session

def load_env_to_airflow():
    """
    Load environment variables from .env file into Airflow variables and connections
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Create a session
    session = Session()
    
    # Load variables
    variables = {
        'postgres_host': os.getenv('POSTGRES_HOST'),
        'postgres_schema': os.getenv('POSTGRES_SCHEMA'),
        'postgres_user': os.getenv('POSTGRES_USER'),
        'postgres_password': os.getenv('POSTGRES_PASSWORD'),
        'postgres_port': os.getenv('POSTGRES_PORT'),
        'gcp_project_id': os.getenv('GCP_PROJECT_ID'),
        'postgres_to_bigquery_config': os.getenv('POSTGRES_TO_BIGQUERY_CONFIG')
    }
    
    # Set variables in Airflow
    for key, value in variables.items():
        if value:
            Variable.set(key, value)
            print(f"Set variable: {key}")
    
    # Set up PostgreSQL connection
    postgres_conn = Connection(
        conn_id='postgres_default',
        conn_type='postgres',
        host=os.getenv('POSTGRES_HOST'),
        schema=os.getenv('POSTGRES_SCHEMA'),
        login=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        port=int(os.getenv('POSTGRES_PORT', '5432'))
    )
    
    # Set up Google Cloud connection
    gcp_conn = Connection(
        conn_id='google_cloud_default',
        conn_type='google_cloud_platform',
        project_id=os.getenv('GCP_PROJECT_ID'),
        keyfile_path=os.path.join(os.path.dirname(__file__), 'gcp-service-account.json'),
        scopes='https://www.googleapis.com/auth/cloud-platform'
    )
    
    # Add or update connections
    for conn in [postgres_conn, gcp_conn]:
        if session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
            session.query(Connection).filter(Connection.conn_id == conn.conn_id).update(conn.__dict__)
        else:
            session.add(conn)
    
    session.commit()
    session.close()
    print("Successfully loaded all credentials into Airflow")

if __name__ == "__main__":
    load_env_to_airflow()