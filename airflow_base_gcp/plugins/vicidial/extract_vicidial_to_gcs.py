import gzip
import json
import os
import requests
import tempfile

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from google.cloud import storage
from google.oauth2.service_account import Credentials


class VicidialToGCSOperator(BaseOperator):
    """
    Airflow operator that queries an Vicidial API and saves the response to a Google Cloud Storage bucket.

    :param url: URL of the Vicidial API endpoint to query.
    :type url: str
    :param username: Username for authenticating with the API.
    :type username: str
    :param password: Password for authenticating with the API.
    :type password: str
    :param token: Token for authenticating with the API.
    :type token: str
    :param period_start: Start date of the period to query the API for (in YYYY-MM-DD format).
    :type period_start: str
    :param period_end: End date of the period to query the API for (in YYYY-MM-DD format).
    :type period_end: str
    :param gcs_service_account_credentials: Location of Google Cloud Storage service account credentials
    :type gcs_bucket_name: str
    :param gcs_bucket_name: Name of the GCS bucket to upload the API response to.
    :type gcs_bucket_name: str
    :param gcs_path: Path within the GCS bucket to upload the API response to.
    :type gcs_path: str
    """

    template_fields = ('period_start', 'period_end')

    @apply_defaults
    def __init__(self, url: str, username: str, password: str, token: str, period_start: str, period_end: str,
                 gcs_service_account_credentials: str, gcs_bucket_name: str, gcs_path: str, *args, **kwargs):
        """
        Initializes the operator with the given parameters.
        """
        super().__init__(*args, **kwargs)
        self.url = url
        self.username = username
        self.password = password
        self.token = token
        self.period_start = period_start
        self.period_end = period_end
        self.gcs_service_account_credentials = gcs_service_account_credentials
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_path = gcs_path

    def execute(self, context):
        """
        Executes the operator. Queries the API for the given period and saves the response to a GCS bucket.
        """
        # Authenticate with GCP using service account credentials
        credentials = Credentials.from_service_account_file(self.gcs_service_account_credentials)

        # Query the API and get the response
        payload = "{\"token\":\"" + self.token + "\", \"data_od\": \"" + self.period_start + "\", \"data_do\": \"" + self.period_end + "\" }"
        response = requests.get(self.url, auth=(self.username, self.password), data=payload)
        if response.status_code != 200:
            raise ValueError(f'Request failed with status code {response.status_code}')

        # Save the response to a temporary file
        with tempfile.NamedTemporaryFile(suffix='.json.gz', delete=False) as file:
            with gzip.GzipFile(fileobj=file, mode='w') as gz:
                gz.write(json.dumps(response.json()).encode())
            tmp_file_path = file.name

        # Upload the temporary file to GCS bucket
        storage_client = storage.Client(project=credentials.project_id, credentials=credentials)
        bucket = storage_client.bucket(self.gcs_bucket_name)

        gcs_filename = f'{self.gcs_path}/{self.period_start.replace("-", "/")}/vicidial.json.gz'
        blob = bucket.blob(gcs_filename)
        blob.upload_from_filename(tmp_file_path)

        # Remove the temporary file
        os.remove(tmp_file_path)

