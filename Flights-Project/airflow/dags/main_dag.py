from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import time

from google.cloud import storage
import requests
from datetime import datetime, timedelta, timezone

import json

def load_config(config_path="config.json"):
    """Load configuration from a JSON file."""
    with open(config_path, "r") as f:
        return json.load(f)

config = load_config()


def fetch_interval(start_time, end_time, type_):
    """Fetch data for a specific time interval."""
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    
    url = f"https://opensky-network.org/api/flights/{type_}?airport=LLBG&begin={start_ts}&end={end_ts}"
    username = config['opensky']['username']
    password = config['opensky']['password']
    
    response = requests.get(url, auth=(username, password))
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def fetch_longer_range(start_time, end_time, type_, interval_hours=24):
    """Fetch data for a range longer than the API allows."""
    current_start = start_time
    all_data = []

    while current_start < end_time:
        current_end = min(current_start + timedelta(hours=interval_hours), end_time)
        data = fetch_interval(current_start, current_end, type_)
        if data:
            all_data.extend(data)
        current_start = current_end
    
    return all_data


def write_gcs(path):
    """Upload json file to Google Cloud Storage"""
    storage_client = storage.Client.from_service_account_json(config['gcs']['credentials_path'])
    bucket_name = config['gcs']['bucket_name']
    
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(path.name)
        blob.upload_from_filename(str(path))
        print(f"File {path.name} uploaded to {bucket_name}")
    except Exception as e:
        print(f"Error uploading file to GCS: {str(e)}")

def web_to_gcs():
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=365)
    for type_ in ['arrival', 'departure']:
        flight_data = fetch_longer_range(start_time, end_time, type_, interval_hours=24)

        output_path = Path(f"{type_}.json")
        with output_path.open('w') as f:
            json.dump(flight_data, f)
        
        write_gcs(output_path)
        
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

def load_config(config_path="config.json"):
    """Load configuration from a JSON file."""
    with open(config_path, "r") as f:
        return json.load(f)

# Load configuration
config = load_config()

def gcs_to_bq():
    """Load data from Google Cloud Storage to BigQuery using config."""
    # Load configurations from the config file
    credentials_path = config['bigquery']['credentials_path']
    project_id = config['bigquery']['project_id']
    dataset_id = config['bigquery']['dataset_id']
    bucket_name = config['gcs']['bucket_name']
    folder_name = config['gcs'].get('folder_name', 'analysis_outputs')  # Default folder name

    # Create credentials object with the required scopes
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    # Initialize BigQuery client with credentials
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id
    )

    # Create dataset reference
    dataset_ref = f"{project_id}.{dataset_id}"
    
    # Create dataset if it doesn't exist
    try:
        client.get_dataset(dataset_ref)  # Try fetching dataset
        print(f"Dataset {dataset_ref} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)  # Create dataset if not found
        print(f"Created dataset {dataset_ref}")

    # Define files and table names from the config
    files_and_tables = config.get('bigquery', {}).get('files_and_tables', {})
    
    # Load each file into BigQuery
    for file_name, table_name in files_and_tables.items():
        try:
            # Define the source URI in GCS
            uri = f"gs://{bucket_name}/{folder_name}/{file_name}"
            
            # Define the full table ID in BigQuery
            table_id = f"{project_id}.{dataset_id}.{table_name}"
            
            print(f"Loading {uri} to {table_id}")
            
            # Configure the loading job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            # Start the loading job
            load_job = client.load_table_from_uri(
                uri,
                table_id,
                job_config=job_config
            )
            
            # Wait for the job to complete
            load_job.result()
            
            print(f"Successfully loaded {uri} into {table_id}")
            
        except Exception as e:
            print(f"Error loading {file_name}: {str(e)}")


def delay_task():
    time.sleep(5 * 60)

# DAG definition
dag = DAG(
    'flight_data_to_gcs_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='10 12 * * *',  # Run daily at 23:00
    catchup=False
)

# Create PythonOperator tasks
get_raw_data_to_gcs = PythonOperator(
    task_id='fetch_and_upload_arrival_data',
    python_callable=web_to_gcs,
    dag=dag
)

get_gcs_data_to_bq = PythonOperator(
    task_id='get_gcs_data_to_bq_task',
    python_callable=gcs_to_bq,
    dag=dag
)

delay_task_operator = PythonOperator(
    task_id='delay_5_minutes',
    python_callable=delay_task,
    dag=dag
)

get_raw_data_to_gcs >> delay_task_operator >> get_gcs_data_to_bq