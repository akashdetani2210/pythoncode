import boto3
import  datetime
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.gcp_transfer_operator import S3ToGoogleCloudStorageTransferOperator

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from io import BytesIO

default_dag_args = {
      'retries': 2,
      'retry_delay': datetime.timedelta(seconds=45),
      'execution_timeout': datetime.timedelta(hours=1),
      'start_date': days_ago(0),
      'schedule_interval': None,
      'email': 'detani@google.com'
}

#Read Airflow Variable
config = Variable.get("bioinf_notification_table", deserialize_json=True)

#Bigquery Variables
bigquery_project = 'tl-mefp16cymab6ifvbsydq'
reference_table = 's3_object_inventory'
reference_dataset = 'bioinfo_alp'
REFERENCE_TABLE_SCHEMA = [
    {"name": "kind", "type": "STRING"},
    {"name": "id", "type": "STRING"},
    {"name": "selfLink", "type": "STRING"},
    {"name": "name", "type": "STRING"},
    {"name": "bucket", "type": "STRING"},
    {"name": "generation", "type": "STRING"},
    {"name": "metageneration", "type": "STRING"},
    {"name": "contentType", "type": "STRING"},
    {"name": "timeCreated", "type": "STRING"},
    {"name": "updated", "type": "STRING"},
    {"name": "storageClass", "type": "STRING"},
    {"name": "timeStorageClassUpdated", "type": "STRING"},
    {"name": "size", "type": "STRING"},
    {"name": "md5Hash", "type": "STRING"},
    {"name": "mediaLink", "type": "STRING"},
    {"name": "crc32c", "type": "STRING"},
    {"name": "etag", "type": "STRING"}]

def create_big_query_table():

    bq_hook = BigQueryHook(bigquery_conn_id='bigquery_default', use_legacy_sql=False)

    gcp_credentials = bq_hook._get_credentials()

    bq_client = bigquery.Client(credentials=gcp_credentials, project=bigquery_project)

    target_dataset_ref = bigquery.DatasetReference(project=bigquery_project, dataset_id=reference_dataset)

    try:
        target_dataset = bq_client.get_dataset(dataset_ref=target_dataset_ref)
    except NotFound as ex:
        # LOGGER.info(f"Dataset '{target_dataset_ref}' not found, attempting to create.")
        target_dataset = bq_client.create_dataset(dataset=target_dataset_ref)
    target_table_ref = bigquery.TableReference(dataset_ref=target_dataset, table_id=reference_table)

    
    target_table = bq_client.delete_table(table=target_table_ref)
    

#Start Tasks
with models.DAG('create_notification_table',
                max_active_runs=1,
                default_args=default_dag_args) as dag:

    create_big_query_table = PythonOperator(task_id="create_notification_table", python_callable=create_big_query_table)
                                    
    #Dag creation
    create_big_query_table