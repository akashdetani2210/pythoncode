# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import boto3
import  datetime
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from io import BytesIO
from google.cloud import storage

default_dag_args = {
      'retries': 2,
      'retry_delay': datetime.timedelta(seconds=45),
      'execution_timeout': datetime.timedelta(hours=1),
      'start_date': days_ago(0),
      'schedule_interval': None,
      'email': 'detani@google.com'
}

#Read Airflow Variable
config = Variable.get("gcs_inventory_to_bq", deserialize_json=True)

#GCS Variables
gcs_bucket = config["gcs_bucket"]

#Bigquery Variables

bigquery_project = config["project_id"]
reference_table = config["bq_table"]
reference_dataset = config["bq_dataset"]

#Bigquery Table Schema
REFERENCE_TABLE_SCHEMA = [{"name":"timestamp", "type":"TIMESTAMP", "mode":"NULLABLE"},
                            {"name": "gcs_data", "type": "RECORD", "mode": "REPEATED",
                            "fields":
                                [{"name": "gcs_bucket", "type":"STRING"}, {"name": "object_key", "type":"STRING"},{"name": "object_size", "type":"STRING"}] }]

#Function to create and insert values in bigquery table
def save_hash_reference(input_rows, project_id=None, dataset=None, table=None, schema=None):

    bq_hook = BigQueryHook(bigquery_conn_id='bigquery_default', use_legacy_sql=False)

    gcp_credentials = bq_hook._get_credentials()

    bq_client = bigquery.Client(credentials=gcp_credentials, project=project_id)

    target_dataset_ref = bigquery.DatasetReference(project=project_id, dataset_id=dataset)

    try:
        target_dataset = bq_client.get_dataset(dataset_ref=target_dataset_ref)
    except NotFound as ex:
        # LOGGER.info(f"Dataset '{target_dataset_ref}' not found, attempting to create.")
        target_dataset = bq_client.create_dataset(dataset=target_dataset_ref)
    
    target_table_ref = bigquery.TableReference(dataset_ref=target_dataset, table_id=table)

    try:
        target_table = bq_client.get_table(table=target_table_ref)
        print("Table found: ",target_table)
    except NotFound as ex:
        print("Table not found")
        t = bigquery.Table(table_ref=target_table_ref, schema=schema)
        target_table = bq_client.create_table(table=t)
        print("Table created: ",target_table)

    insert_rows = {
        "timestamp": datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f'),
        "gcs_data" : input_rows
    }
    print("Rows to insert: ", input_rows)
    print("Target Table: ", target_table)
    error = bq_client.insert_rows_json(table=target_table, json_rows=[insert_rows])
    print(error)
    

def gcs_object_inventory():
    
    client = storage.Client()

    insert_rows = []
    for blob in client.list_blobs(gcs_bucket):
        # if obj['Size']>0:
        insert_rows.append({"gcs_bucket":str(blob.bucket.name), "object_key":str(blob.name), "object_size":str(blob.size)})

    save_hash_reference(insert_rows, project_id=bigquery_project, dataset=reference_dataset, table=reference_table, schema=REFERENCE_TABLE_SCHEMA)

#Start Tasks
with models.DAG('gcs_objinv',
                max_active_runs=1,
                default_args=default_dag_args) as dag:
                    

    gcs_object_inventory = PythonOperator(task_id="gcs_list_task", python_callable=gcs_object_inventory)
    
    
    #Dag creation
    gcs_object_inventory