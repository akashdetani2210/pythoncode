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

import datetime
import json
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.contrib.operators.s3_list_operator import S3ListOperator
import os
import logging
from airflow.models import Variable


default_dag_args = {
      'retries': 2,
      'retry_delay': datetime.timedelta(seconds=45),
      'execution_timeout': datetime.timedelta(hours=1),
      'start_date': days_ago(0),
      'schedule_interval': None,
      'email': 'detani@google.com'
}


DAG_ID = "s3_to_gcs_migration_dags"

# Configuration is set here, but pushed into an Airflow variable using Variable.set to enable
# templating.
s3_to_gcs_config = {
  "s3_bucket": "pipeline-refdata",
  "gcs_bucket": "tl-alp-storage-transfer"
}

DAG_TEMPLATE = """\
## This file is auto generated! Do not edit.
import datetime
from airflow.models import DAG
from airflow.utils.dates import days_ago
from migrate_table import create_dag
DEFAULT_DAG_ARGS = {{
      'retries': 2,
      'retry_delay': datetime.timedelta(seconds=30),
      'execution_timeout': datetime.timedelta(hours=1),
      'start_date': days_ago(0),
      'schedule_interval': 'None',
      'email': 'detani@google.com'
}}

folder = "{folder}"

dag_id = "migrateV2_" + folder

with DAG(dag_id, default_args=DEFAULT_DAG_ARGS) as dag:
    create_dag(dag, folder)
"""

#setting variables in airflow
def save_config_variable():
    Variable.set(key="s3_to_gcs_config", value=json.dumps(s3_to_gcs_config))
    
##Read Airflow Variable
config = s3_to_gcs_config
s3_bucket=config["s3_bucket"]
gcs_bucket=config["gcs_bucket"]

#generating dags
def generate_dags(folders=None, dag_bucket=None, dag_folder=None):

    for i, folder in enumerate(folders):
        new_dag = DAG_TEMPLATE.format(folder=folder)
        dag_filename = f"auto_generated_migrate_{folder}.py"
        dag_path = os.path.join(dag_folder, dag_filename)
        logging.info(f"Write DAG: '{new_dag}'")
        logging.info(f"Write new DAG to {dag_bucket} at {dag_path}")
        hook = GCSHook()
        hook.upload(bucket_name=dag_bucket, object_name=dag_path, data=new_dag)

with models.DAG(
      dag_id=DAG_ID,
      max_active_runs=1,
      default_args=default_dag_args) as dag:
    
    start = PythonOperator(task_id="start_task", python_callable=save_config_variable)
    
    #listing folders from s3 bucket and creating dags based on folder prefix
    folder_list = S3ListOperator(
        task_id='list_s3_folders',
        bucket=s3_bucket
        )
        
    list_folders = folder_list.execute(None)
    file_list = set()
    for i in list_folders:
        file_list.add(i.split('/')[0])
        
    
    logging.info(f"List of folders in s3 bucket: '{file_list}'")
        
    generate = PythonOperator(
          task_id="generate_dag_files",
          task_concurrency=1,
          python_callable=generate_dags,
          op_kwargs={
                "folders": list(file_list),
                "dag_bucket": gcs_bucket,
                "dag_folder": "dags"
          })
          
    # Arrange the DAG
    start >> folder_list >> generate