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
import boto3
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.contrib.operators.gcp_transfer_operator import S3ToGoogleCloudStorageTransferOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook



default_dag_args = {
      'retries': 2,
      'retry_delay': datetime.timedelta(seconds=45),
      'execution_timeout': datetime.timedelta(hours=1),
      'start_date': days_ago(1),
      'schedule_interval': '*/5 * * * *',
      'email': 'detani@google.com'
}

#Read Airflow Variable
config = Variable.get("bioInfo_s3_to_gcs_config", deserialize_json=True)


#AWS Variables
s3_bucket = config["s3_bucket"]

#GCS Variables
gcs_bucket = config["gcs_bucket"]

#Function to list the s3 folders and transfer the objects by creating multiple task based on different files/folders.
def s3_to_gcs():
    tasks = []
    file_list = set()
    ACCESS_KEY_ID = Variable.get(key="ACCESS_KEY_ID")
    SECRET_ACCESS_KEY = Variable.get(key="SECRET_ACCESS_KEY")

    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY
    )

    s3 = session.client('s3')
    bucket = s3.list_objects_v2(Bucket=s3_bucket)
    
    for obj in bucket['Contents']:
    #     if obj['Size']>0:
        file_list.add(obj['Key'].split('/')[0])

    # file_list_1 = ['ACL']

    for folder in file_list:
        task_id = f"load_from_S3_{folder}"
        new_task = S3ToGoogleCloudStorageTransferOperator(
                                        aws_conn_id='aws_default',
                                        task_id=task_id,
                                        s3_bucket=s3_bucket,
                                        gcs_bucket=gcs_bucket,
                                        description= f"Transfer unloaded data from S3 for {folder}",
                                        object_conditions={ 'include_prefixes': [ folder ] },
                                        timeout=60,
                                        wait=1
                                        )
        tasks.append(new_task)
    return tasks

#Start Tasks
with models.DAG('s3_to_gcs_folder_task',
                max_active_runs=1,
                default_args=default_dag_args) as dag:

    # folder_list = PythonOperator(task_id="s3_hook", python_callable=s3_folders)

    s3_to_gcs = s3_to_gcs()

                                    

    #Dag creation
    s3_to_gcs