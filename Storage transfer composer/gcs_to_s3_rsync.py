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

import airflow
import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


default_dag_args = {
      'retries': 2,
      'retry_delay': datetime.timedelta(seconds=45),
      'execution_timeout': datetime.timedelta(hours=1),
      'start_date': days_ago(0),
      'schedule_interval': None,
      'email': 'detani@google.com'
}



#Read Airflow Variable
config = Variable.get("gcs_to_s3_sync", deserialize_json=True)


#Reading gcs variable
s3_path = config["s3_path"]
gcs_path = config["gcs_path"]
folder = config["folder"]


bash_env = {
        "AWS_ACCESS_KEY_ID": "{{var.value.access_id}}",
        "AWS_SECRET_ACCESS_KEY": "{{var.value.secret_id}}"
}

rsync_command = '''
    set -e;
    export AWS_ACCESS_KEY_ID="%s";
    export AWS_SECRET_ACCESS_KEY="%s";
''' %(bash_env.get('AWS_ACCESS_KEY_ID'), bash_env.get('AWS_SECRET_ACCESS_KEY')) \
+  '''
    gsutil -m rsync -r gs://{gcs_path}/{folder}/ s3://{s3_path}/
''' .format(gcs_path=gcs_path, folder=folder, s3_path=s3_path)

#Start Tasks
with models.DAG('rsync_gcs_to_s3',
                max_active_runs=1,
                default_args=default_dag_args) as dag:


    s3_sync = BashOperator(
        task_id='gsutil_s3_gcp_sync',
        bash_command=rsync_command,
        dag=dag
    )
    
    #Dag creation
    s3_sync