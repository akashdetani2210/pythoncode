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
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.contrib.operators.gcp_transfer_operator import S3ToGoogleCloudStorageTransferOperator

default_dag_args = {
      'retries': 2,
      'retry_delay': datetime.timedelta(seconds=45),
      'execution_timeout': datetime.timedelta(hours=1),
      'start_date': days_ago(0),
      'schedule_interval': None,
      'email': 'detani@google.com'
}

#Read Airflow Variable
config = Variable.get("bioInfo_s3_to_gcs_config", deserialize_json=True)


#AWS Variables
s3_bucket = config["s3_bucket"]


#GCS Variables
gcs_bucket = config["gcs_bucket"]

gcs_include_prefix =  '{{dag_run.conf["gcs_include_prefix"]}}'

    
#Start Tasks
with models.DAG('s3_to_gcs_prefix',
                max_active_runs=1,
                default_args=default_dag_args) as dag:

    
    
    s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
                                    task_id='s3_to_gcs',
                                    s3_bucket=s3_bucket,
                                    gcs_bucket=gcs_bucket,
                                    description= "Transfer unloaded data from S3",
                                    object_conditions={ 'include_prefixes': [ gcs_include_prefix ] },
                                    timeout=60,
                                    wait=1
                                    )
                                    

    #Dag creation
    s3_to_gcs