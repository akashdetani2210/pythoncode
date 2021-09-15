##Copyright 2020 Google LLC.
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.

import datetime
import logging

from airflow.contrib.operators.gcp_transfer_operator import S3ToGoogleCloudStorageTransferOperator

from airflow.models import Variable

#Read Airflow Variable
config = Variable.get("bioInfo_s3_to_gcs_config", deserialize_json=True)

s3_bucket=config["s3_bucket"]
gcs_bucket=config["gcs_bucket"]


def create_dag(dag, folder, default_dag_args=None):
    
    #S3 to GCS transfer
    s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
                                    dag=dag,
                                    task_id=f"s3_to_gcs_{folder}",
                                    s3_bucket=s3_bucket,
                                    gcs_bucket=gcs_bucket,
                                    description= "Transfer unloaded data from S3",
                                    object_conditions={ 'include_prefixes': [ folder ] },
                                    timeout=60,
                                    wait=1
                                    )
    
    # Arrange the DAG
    s3_to_gcs

    return dag