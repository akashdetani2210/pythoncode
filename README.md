# Storage Transfer Composer

Various DAGS and plugins used for Storage Transfer project.

## Prerequisites:

- There are certain variables which are defined in DAG which we need to create in airflow UI under airflow variables.
- Access keys and Secret access keys to access AWS s3 folders need to be created in GCP Secret Manager.
- AWS default connection to connect with AWS should be created in Secret Manager.

## Requirement:

- bioInfo_s3_to_gcs_config

```
{
  "s3_bucket": "pipeline-refdata",
  "gcs_bucket": "tl-bet-bioinf-analysis-complete-us",
  "gcs_include_prefix": "a",
  "project_id": "tl-f2hm8zeympjvhm41k5yo",
  "bq_table": "s3_object_inventory",
  "bq_dataset": "bioinfo_bet",
  "bet_project_id": "tl-zoam3i2fls475phuacbm",
  "bet_gcs_bucket": "tl-bet-bioinf-analysis-complete-us"
}
```

- gcs_to_s3_sync

```
{
  "s3_path": "s3-testing-dc",
  "gcs_path": "us-east1-composer-new-sandb-4490857a-bucket",
  "folder": "export_batch"
}
```

## Airflow DAGS:

### 1.  s3_to_gcs_folder_task.py

- DAG will generate multiple tasks by listing prefixes in the S3 bucket and each task will transfer data from S3 to GCS.


### 2.  s3_to_gcs_prefix.py

- DAG will transfer 1 s3 folder based on prefix.


### 3.  s3_to_gcs_objinv.py

- DAG will send the gcs object details into Bigquery table.


### 4.  gcs_to_s3_rsync.py

- DAG will sync the files to s3 if any new data loaded directly in gcs bucket.