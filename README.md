# Storage Transfer Composer

Various DAGS and plugins used for Storage Transfer project.

## Airflow DAGS:

### 1. s3_to_gcs_full.py

- DAG will generate multiple DAGs by listing prefixes in the S3 bucket and each DAG will transfer data from S3 to GCS.
DAG will generate multiple DAGs by listing prefixes in the S3 bucket and each DAG will transfer data from S3 to GCS.

Requirement:
```
module "concourse_pipeline" {
  source         = "git@github.com:tempuslabs/autoform-modules//modules/composite/concourse/cloud-composer-pipeline"
  concourse_team = "rp"
  name           = "storage-transfer-composer"
  git_repo       = "storage-transfer-composer"
  git_branch     = "master"
  entities = [
    # TODO: add plugins if necessary
    {
      type        = "dags"
      source      = ""dags/<project-name>/""
      destination = ""
      alpha = {
  ...
```
