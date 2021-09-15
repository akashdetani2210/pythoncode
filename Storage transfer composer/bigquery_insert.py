import datetime
import boto3
from airflow import DAG
from airflow import models
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator



default_dag_args = {
      'retries': 2,
      'retry_delay': datetime.timedelta(seconds=45),
      'execution_timeout': datetime.timedelta(hours=1),
      'start_date': days_ago(0),
      'schedule_interval': None,
      'email': 'detani@google.com'
}



DATASET_NAME='tl_alp_storage_transfer_notification'
TABLE_1='notifications'

def bq_insert():
    

    INSERT_ROWS_QUERY = (
                                f"INSERT into {DATASET_NAME}.{TABLE_1} VALUES ( 'storage#object','tl-alp-storage-transfer/TempusVCF/1.6.0/~/xO_germline_db.rdata/1613720449739061','https://www.googleapis.com/storage/v1/b/dag_test_bucket_sub/o/test1%2F','TempusVCF/1.10.0/xF_bgrd_ave.csv','tl-alp-storage-transfer','1613720449739061','1','application/x-directory; charset=UTF-8','2021-02-25 07:40:49.822 UTC','2021-02-25 07:40:49.822 UTC','STANDARD','2021-02-19 07:40:49.822 UTC','823540771','1B2M2Y8AsgTpgAmY7PhCfg==','https://www.googleapis.com/download/storage/v1/b/dag_test_bucket_sub/o/test1%2F?generation=1613720449739061&alt=media','AAAAAA==','CLXSsZG59e4CEAE='),( 'storage#object','tl-alp-storage-transfer/TempusVCF/1.6.5/hs_depth.csv/1613720449739061','https://www.googleapis.com/storage/v1/b/dag_test_bucket_sub/o/test1%2F','TempusVCF/1.6.5/hs_depth.csv','tl-alp-storage-transfer','1613720449739061','1','application/x-directory; charset=UTF-8','2021-02-25 07:40:49.822 UTC','2021-02-25 07:40:49.822 UTC','STANDARD','2021-02-19 07:40:49.822 UTC','443893876','1B2M2Y8AsgTpgAmY7PhCfg==','https://www.googleapis.com/download/storage/v1/b/dag_test_bucket_sub/o/test1%2F?generation=1613720449739061&alt=media','AAAAAA==','CLXSsZG59e4CEAE='),( 'storage#object','tl-alp-storage-transfer/TempusVCF/1.6.0/~/xO_germline_db.rdata/1613720449739061','https://www.googleapis.com/storage/v1/b/dag_test_bucket_sub/o/test1%2F','TempusVCF/1.6.5/xf_whitelist_cpras.txt','tl-alp-storage-transfer','1613720449739061','1','application/x-directory; charset=UTF-8','2021-02-25 07:40:49.822 UTC','2021-02-25 07:40:49.822 UTC','STANDARD','2021-02-19 07:40:49.822 UTC','8400','1B2M2Y8AsgTpgAmY7PhCfg==','https://www.googleapis.com/download/storage/v1/b/dag_test_bucket_sub/o/test1%2F?generation=1613720449739061&alt=media','AAAAAA==','CLXSsZG59e4CEAE='),( 'storage#object','tl-alp-storage-transfer/TempusVCF/1.6.0/~/xO_germline_db.rdata/1613720449739061','https://www.googleapis.com/storage/v1/b/dag_test_bucket_sub/o/test1%2F','TempusVCF/1.6.3/xe_gene_coordinates.bed','tl-alp-storage-transfer','1613720449739061','1','application/x-directory; charset=UTF-8','2021-02-25 07:40:49.822 UTC','2021-02-25 07:40:49.822 UTC','STANDARD','2021-02-19 07:40:49.822 UTC','5803378','1B2M2Y8AsgTpgAmY7PhCfg==','https://www.googleapis.com/download/storage/v1/b/dag_test_bucket_sub/o/test1%2F?generation=1613720449739061&alt=media','AAAAAA==','CLXSsZG59e4CEAE='),( 'storage#object','tl-alp-storage-transfer/TempusVCF/1.6.0/~/xO_germline_db.rdata/1613720449739061','https://www.googleapis.com/storage/v1/b/dag_test_bucket_sub/o/test1%2F','TempusVCF/1.10.0/xF_germline_db.csv','tl-alp-storage-transfer','1613720449739061','1','application/x-directory; charset=UTF-8','2021-02-25 07:40:49.822 UTC','2021-02-25 07:40:49.822 UTC','STANDARD','2021-02-19 07:40:49.822 UTC','3225246','1B2M2Y8AsgTpgAmY7PhCfg==','https://www.googleapis.com/download/storage/v1/b/dag_test_bucket_sub/o/test1%2F?generation=1613720449739061&alt=media','AAAAAA==','CLXSsZG59e4CEAE=');"
                            )
                            
    print(INSERT_ROWS_QUERY)
        
       # task_id=f"insert_query_{name}"
    bq_job = BigQueryInsertJobOperator(task_id='insert_job',
                                            configuration={
                                                "query": {
                                                        "query": INSERT_ROWS_QUERY,
                                                        "useLegacySql": False,
                                                    }
                                                }
                                            )
    
    
#Start Tasks
with models.DAG('bq_insert_row_test',
                max_active_runs=1,
                default_args=default_dag_args) as dag:

    bq_insert = bq_insert()
    

    #Dag creation
    bq_insert