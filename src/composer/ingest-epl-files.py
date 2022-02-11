import datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import python_operator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator 
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

project_id                          =  Variable.get('gcp_project_name')
dataset                             =  Variable.get('dataset')
input_files_path                   =  Variable.get('bucket_names', deserialize_json=True)['files']
today                               =  datetime.datetime.now()

default_args = {
    'owner': 'EPL-Analitycs',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(today.year, today.month, today.day),
}

with DAG(
    'ingest-epl-files',
    default_args=default_args,
    description='DAG for EPL Files',
    schedule_interval=None, 
    tags=['ingestion'],
) as dag:

    start           = DummyOperator(task_id='start')
    end             = DummyOperator(task_id='end')

    loading_in_bigquery = GoogleCloudStorageToBigQueryOperator(

    # task_id="load_pres_parquet_file",
    # bucket="{{ti.xcom_pull(key='bucketPath',task_ids='start')}}",
    # source_objects=["{{ti.xcom_pull(key='filePath',task_ids='start')}}"],
    # destination_project_dataset_table="{{var.value.gcp_project_name}}.{{var.value.recon_dataset_name}}.{{ti.xcom_pull(key='temp_table_name', task_ids='start')}}" + '_' + "{{ti.xcom_pull(key='tmp_file_name_suffix', task_ids='start')}}",
    # source_format="PARQUET",
    # create_disposition="CREATE_IF_NEEDED",
    # write_disposition="WRITE_TRUNCATE",
    # google_cloud_storage_conn_id="google_cloud_default",
    # bigquery_conn_id="bigquery_default",
    # dag=dag)


    )

    transform_in_bigquery = BigQueryOperator(

    # task_id='insert_preraw_status_into_status_table',
    # use_legacy_sql=False,
    # write_disposition='WRITE_APPEND',
    # allow_large_results=True,
    # sql='''
    # #standardSQL
    # insert into `{{var.value.gcp_project_name}}.{{var.value.recon_dataset_name}}.{{var.json.table_names.file_ingestion_status}}`
    #         (file_name,
    #         file_date,
    #         process_name,
    #         total_transactions,
    #         failed_transactions,
    #         total_transaction_amount,
    #         failed_transaction_amount,
    #         version,
    #         status,
    #         loaded_to_raw,
    #         loaded_to_std,
    #         source_file_location,
    #         error_file_location,
    #         metadata,
    #         comments,
    #         created_at,
    #         updated_at)
    #     SELECT  "{{ti.xcom_pull(key='fileName',task_ids='start')}}",
    #             SAFE_CAST("{{ti.xcom_pull(key='fileDate',task_ids='start')}}" AS DATE),
    #             CASE WHEN "{{ti.xcom_pull(key='fileType',task_ids='start')}}"="settlement" THEN "MERCHANT_SET" WHEN "{{ti.xcom_pull(key='fileType',task_ids='start')}}"="presentation" THEN "MERCHANT_PRES" ELSE "{{ti.xcom_pull(key='fileType',task_ids='start')}}" END as process_name,
    #             null,
    #             null,
    #             null,
    #             null,
    #             1,
    #             "PRE_RAW_INGESTION_SUCCESS",
    #             true,
    #             null,
    #             "{{ti.xcom_pull(key='processed_file_path',task_ids='start')}}",
    #             null,
    #             null,
    #             "PRE RAW INGESTION IS SUCCESSFUL",
    #             current_timestamp(),
    #             null
    #     FROM  `{{var.value.gcp_project_name}}.{{var.value.recon_dataset_name}}.{{ti.xcom_pull(key='temp_table_name', task_ids='start')}}''' + '_' + '''{{ti.xcom_pull(key='tmp_file_name_suffix', task_ids='start')}}`
    #     ''',
    # trigger_rule=TriggerRule.ONE_SUCCESS,
    # on_failure_callback=on_dag_failure,
    # retries=0,
    # dag=dag
    )

    save_file_in_gcs = BigQueryToCloudStorageOperator (
        #https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/contrib/operators/bigquery_to_gcs/index.html
        #source_project_dataset_table, destination_cloud_storage_uris, compression='NONE', export_format='CSV', field_delimiter=',', print_header=True, bigquery_conn_id='bigquery_default', delegate_to=None, labels=None, *args, **kwargs
    )
    
    start >> loading_in_bigquery
    loading_in_bigquery >> transform_in_bigquery
    transform_in_bigquery >> save_file_in_gcs
    save_file_in_gcs >> end
