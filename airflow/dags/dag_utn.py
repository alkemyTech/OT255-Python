import logging
from datetime import datetime, timedelta

from airflow import DAG

# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.operators.postgres import PostgresHook
# from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from src.py_functions import data_process, query_process, s3_upload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d",
)

# Create custom logger, add console handler
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)


with DAG(
    "dag_utn",
    description="Conection with utn data in training db",
    start_date=datetime(2022, 7, 19),
    # schedule_interval=timedelta(hours=1),
    schedule_interval=None,
) as dag:

    t_export_data = PythonOperator(
        task_id="export_data",
        python_callable=query_process.db_extract_dag,
        op_kwargs={"university": "utn"},
        retries=5,
    )

    t_data_transform = PythonOperator(
        task_id="data_transform",
        python_callable=data_process.csvByLocation_to_txt_dag,
        op_kwargs={"university": "utn"},
    )

    # t_creat_bucket = S3CreateBucketOperator(
    #    task_id="create_bucket", bucket_name="univ_3febrero_bucket"
    # )

    # t_data_load = PythonOperator(
    # task_id= 'upload_utn_to_s3',
    #        python_callable=s3_upload.upload_univ_to_s3,
    #        op_kwargs={
    #            'filename': 'files/modified/g255_utn.txt',
    #            'key': 'g255_utn.txt',
    #            'bucket_name': 'cohorte-julio-8972766c'}
    #        )

    t_data_load = LocalFilesystemToS3Operator(
        task_id="upload_utn",
        filename=f"./files/modified/G255_utn.txt",
        aws_conn_id="s3_conn",
        dest_key=f"G255_utn.txt",
        dest_bucket="cohorte-julio-8972766c",
        replace=True,
    )

    t_export_data >> t_data_transform >> t_data_load
