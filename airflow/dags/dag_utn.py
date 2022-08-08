import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import \
    LocalFilesystemToS3Operator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.operators.postgres import PostgresHook
# from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from src.py_functions import callables

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
    schedule_interval=timedelta(hours=1),
    schedule_interval=None,
) as dag:

    t_export_data = PythonOperator(
        task_id="export_data",
        python_callable=callables.db_extract_dag,
        op_kwargs={"university": "utn"},
        retries=5,
    )

    t_data_transform = PythonOperator(
        task_id="data_transform",
        python_callable=callables.csvByLocation_to_txt_dag,
        op_kwargs={"university": "utn"},
    )

    t_data_load = LocalFilesystemToS3Operator(
        task_id="upload_utn",
        filename=Path.cwd() / "files" / "modified" / "G255_3feb.txt",
        aws_conn_id="s3_conn",
        dest_key=f"G255_utn.txt",
        dest_bucket="cohorte-julio-8972766c",
        replace=True,
    )

    t_export_data >> t_data_transform >> t_data_load
