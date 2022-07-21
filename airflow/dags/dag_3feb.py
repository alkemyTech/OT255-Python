from airflow import DAG

# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d",
)

# Create custom logger, add console handler
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)


def database_connect():
    pass


def export_data():
    pass


def data_transform():
    pass


def hook_and_upload():
    # hook = S3Hook(" ")      # [Connection_Id_name]
    # hook.load_file(" ")     # [file_name]
    pass


with DAG(
    "3feb_table_data",
    description="Conection with utn data in training db",
    start_date=datetime(2022, 7, 19),
    schedule_interval=timedelta(hours=1),
) as dag:

    t_conect_db = PythonOperator(
        task_id="db_connect", python_callable=database_connect, retries=5
    )

    t_export_data = PythonOperator(task_id="export_data", python_callable=export_data)

    t_data_transform = PythonOperator(
        task_id="data_transform", python_callable=data_transform
    )

    t_creat_bucket = S3CreateBucketOperator(
        task_id="create_bucket", bucket_name="utn_bucket"
    )

    t_data_load = PythonOperator(
        task_id="hook_and_upload_to_s3", python_callable=hook_and_upload
    )

    t_conect_db >> t_export_data >> t_data_transform >> t_creat_bucket >> t_data_load
