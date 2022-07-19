from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime


def data_transform():
    pass

def hook_and_upload_to_s3():
    hook = S3Hook(" ")      # [Connection_Id_name]
    hook.load_file(" ")     # [file_name]

with DAG(
    '3feb_table_data',
    description = 'Conection with utn data in training db',
    start_date = datetime(2022, 7, 19),
    schedule_interval = timedelta(hours = 1)
) as dag:
    
    task_1 = PostgresHook (
        task_id = 'db_connect',
        postgress_conn_id = " "         # The postgress conn id reference to a specific postgres database
    )

    task_2 = PostgresOperator(
        task_id = '3feb_data_consult',
        postgres_conn_id = " ",         # [Connection_Id_name]
        sql = " "                       # [sql_consult_name_file.sql]
    )

    task_3 = PythonOperator(
        task_id = 'data_transform',
        python_callable = data_transform
    )

    task_4 = PythonOperator(
        task_id = "hook_and_upload_to_s3"    
    )

    task_1 >> task_2 >> task_3 >> task_4
