import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator


def conf_logs():
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d",
        level=logging.INFO,
    )


def conex_postgres():
    # Aca realizo la Query con PostgresOperator
    pass


def manip_pandas():
    # Aca hago todo el procesamiento de la data con PythonOperator
    pass


def upload_to_s3():
    # Primero establezco la conexion con S3Hook()
    # Luego, creo un bucket, si no existe, con S3CreateBucketOperator
    # Por ultimo, subo el archivo a ese bucket
    pass


def_args = {
    "owner": "Lautaro Flores",
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "dag_flores",
    default_args=def_args,
    description="DAG para la Universidad de Flores.",
    schedule_interval="0 * * * *",
    start_date=datetime(2022, 7, 19),
) as dag:

    loggs = PythonOperator(task_id="loggs", python_callable=conf_logs)

    conexion_postgres = PythonOperator(
        task_id="conexion_postgres", python_callable=conex_postgres
    )

    process_pandas = PythonOperator(
        task_id="extract_pandas", python_callable=manip_pandas
    )

    upload_s3 = PythonOperator(task_id="upload_s3", python_callable=upload_to_s3)

    loggs >> conexion_postgres >> process_pandas >> upload_s3
