from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from datetime import datetime, timedelta

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d')


def extraer_latinoam():
    logging.info(
        "Extrayendo datos de la Facultad Latinoamericana de Ciencias Sociales")
    pass


def extraer_kenn():
    logging.info("Extrayendo datos de la Universidad J.F. Kennedy")
    pass


def transform_data():
    logging.info("Transformando la data")
    pass


def load_s3():
    logging.info("Cargando la data a S3")
    pass


def log_inicio():
    logging.info(
        'Iniciando el dag: dag_facultad_latinoamericana_universidad_kennedy')


with DAG(
    'dag_facultad_latinoamericana_universidad_kennedy',
    description='Operators que se deberán utilizar a futuro',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 7, 19)
) as dag:

    # Log de inicio del DAG
    log_inicio_dag = PythonOperator(
        task_id='log_inicio',
        python_callable=log_inicio
    )

    # Extraer la informacion de las dos universidades
    extraer_fac_latinoamericana = PythonOperator(
        task_id='extraer_fac_latinoamericana',
        python_callable=extraer_latinoam,
        retries=5,
        retry_delay=timedelta(seconds=120)
    )
    extraer_kennedy = PythonOperator(
        task_id='extraer_kennedy',
        python_callable=extraer_kenn,
        retries=5,
        retry_delay=timedelta(seconds=120)
    )

# Transformar la data usando pandas
    transform = PythonOperator(
        task_id='transform', python_callable=transform_data)

# Crear un bucket (si existe no lo crea)
    crear_bucket = S3CreateBucketOperator(
        task_id='crear_bucket', bucket_name='bucket_latinoamericana_kennedy')

# Carga el archivo en el servidor s3
    load_to_s3 = PythonOperator(task_id='load_s3', python_callable=load_s3)

    log_inicio_dag >> [extraer_fac_latinoamericana,
                       extraer_kennedy] >> transform >> crear_bucket >> load_to_s3
