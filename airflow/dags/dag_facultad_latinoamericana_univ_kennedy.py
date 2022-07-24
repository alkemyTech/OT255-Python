import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d",
)


def extraer_latinoam():
    logging.info("Extrayendo datos de la Facultad Latinoamericana de Ciencias Sociales")
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
    logging.info("Iniciando el dag: dag_facultad_latinoamericana_universidad_kennedy")


with DAG(
    "dag_facultad_latinoamericana_universidad_kennedy",
    description="Operators que se deberán utilizar a futuro",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 7, 19),
) as dag:

    # Log de inicio del DAG
    log_inicio_dag = PythonOperator(task_id="log_inicio", python_callable=log_inicio)

    # Extraer la informacion de las dos universidades
    extraer_fac_latinoamericana = PythonOperator(
        task_id="extraer_fac_latinoamericana",
        python_callable=extraer_latinoam,
        retries=5,
        retry_delay=timedelta(seconds=120),
    )
    extraer_kennedy = PythonOperator(
        task_id="extraer_kennedy",
        python_callable=extraer_kenn,
        retries=5,
        retry_delay=timedelta(seconds=120),
    )

    # Transformar la data usando pandas
    transform = PythonOperator(task_id="transform", python_callable=transform_data)

    # Carga el archivo en el servidor s3
    load_to_s3 = PythonOperator(task_id="load_s3", python_callable=load_s3)

    (
        log_inicio_dag
        >> [extraer_fac_latinoamericana, extraer_kennedy]
        >> transform
        >> load_to_s3
    )
