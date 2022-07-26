import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FOLDER_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(os.path.dirname(FOLDER_DIR))

from src.py_functions.extract_fac_latam_univ_jfk import extrac_fac_y_univ

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d",
)


def transform_data():
    logging.info("Transformando la data")
    pass


def load_s3():
    logging.info("Cargando la data a S3")
    pass


def log_inicio():
    logging.info(
        "Iniciando el dag: dag_facultad_latinoamericana_universidad_kennedy"
    )


with DAG(
    "dag_facultad_latinoamericana_universidad_kennedy",
    description="Operators que se deberán utilizar a futuro",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 7, 19),
) as dag:

    # Log de inicio del DAG
    log_inicio_dag = PythonOperator(
        task_id="log_inicio", python_callable=log_inicio
    )

    # Extraer la informacion de las dos universidades
    extraer_fac_latam_y_jfk = PythonOperator(
        task_id="extraer_fac_latam_y_jfk",
        python_callable=transform_data,
        retries=5,
        retry_delay=timedelta(seconds=120),
    )

    # Transformar la data usando pandas
    transform = PythonOperator(
        task_id="transform", python_callable=transform_data
    )

    # Carga el archivo en el servidor s3
    load_to_s3 = PythonOperator(task_id="load_s3", python_callable=load_s3)

    (log_inicio_dag >> extraer_fac_latam_y_jfk >> transform >> load_to_s3)
