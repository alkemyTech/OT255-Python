import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from decouple import config
from src.py_functions.extract_fac_latam_univ_jfk import extrac_fac_y_univ
from src.py_functions.transform_fac_latam_univ_jfk import (
    transform_faclatam_ujfk,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d",
)

ruta_latam = (
    Path(__file__).parent.parent.parent
    / "files"
    / "modified"
    / "g255_fac_latam.csv"
)

ruta_jfk = (
    Path(__file__).parent.parent.parent
    / "files"
    / "modified"
    / "g255_univ_jfk_.csv"
)


def log_inicio():
    logging.info(
        "Iniciando el dag: dag_facultad_latinoamericana_universidad_kennedy"
    )


with DAG(
    "dag_facultad_latinoamericana_universidad_kennedy",
    description="Operators que se deberán utilizar a futuro",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 7, 30),
) as dag:

    # Log de inicio del DAG
    log_inicio_dag = PythonOperator(
        task_id="log_inicio", python_callable=log_inicio
    )

    # Extraer la informacion de las dos universidades
    extraer_fac_latam_y_jfk = PythonOperator(
        task_id="extraer_fac_latam_y_jfk",
        python_callable=extrac_fac_y_univ,
        retries=5,
        retry_delay=timedelta(seconds=120),
    )

    # Transformar la data usando pandas
    transform = PythonOperator(
        task_id="transform", python_callable=transform_faclatam_ujfk
    )

    # Carga el archivo en el servidor s3
    Cargar_csv_latam_s3 = LocalFilesystemToS3Operator(
        task_id="Cargar_csv_latam_s3",
        filename=ruta_latam,
        dest_key=config("S3_KEY"),
        dest_bucket=config("S3_BUCKET"),
        aws_conn_id="g255 S3",
        replace=True,
    )

    Cargar_csv_jfk_s3 = LocalFilesystemToS3Operator(
        task_id="Cargar_csv_jfk_s3",
        filename=ruta_jfk,
        dest_key=config("S3_KEY"),
        dest_bucket=config("S3_BUCKET"),
        aws_conn_id="g255 S3",
        replace=True,
    )

    (
        log_inicio_dag
        >> extraer_fac_latam_y_jfk
        >> transform
        >> Cargar_csv_latam_s3
        >> Cargar_csv_jfk_s3
    )
