import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import \
    LocalFilesystemToS3Operator

logging.basicConfig(format="%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d")

args = {
    "description": "obtener información sobre Univ. Nacional de Jujuy",
    "start_date": datetime(2022, 7, 30),
    "schedule_interval": timedelta(hours=1),
}

dag = DAG("dag_univ_nac_de_jujuy", default_args=args)


logging.info("Se extraen datos.")
extraer_datos_univ_nac_de_jujuy = PythonOperator(
    task_id="extraer_datos",
    retries=5,
    retry_delay=timedelta(minutes=1),
    python_callable=extraer_y_escribir_datos_en_csv("univ_nacional_de_jujuy"),
    dag=dag,
)

logging.info("Se transforman datos.")
transformar_datos_univ_nac_de_jujuy = PythonOperator(
    task_id="transformar_datos",
    python_callable=normalizar_y_escribir_datos_en_txt("univ_nacional_de_jujuy"),
    dag=dag,
)

logging.info("Se cargan datos.")
cargar_datos_univ_nac_de_jujuy = LocalFilesystemToS3Operator(
    task_id="cargar_datos_UNJ",
    filename=pathlib.Path.cwd() / "src" / "txt" / "univ_nacional_de_jujuy.txt",
    aws_conn_id="carga_to_S3",
    dest_key="univ_nacional_de_jujuy.txt",
    dest_bucket="cohorte-julio-8972766c",
    replace=True,
    dag=dag,
)


(
    extraer_datos_univ_nac_de_jujuy
    << transformar_datos_univ_nac_de_jujuy
    << cargar_datos_univ_nac_de_jujuy
)
