import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(format="%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d")

args = {
    "description": "obtener información sobre Univ. de Palermo",
    "start_date": datetime(2022, 7, 30),
    "schedule_interval": timedelta(hours=1),
}

dag = DAG("dag_univ_palermo", default_args=args)


def extraer_datos_univ_palermo():
    pass


def transformar_datos_univ_palermo():
    pass


def cargar_datos_univ_palermo():
    pass


extraer_datos_univ_palermo = PythonOperator(
    task_id="extraer_datos",
    retries=5,
    retry_delay=timedelta(minutes=1),
    python_callable=extraer_y_escribir_datos_en_csv("univ_de_palermo"),
    dag=dag,
)

transformar_datos_univ_palermo = PythonOperator(
    task_id="transformar_datos", python_callable=normalizar_y_escribir_datos_en_txt("univ_de_palermo"), dag=dag
)

cargar_datos_univ_palermo = PythonOperator(
    task_id="cargar_datos", python_callable=cargar_datos_univ_palermo, dag=dag
)

(
    extraer_datos_univ_palermo
    << transformar_datos_univ_palermo
    << cargar_datos_univ_palermo
)
