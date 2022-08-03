import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from src.funciones import univ_nac_jujuy_y_palermo as fun

logging.basicConfig(format="%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d")

args = {
    "description": "obtener información sobre Univ. Nacional de Jujuy",
    "start_date": datetime(2022, 7, 30),
    "schedule_interval": timedelta(hours=1),
}

dag = DAG("dag_univ_nac_de_jujuy", default_args=args)


extraer_datos_univ_nac_de_jujuy = PythonOperator(
    task_id="extraer_datos",
    retries=5,
    retry_delay=timedelta(minutes=1),
    python_callable=fun.extraer_datos_univ_nac_de_jujuy,
    dag=dag,
)

transformar_datos_univ_nac_de_jujuy = PythonOperator(
    task_id="transformar_datos",
    python_callable=fun.transformar_datos_univ_nac_de_jujuy,
    dag=dag,
)

cargar_datos_univ_nac_de_jujuy = PythonOperator(
    task_id="cargar_datos", python_callable=fun.cargar_datos_univ_nac_de_jujuy, dag=dag
)

(
    extraer_datos_univ_nac_de_jujuy
    << transformar_datos_univ_nac_de_jujuy
    << cargar_datos_univ_nac_de_jujuy
)
