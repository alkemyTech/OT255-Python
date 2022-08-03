import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from src.funciones import univ_nac_jujuy_y_palermo as fun

logging.basicConfig(format="%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d")


args = {
    "description": "obtener información sobre Univ. de Palermo",
    "start_date": datetime(2022, 7, 30),
    "schedule_interval": timedelta(hours=1),
}

dag = DAG("dag_univ_palermo", default_args=args)


extraer_datos_univ_palermo = PythonOperator(
    task_id="extraer_datos",
    retries=5,
    retry_delay=timedelta(minutes=1),
    python_callable=fun.extraer_datos_univ_palermo,
    dag=dag,
)

transformar_datos_univ_palermo = PythonOperator(
    task_id="transformar_datos",
    python_callable=fun.transformar_datos_univ_palermo,
    dag=dag,
)

cargar_datos_univ_palermo = PythonOperator(
    task_id="cargar_datos", python_callable=fun.cargar_datos_univ_palermo, dag=dag
)

(
    extraer_datos_univ_palermo
    << transformar_datos_univ_palermo
    << cargar_datos_univ_palermo
)
