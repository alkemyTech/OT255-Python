from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d"
)

args = {
	'description': 'obtener información sobre Univ. Nacional de Jujuy',
    'start_date': datetime(2022, 7, 30)
}

dag = DAG('dag_univ_nac_de_jujuy',default_args=args)

def extraer_datos_univ_nac_de_jujuy():
    pass

def transformar_datos_univ_nac_de_jujuy():
    pass

def cargar_datos_univ_nac_de_jujuy():
    pass


extraer_datos_univ_nac_de_jujuy = PythonOperator(
    task_id = 'extraer_datos',
    retries = 5,
    retry_delay = timedelta(minutes = 1),
    python_callable = extraer_datos_univ_nac_de_jujuy,
    dag=dag
)

transformar_datos_univ_nac_de_jujuy = PythonOperator(
    task_id = 'transformar_datos',
    python_callable = transformar_datos_univ_nac_de_jujuy,
    dag=dag
)

cargar_datos_univ_nac_de_jujuy = PythonOperator(
    task_id = "cargar_datos",
    python_callable = cargar_datos_univ_nac_de_jujuy,
    dag=dag
)

extraer_datos_univ_nac_de_jujuy << transformar_datos_univ_nac_de_jujuy << cargar_datos_univ_nac_de_jujuy