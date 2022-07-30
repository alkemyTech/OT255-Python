import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Importo la funcion para hacer la extraccion
from src.py_functions import extraccion_PyOp

# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

# Logs
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formato = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# Formato
stream_handler = logging.StreamHandler()  # Seteo en la consola
stream_handler.setFormatter(formato)
logger.addHandler(stream_handler)  # Se agrega a logger


def manip_pandas():
    # Aca hago todo el procesamiento de la data con PythonOperator
    pass


def upload_to_s3():
    # Primero establezco la conexion con S3Hook()
    # Luego, creo un bucket, si no existe, con S3CreateBucketOperator
    # Por ultimo, subo el archivo a ese bucket
    pass


# Configuracion del DAG
with DAG(
    "dag_villamaria",
    description="DAG para la Universidad de Villa Maria.",
    schedule_interval="0 * * * *",
    start_date=datetime(2022, 7, 19),
) as dag:
    # Conexion a la base de datos y extraccion de la data de la universidad
    extraccion_bd = PythonOperator(
        task_id="exrtaccion_bd",
        python_callable=extraccion_PyOp("villa_maria"),
        retries=5,
    )

    # Manipulacion de los datos con Pandas
    process_pandas = PythonOperator(
        task_id="extract_pandas", python_callable=manip_pandas
    )

    upload_s3 = PythonOperator(task_id="upload_s3", python_callable=upload_to_s3)

    extraccion_bd >> process_pandas >> upload_s3
