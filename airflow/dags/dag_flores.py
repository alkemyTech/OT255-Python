import logging
from datetime import datetime
from src.py_functions.env_call import *

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from py_functions import manip_pandas_flores
from src.py_functions import extraccion_PyOp

# Logs
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formato = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_hanlder = logging.FileHandler("daf_flores.log")
file_hanlder.setFormatter(formato)
# Formato
stream_handler = logging.StreamHandler()  # Seteo en la consola
stream_handler.setFormatter(formato)
logger.addHandler(stream_handler)  # Se agrega a logger


# Configuracion del DAG
with DAG(
    "dag_flores",
    description="DAG para la Universidad de Flores.",
    schedule_interval="0 * * * *",
    start_date=datetime(2022, 7, 19),
) as dag:
    # Conexion a la base de datos y extraccion de la data de la universidad
    extraccion_bd = PythonOperator(
        task_id="extraccion_bd",
        python_callable=extraccion_PyOp,
        retries=5,
        op_kwargs={"university": "flores"},
    )

    # Manipulacion de los datos con Pandas
    process_pandas = PythonOperator(
        task_id="extract_pandas", python_callable=manip_pandas_flores
    )

    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/transfers/local_to_s3/index.html#module-airflow.providers.amazon.aws.transfers.local_to_s3
    upload_s3 = LocalFilesystemToS3Operator(
        task_id="upload_s3",
        aws_conn_id="S3_Connection",
        replace=True,
        # filename="files/modified/g255_flores.txt"
        filename=Path(__file__).parent.parent.parent
        / "files"
        / "modified"
        / "g255_flores.txt",
        dest_key=public_key,
        dest_bucket=bucket_name,
    )

    extraccion_bd >> process_pandas >> upload_s3
